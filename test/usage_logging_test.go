package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/redisqueue"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

func TestGeminiExecutorRecordsSuccessfulZeroUsageInQueue(t *testing.T) {
	model := fmt.Sprintf("gemini-2.5-flash-zero-usage-%d", time.Now().UnixNano())
	source := fmt.Sprintf("zero-usage-%d@example.com", time.Now().UnixNano())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantPath := "/v1beta/models/" + model + ":generateContent"
		if r.URL.Path != wantPath {
			t.Fatalf("path = %q, want %q", r.URL.Path, wantPath)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":0,"candidatesTokenCount":0,"totalTokenCount":0}}`))
	}))
	defer server.Close()

	executor := runtimeexecutor.NewGeminiExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key":  "test-upstream-key",
			"base_url": server.URL,
		},
		Metadata: map[string]any{
			"email": source,
		},
	}

	prevQueueEnabled := redisqueue.Enabled()
	prevUsageEnabled := redisqueue.UsageStatisticsEnabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	redisqueue.SetUsageStatisticsEnabled(true)
	t.Cleanup(func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
		redisqueue.SetUsageStatisticsEnabled(prevUsageEnabled)
	})

	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FormatGemini,
		OriginalRequest: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	waitForQueuedUsageModelTotalTokens(t, "gemini", model, 0)
}

func waitForQueuedUsageModelTotalTokens(t *testing.T, wantProvider, wantModel string, wantTokens int64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		items := redisqueue.PopOldest(10)
		for _, item := range items {
			got, ok := parseQueuedUsagePayload(t, item)
			if !ok {
				continue
			}
			if got.Provider != wantProvider || got.Model != wantModel {
				continue
			}
			if got.Failed {
				t.Fatalf("payload failed = true, want false")
			}
			if got.Tokens.TotalTokens != wantTokens {
				t.Fatalf("payload total tokens = %d, want %d", got.Tokens.TotalTokens, wantTokens)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for queued usage payload for provider=%q model=%q", wantProvider, wantModel)
}

type queuedUsagePayload struct {
	Provider            string `json:"provider"`
	Model               string `json:"model"`
	Failed              bool   `json:"failed"`
	ServiceTier         string `json:"service_tier"`
	ResponseServiceTier string `json:"response_service_tier"`
	Tokens              struct {
		InputTokens           int64 `json:"input_tokens"`
		OutputTokens          int64 `json:"output_tokens"`
		CachedTokens          int64 `json:"cached_tokens"`
		CacheReadTokens       int64 `json:"cache_read_tokens"`
		CacheCreationTokens   int64 `json:"cache_creation_tokens"`
		TotalTokens           int64 `json:"total_tokens"`
		ImageInputTokens      int64 `json:"image_input_tokens"`
		ImageCacheReadTokens  int64 `json:"image_cache_read_tokens"`
		ImageOutputTokens     int64 `json:"image_output_tokens"`
		CacheCreation1hTokens int64 `json:"cache_creation_1h_tokens"`
		ImageTokensKnown      bool  `json:"image_tokens_known"`
		CacheCreationTTLKnown bool  `json:"cache_creation_ttl_known"`
	} `json:"tokens"`
}

func parseQueuedUsagePayload(t *testing.T, payload []byte) (queuedUsagePayload, bool) {
	t.Helper()

	var parsed queuedUsagePayload
	if len(payload) == 0 {
		return parsed, false
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return parsed, false
	}
	if parsed.Provider == "" || parsed.Model == "" {
		return parsed, false
	}
	return parsed, true
}

func TestClaudeExecutorRecords1hCacheCreationAndFastInQueue(t *testing.T) {
	model := fmt.Sprintf("claude-opus-5-test-%d", time.Now().UnixNano())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id": "msg_01",
			"type": "message",
			"speed": "fast",
			"usage": {
				"input_tokens": 1000,
				"output_tokens": 500,
				"cache_read_input_tokens": 10000,
				"cache_creation_input_tokens": 50000,
				"cache_creation": {
					"ephemeral_5m_input_tokens": 20000,
					"ephemeral_1h_input_tokens": 30000
				}
			}
		}`))
	}))
	defer server.Close()

	executor := runtimeexecutor.NewClaudeExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "claude",
		Attributes: map[string]string{
			"api_key":  "test-claude-key",
			"base_url": server.URL,
		},
	}

	prevQueueEnabled := redisqueue.Enabled()
	prevUsageEnabled := redisqueue.UsageStatisticsEnabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	redisqueue.SetUsageStatisticsEnabled(true)
	t.Cleanup(func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
		redisqueue.SetUsageStatisticsEnabled(prevUsageEnabled)
	})

	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"model":"` + model + `","speed":"fast","messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatClaude,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	payload := waitForQueuedPayload(t, "claude", model)
	if payload.Tokens.InputTokens != 1000 {
		t.Fatalf("InputTokens = %d, want 1000", payload.Tokens.InputTokens)
	}
	if payload.Tokens.CacheReadTokens != 10000 {
		t.Fatalf("CacheReadTokens = %d, want 10000", payload.Tokens.CacheReadTokens)
	}
	if payload.Tokens.CacheCreationTokens != 50000 {
		t.Fatalf("CacheCreationTokens = %d, want 50000", payload.Tokens.CacheCreationTokens)
	}
	if payload.Tokens.CacheCreation1hTokens != 30000 {
		t.Fatalf("CacheCreation1hTokens = %d, want 30000", payload.Tokens.CacheCreation1hTokens)
	}
	if !payload.Tokens.CacheCreationTTLKnown {
		t.Fatalf("CacheCreationTTLKnown = false, want true")
	}
	if payload.ServiceTier != "fast" {
		t.Fatalf("ServiceTier = %q, want fast", payload.ServiceTier)
	}
	if payload.ResponseServiceTier != "fast" {
		t.Fatalf("ResponseServiceTier = %q, want fast", payload.ResponseServiceTier)
	}
}

func TestClaudeStreamExecutorMergesCacheCreationAndOutputTokensInQueue(t *testing.T) {
	model := fmt.Sprintf("claude-stream-test-%d", time.Now().UnixNano())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"" + model + "\",\"speed\":\"fast\",\"usage\":{\"input_tokens\":1000,\"cache_creation_input_tokens\":50000,\"cache_read_input_tokens\":10000,\"cache_creation\":{\"ephemeral_5m_input_tokens\":20000,\"ephemeral_1h_input_tokens\":30000}}}}\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
		_, _ = w.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
		_, _ = w.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":500}}\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
		_, _ = w.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer server.Close()

	executor := runtimeexecutor.NewClaudeExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "claude",
		Attributes: map[string]string{
			"api_key":  "test-claude-key",
			"base_url": server.URL,
		},
	}

	prevQueueEnabled := redisqueue.Enabled()
	prevUsageEnabled := redisqueue.UsageStatisticsEnabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	redisqueue.SetUsageStatisticsEnabled(true)
	t.Cleanup(func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
		redisqueue.SetUsageStatisticsEnabled(prevUsageEnabled)
	})

	streamResult, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"model":"` + model + `","stream":true,"speed":"fast","messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{
		Stream:       true,
		SourceFormat: sdktranslator.FormatClaude,
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	for range streamResult.Chunks {
	}

	payload := waitForQueuedPayload(t, "claude", model)
	if payload.Tokens.InputTokens != 1000 {
		t.Fatalf("InputTokens = %d, want 1000", payload.Tokens.InputTokens)
	}
	if payload.Tokens.CacheReadTokens != 10000 {
		t.Fatalf("CacheReadTokens = %d, want 10000", payload.Tokens.CacheReadTokens)
	}
	if payload.Tokens.CacheCreationTokens != 50000 {
		t.Fatalf("CacheCreationTokens = %d, want 50000", payload.Tokens.CacheCreationTokens)
	}
	if payload.Tokens.CacheCreation1hTokens != 30000 {
		t.Fatalf("CacheCreation1hTokens = %d, want 30000", payload.Tokens.CacheCreation1hTokens)
	}
	if !payload.Tokens.CacheCreationTTLKnown {
		t.Fatalf("CacheCreationTTLKnown = false, want true")
	}
	if payload.Tokens.OutputTokens != 500 {
		t.Fatalf("OutputTokens = %d, want 500", payload.Tokens.OutputTokens)
	}
	if payload.ServiceTier != "fast" {
		t.Fatalf("ServiceTier = %q, want fast", payload.ServiceTier)
	}
}

func TestGeminiExecutorRecordsImageModalityInQueue(t *testing.T) {
	model := fmt.Sprintf("gemini-image-test-%d", time.Now().UnixNano())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"candidates":[{"content":{"role":"model","parts":[{"text":"draw"}]},"finishReason":"STOP"}],
			"usageMetadata":{
				"promptTokenCount":100,
				"candidatesTokenCount":1300,
				"thoughtsTokenCount":20,
				"totalTokenCount":1420,
				"promptTokensDetails":[{"modality":"TEXT","tokenCount":80},{"modality":"IMAGE","tokenCount":20}],
				"candidatesTokensDetails":[{"modality":"TEXT","tokenCount":180},{"modality":"IMAGE","tokenCount":1120}]
			}
		}`))
	}))
	defer server.Close()

	executor := runtimeexecutor.NewGeminiExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key":  "test-upstream-key",
			"base_url": server.URL,
		},
	}

	prevQueueEnabled := redisqueue.Enabled()
	prevUsageEnabled := redisqueue.UsageStatisticsEnabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	redisqueue.SetUsageStatisticsEnabled(true)
	t.Cleanup(func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
		redisqueue.SetUsageStatisticsEnabled(prevUsageEnabled)
	})

	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatGemini,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	payload := waitForQueuedPayload(t, "gemini", model)
	if payload.Tokens.InputTokens != 100 {
		t.Fatalf("InputTokens = %d, want 100", payload.Tokens.InputTokens)
	}
	if payload.Tokens.ImageInputTokens != 20 {
		t.Fatalf("ImageInputTokens = %d, want 20", payload.Tokens.ImageInputTokens)
	}
	if payload.Tokens.ImageOutputTokens != 1120 {
		t.Fatalf("ImageOutputTokens = %d, want 1120", payload.Tokens.ImageOutputTokens)
	}
	if !payload.Tokens.ImageTokensKnown {
		t.Fatalf("ImageTokensKnown = false, want true")
	}
}

func waitForQueuedPayload(t *testing.T, wantProvider, wantModel string) queuedUsagePayload {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		items := redisqueue.PopOldest(10)
		for _, item := range items {
			got, ok := parseQueuedUsagePayload(t, item)
			if !ok {
				continue
			}
			if got.Provider != wantProvider || got.Model != wantModel {
				continue
			}
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for queued usage payload for provider=%q model=%q", wantProvider, wantModel)
	return queuedUsagePayload{}
}

func TestCodexDirectImageRecordsImageOutputTokensInQueue(t *testing.T) {
	model := "gpt-image-2.5"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/images/generations" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"created": 1700000000,
			"model": "gpt-image-2.5",
			"data": [{"b64_json": "AAAA"}],
			"usage": {
				"prompt_tokens": 100,
				"completion_tokens": 50,
				"total_tokens": 150
			}
		}`))
	}))
	defer server.Close()

	executor := runtimeexecutor.NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":  "test-codex-key",
			"base_url": server.URL,
		},
	}

	prevQueueEnabled := redisqueue.Enabled()
	prevUsageEnabled := redisqueue.UsageStatisticsEnabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	redisqueue.SetUsageStatisticsEnabled(true)
	t.Cleanup(func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
		redisqueue.SetUsageStatisticsEnabled(prevUsageEnabled)
	})

	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"model":"gpt-image-2.5","prompt":"a cute cat"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-image"),
		Metadata: map[string]any{
			cliproxyexecutor.RequestPathMetadataKey: "/v1/images/generations",
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	payload := waitForQueuedPayload(t, "codex", model)
	if payload.Tokens.InputTokens != 100 {
		t.Fatalf("InputTokens = %d, want 100", payload.Tokens.InputTokens)
	}
	if payload.Tokens.OutputTokens != 50 {
		t.Fatalf("OutputTokens = %d, want 50", payload.Tokens.OutputTokens)
	}
	if payload.Tokens.ImageOutputTokens != 50 {
		t.Fatalf("ImageOutputTokens = %d, want 50", payload.Tokens.ImageOutputTokens)
	}
	if !payload.Tokens.ImageTokensKnown {
		t.Fatalf("ImageTokensKnown = false, want true")
	}
}

func TestStreamFailurePreservesConsumedTokensInQueue(t *testing.T) {
	model := fmt.Sprintf("stream-fail-%d", time.Now().UnixNano())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		// First frame delivers usage
		_, _ = w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_fail\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"" + model + "\",\"usage\":{\"input_tokens\":500,\"cache_creation_input_tokens\":2000,\"cache_read_input_tokens\":100,\"cache_creation\":{\"ephemeral_1h_input_tokens\":2000}}}}\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
		// Stream disconnects or errors
		conn, _, _ := w.(http.Hijacker).Hijack()
		if conn != nil {
			_ = conn.Close()
		}
	}))
	defer server.Close()

	executor := runtimeexecutor.NewClaudeExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "claude",
		Attributes: map[string]string{
			"api_key":  "test-claude-key",
			"base_url": server.URL,
		},
	}

	prevQueueEnabled := redisqueue.Enabled()
	prevUsageEnabled := redisqueue.UsageStatisticsEnabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	redisqueue.SetUsageStatisticsEnabled(true)
	t.Cleanup(func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
		redisqueue.SetUsageStatisticsEnabled(prevUsageEnabled)
	})

	streamResult, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"model":"` + model + `","stream":true,"messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{
		Stream:       true,
		SourceFormat: sdktranslator.FormatClaude,
	})
	if err == nil {
		for chunk := range streamResult.Chunks {
			_ = chunk
		}
	}

	payload := waitForQueuedPayload(t, "claude", model)
	if !payload.Failed {
		t.Fatalf("payload.Failed = false, want true")
	}
	if payload.Tokens.InputTokens != 500 {
		t.Fatalf("InputTokens = %d, want 500", payload.Tokens.InputTokens)
	}
	if payload.Tokens.CacheCreation1hTokens != 2000 {
		t.Fatalf("CacheCreation1hTokens = %d, want 2000", payload.Tokens.CacheCreation1hTokens)
	}
	if !payload.Tokens.CacheCreationTTLKnown {
		t.Fatalf("CacheCreationTTLKnown = false, want true")
	}
}

func TestGeminiMissingPromptModalitiesRecordsIncompleteKnownInQueue(t *testing.T) {
	model := fmt.Sprintf("gemini-partial-test-%d", time.Now().UnixNano())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"candidates":[{"content":{"role":"model","parts":[{"text":"draw"}]},"finishReason":"STOP"}],
			"usageMetadata":{
				"promptTokenCount":100,
				"candidatesTokenCount":1300,
				"totalTokenCount":1400,
				"candidatesTokensDetails":[{"modality":"TEXT","tokenCount":180},{"modality":"IMAGE","tokenCount":1120}]
			}
		}`))
	}))
	defer server.Close()

	executor := runtimeexecutor.NewGeminiExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key":  "test-upstream-key",
			"base_url": server.URL,
		},
	}

	prevQueueEnabled := redisqueue.Enabled()
	prevUsageEnabled := redisqueue.UsageStatisticsEnabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	redisqueue.SetUsageStatisticsEnabled(true)
	t.Cleanup(func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
		redisqueue.SetUsageStatisticsEnabled(prevUsageEnabled)
	})

	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatGemini,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	payload := waitForQueuedPayload(t, "gemini", model)
	if payload.Tokens.InputTokens != 100 {
		t.Fatalf("InputTokens = %d, want 100", payload.Tokens.InputTokens)
	}
	if payload.Tokens.ImageOutputTokens != 1120 {
		t.Fatalf("ImageOutputTokens = %d, want 1120", payload.Tokens.ImageOutputTokens)
	}
	if payload.Tokens.ImageTokensKnown {
		t.Fatalf("ImageTokensKnown = true, want false because promptTokensDetails was missing")
	}
}

func TestOpenAIPartialImageUsageRecordsIncompleteKnownInQueue(t *testing.T) {
	model := fmt.Sprintf("openai-partial-image-%d", time.Now().UnixNano())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id": "chatcmpl-test",
			"object": "chat.completion",
			"model": "` + model + `",
			"choices": [{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
			"usage": {
				"prompt_tokens": 100,
				"completion_tokens": 50,
				"total_tokens": 150,
				"output_tokens_details": {
					"image_tokens": 50
				}
			}
		}`))
	}))
	defer server.Close()

	executor := runtimeexecutor.NewOpenAICompatExecutor("openai-compat-test", &config.Config{})

	auth := &cliproxyauth.Auth{
		Provider: "openai-compat-test",
		Attributes: map[string]string{
			"api_key":      "test-api-key",
			"base_url":     server.URL,
			"compat_name":  "openai-compat-test",
			"config_index": "0",
		},
	}

	prevQueueEnabled := redisqueue.Enabled()
	prevUsageEnabled := redisqueue.UsageStatisticsEnabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	redisqueue.SetUsageStatisticsEnabled(true)
	t.Cleanup(func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
		redisqueue.SetUsageStatisticsEnabled(prevUsageEnabled)
	})

	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAI,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	payload := waitForQueuedPayload(t, "openai-compat-test", model)
	if payload.Tokens.InputTokens != 100 {
		t.Fatalf("InputTokens = %d, want 100", payload.Tokens.InputTokens)
	}
	if payload.Tokens.ImageOutputTokens != 50 {
		t.Fatalf("ImageOutputTokens = %d, want 50", payload.Tokens.ImageOutputTokens)
	}
	if payload.Tokens.ImageTokensKnown {
		t.Fatalf("ImageTokensKnown = true, want false because input image tokens were missing")
	}
}
