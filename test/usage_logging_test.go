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

func TestClaudeExecutorEmitsCacheCreationTTLInQueue(t *testing.T) {
	model := fmt.Sprintf("claude-ttl-%d", time.Now().UnixNano())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"msg_ttl","type":"message","role":"assistant","model":"` + model + `","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":100,"output_tokens":10,"cache_read_input_tokens":50,"cache_creation_input_tokens":1000,"cache_creation":{"ephemeral_5m_input_tokens":400,"ephemeral_1h_input_tokens":600}}}`))
	}))
	defer server.Close()

	enableUsageQueueForTest(t)

	executor := runtimeexecutor.NewClaudeExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"api_key":  "test-upstream-key",
		"base_url": server.URL,
	}}
	if _, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("claude")}); err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	got := waitForQueuedUsagePayload(t, "claude", model)
	if got.Tokens.CacheCreationTokens != 1000 {
		t.Fatalf("payload cache creation tokens = %d, want 1000", got.Tokens.CacheCreationTokens)
	}
	if got.Tokens.CacheCreation1hTokens != 600 || !got.Tokens.CacheCreationTTLKnown {
		t.Fatalf("payload cache creation ttl = (1h=%d known=%t), want (600, true)", got.Tokens.CacheCreation1hTokens, got.Tokens.CacheCreationTTLKnown)
	}
	if got.Tokens.TotalTokens != 1160 {
		t.Fatalf("payload total tokens = %d, want 1160", got.Tokens.TotalTokens)
	}
}

func TestClaudeExecutorStreamEmitsCacheCreationTTLInQueue(t *testing.T) {
	model := fmt.Sprintf("claude-ttl-stream-%d", time.Now().UnixNano())
	streamData := "event: message_start\n" +
		`data: {"type":"message_start","message":{"id":"msg_ttl","type":"message","role":"assistant","content":[],"model":"` + model + `","usage":{"input_tokens":100,"output_tokens":1,"cache_read_input_tokens":50,"cache_creation_input_tokens":1000,"cache_creation":{"ephemeral_5m_input_tokens":400,"ephemeral_1h_input_tokens":600}}}}` + "\n\n" +
		"event: content_block_start\n" +
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
		"event: content_block_delta\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}` + "\n\n" +
		"event: content_block_stop\n" +
		`data: {"type":"content_block_stop","index":0}` + "\n\n" +
		"event: message_delta\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":15}}` + "\n\n" +
		"event: message_stop\n" +
		`data: {"type":"message_stop"}` + "\n\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(streamData))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer server.Close()

	enableUsageQueueForTest(t)

	executor := runtimeexecutor.NewClaudeExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"api_key":  "test-upstream-key",
		"base_url": server.URL,
	}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"hi"}],"stream":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("claude")})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
	}

	got := waitForQueuedUsagePayload(t, "claude", model)
	if got.Tokens.CacheCreationTokens != 1000 {
		t.Fatalf("payload cache creation tokens = %d, want 1000 without double counting", got.Tokens.CacheCreationTokens)
	}
	if got.Tokens.CacheCreation1hTokens != 600 || !got.Tokens.CacheCreationTTLKnown {
		t.Fatalf("payload cache creation ttl = (1h=%d known=%t), want (600, true)", got.Tokens.CacheCreation1hTokens, got.Tokens.CacheCreationTTLKnown)
	}
	if got.Tokens.OutputTokens != 15 {
		t.Fatalf("payload output tokens = %d, want 15", got.Tokens.OutputTokens)
	}
	if got.Tokens.TotalTokens != 1165 {
		t.Fatalf("payload total tokens = %d, want 1165", got.Tokens.TotalTokens)
	}
}

func enableUsageQueueForTest(t *testing.T) {
	t.Helper()

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
}

func waitForQueuedUsagePayload(t *testing.T, wantProvider, wantModel string) queuedUsagePayload {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, item := range redisqueue.PopOldest(10) {
			got, ok := parseQueuedUsagePayload(t, item)
			if !ok || got.Provider != wantProvider || got.Model != wantModel {
				continue
			}
			if got.Failed {
				t.Fatalf("payload failed = true, want false")
			}
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for queued usage payload for provider=%q model=%q", wantProvider, wantModel)
	return queuedUsagePayload{}
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
	Provider string `json:"provider"`
	Model    string `json:"model"`
	Failed   bool   `json:"failed"`
	Tokens   struct {
		InputTokens           int64 `json:"input_tokens"`
		OutputTokens          int64 `json:"output_tokens"`
		CacheCreationTokens   int64 `json:"cache_creation_tokens"`
		CacheCreation1hTokens int64 `json:"cache_creation_1h_tokens"`
		CacheCreationTTLKnown bool  `json:"cache_creation_ttl_known"`
		TotalTokens           int64 `json:"total_tokens"`
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
