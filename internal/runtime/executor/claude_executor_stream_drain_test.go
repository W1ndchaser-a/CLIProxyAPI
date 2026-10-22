package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

const drainTestStream = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-20241022\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
	"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
	"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
	"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n" +
	"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

// drainTestUpstream mimics a gateway that finalises after message_stop: it holds the
// response open for tail and reports whether the caller hung up before it ended.
func drainTestUpstream(t *testing.T, beforeStop bool, tail time.Duration) (*httptest.Server, *atomic.Bool, chan struct{}) {
	t.Helper()
	var aborted atomic.Bool
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		stream := drainTestStream
		if beforeStop {
			stream = stream[:strings.Index(stream, "event: content_block_stop")]
		}
		_, _ = w.Write([]byte(stream))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			aborted.Store(true)
		case <-time.After(tail):
		}
	}))
	t.Cleanup(server.Close)
	return server, &aborted, done
}

func drainTestStreamRequest(t *testing.T, ctx context.Context, serverURL string, source string) *cliproxyexecutor.StreamResult {
	t.Helper()
	executor := NewClaudeExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "key-123", "base_url": serverURL}}
	payload := []byte(`{"model":"claude-3-5-sonnet-20241022","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	result, err := executor.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "claude-3-5-sonnet-20241022", Payload: payload},
		cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString(source), Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	return result
}

func waitDrainTestUpstream(t *testing.T, done chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream handler did not finish")
	}
}

func TestClaudeExecutor_ExecuteStreamReadsUpstreamToEndAfterMessageStop(t *testing.T) {
	for _, source := range []string{"claude", "openai"} {
		t.Run(source, func(t *testing.T) {
			server, aborted, done := drainTestUpstream(t, false, 300*time.Millisecond)
			result := drainTestStreamRequest(t, context.Background(), server.URL, source)
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					t.Fatalf("unexpected chunk error: %v", chunk.Err)
				}
			}
			waitDrainTestUpstream(t, done)
			if aborted.Load() {
				t.Fatal("upstream connection closed before the upstream finished its response")
			}
		})
	}
}

func TestClaudeExecutor_ExecuteStreamClientHangUpAfterMessageStopDoesNotCancelUpstream(t *testing.T) {
	server, aborted, done := drainTestUpstream(t, false, 300*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := drainTestStreamRequest(t, ctx, server.URL, "claude")
	for chunk := range result.Chunks {
		if strings.Contains(string(chunk.Payload), "message_stop") {
			cancel()
		}
	}
	waitDrainTestUpstream(t, done)
	if aborted.Load() {
		t.Fatal("a client hang-up after message_stop cancelled the upstream request")
	}
}

func TestClaudeExecutor_ExecuteStreamClientHangUpBeforeMessageStopCancelsUpstream(t *testing.T) {
	server, aborted, done := drainTestUpstream(t, true, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	result := drainTestStreamRequest(t, ctx, server.URL, "claude")
	<-result.Chunks
	started := time.Now()
	cancel()
	for range result.Chunks {
	}
	waitDrainTestUpstream(t, done)
	if !aborted.Load() || time.Since(started) > time.Second {
		t.Fatalf("mid-stream hang-up did not cancel the upstream promptly (aborted=%v after %v)", aborted.Load(), time.Since(started))
	}
}

func TestClaudeExecutor_ExecuteStreamDrainAfterMessageStopIsBounded(t *testing.T) {
	previous := claudeStreamDrainBound
	claudeStreamDrainBound = 200 * time.Millisecond
	t.Cleanup(func() { claudeStreamDrainBound = previous })
	server, aborted, done := drainTestUpstream(t, false, 3*time.Second)
	result := drainTestStreamRequest(t, context.Background(), server.URL, "claude")
	started := time.Now()
	for range result.Chunks {
	}
	waitDrainTestUpstream(t, done)
	if !aborted.Load() || time.Since(started) > 2*time.Second {
		t.Fatalf("drain was not cut off at its bound (aborted=%v after %v)", aborted.Load(), time.Since(started))
	}
}
