package traffic

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRunSmokeCheckPasses(t *testing.T) {
	srv := streamAPI(t, 0, 2*time.Millisecond, 5, http.StatusOK)
	t.Cleanup(srv.Close)

	cfg := &Config{URL: srv.URL, Stream: true, Timeout: 5 * time.Second}
	sc, err := RunSmokeCheck(context.Background(), cfg)
	if err != nil {
		t.Fatalf("smoke check: %v", err)
	}
	if sc.Chunks != 5 {
		t.Errorf("chunks = %d, want 5", sc.Chunks)
	}
	if sc.TTFTMs >= sc.E2EMs {
		t.Errorf("ttft %v not < e2e %v", sc.TTFTMs, sc.E2EMs)
	}
}

func TestRunSmokeCheckRejectsSingleChunkStream(t *testing.T) {
	// The H2 symptom: a stream where every token is dropped and only the
	// Done chunk arrives. TTFT equals e2e and the run must abort.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: chunk\ndata: {\"job_id\":\"j1\",\"text\":\"\",\"done\":true,\"tokens_in\":10,\"tokens_out\":20,\"duration_ms\":50}\n\n"))
		_, _ = w.Write([]byte("event: done\ndata: [DONE]\n\n"))
	}))
	t.Cleanup(srv.Close)

	cfg := &Config{URL: srv.URL, Stream: true, Timeout: 5 * time.Second}
	sc, err := RunSmokeCheck(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected smoke check failure for a single-chunk stream")
	}
	if sc.Chunks != 1 {
		t.Errorf("chunks = %d, want 1", sc.Chunks)
	}
}

func TestRunSmokeCheckRejectsShortOutput(t *testing.T) {
	// An ignore_eos run whose smoke response ends before max_tokens means
	// the requested output length is not being honored.
	srv := streamAPI(t, 0, time.Millisecond, 5, http.StatusOK)
	t.Cleanup(srv.Close)

	cfg := &Config{URL: srv.URL, Stream: true, Timeout: 5 * time.Second, GenerationParams: GenerationParams{MaxTokens: 64, IgnoreEOS: true}}
	if _, err := RunSmokeCheck(context.Background(), cfg); err == nil {
		t.Fatal("expected smoke check failure when tokens_out != max_tokens")
	}
}

func TestRunSmokeCheckRequiresStream(t *testing.T) {
	cfg := &Config{Stream: false, Timeout: time.Second}
	if _, err := RunSmokeCheck(context.Background(), cfg); err == nil {
		t.Fatal("expected error for non-streaming config")
	}
}
