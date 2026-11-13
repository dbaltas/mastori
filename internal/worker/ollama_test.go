package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbaltas/mastori/internal/queue"
)

func sseChunk(content string) string {
	return "data: {\"id\":\"cmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"" + content + "\"}}]}\n\n"
}

func newFakeAdapter(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/completions" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func collectOllamaChunks(t *testing.T, srv *httptest.Server, job queue.Job) ([]queue.Chunk, error) {
	t.Helper()
	g := &OllamaGenerator{BaseURL: srv.URL, Model: "tiny", Client: srv.Client()}
	var chunks []queue.Chunk
	err := g.Run(context.Background(), job, func(c queue.Chunk) error {
		chunks = append(chunks, c)
		return nil
	})
	return chunks, err
}

func TestOllamaRunStreamsChunksAndUsage(t *testing.T) {
	body := sseChunk("the ") + sseChunk("model ") + sseChunk("streams") + "\n" +
		"data: {\"id\":\"cmpl-1\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n" +
		"data: [DONE]\n\n"
	srv := newFakeAdapter(t, http.StatusOK, body)

	chunks, err := collectOllamaChunks(t, srv, queue.Job{ID: "j1", Prompt: "hello", CreatedAt: time.Now().UTC().Add(-5 * time.Millisecond)})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(chunks) != 4 {
		t.Fatalf("chunks = %d, want 3 text + 1 done", len(chunks))
	}
	var sb strings.Builder
	for _, c := range chunks[:3] {
		if c.Done {
			t.Error("text chunk marked done")
		}
		sb.WriteString(c.Text)
	}
	if sb.String() != "the model streams" {
		t.Errorf("text = %q", sb.String())
	}
	last := chunks[3]
	if !last.Done {
		t.Error("final chunk not marked done")
	}
	if last.TokensIn != 7 || last.TokensOut != 3 {
		t.Errorf("tokens = %d/%d, want 7/3", last.TokensIn, last.TokensOut)
	}
	if last.QueueWaitMs < 5 {
		t.Errorf("queue_wait_ms = %d, want >= 5", last.QueueWaitMs)
	}
}

func TestOllamaRunFallsBackToEstimatedTokens(t *testing.T) {
	body := sseChunk("one two three ") + "data: [DONE]\n\n"
	srv := newFakeAdapter(t, http.StatusOK, body)

	chunks, err := collectOllamaChunks(t, srv, queue.Job{ID: "j2", Prompt: "hi", CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	last := chunks[len(chunks)-1]
	if !last.Done {
		t.Error("final chunk not marked done")
	}
	if last.TokensOut != 3 {
		t.Errorf("tokens_out = %d, want estimated 3", last.TokensOut)
	}
}

func TestOllamaRunBackendError(t *testing.T) {
	srv := newFakeAdapter(t, http.StatusServiceUnavailable, "ollama down")
	_, err := collectOllamaChunks(t, srv, queue.Job{ID: "j3", Prompt: "hi", CreatedAt: time.Now().UTC()})
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err = %v, want status 503 error", err)
	}
}

func TestOllamaRunSSEErrorEvent(t *testing.T) {
	body := "data: {\"error\":{\"message\":\"model not found\"}}\n\ndata: [DONE]\n\n"
	srv := newFakeAdapter(t, http.StatusOK, body)
	_, err := collectOllamaChunks(t, srv, queue.Job{ID: "j4", Prompt: "hi", CreatedAt: time.Now().UTC()})
	if err == nil || !strings.Contains(err.Error(), "model not found") {
		t.Fatalf("err = %v, want backend error message", err)
	}
}

func TestOllamaRunPropagatesChunkError(t *testing.T) {
	body := sseChunk("a") + sseChunk("b") + "data: [DONE]\n\n"
	srv := newFakeAdapter(t, http.StatusOK, body)
	g := &OllamaGenerator{BaseURL: srv.URL, Model: "tiny", Client: srv.Client()}
	want := errors.New("redis down")
	err := g.Run(context.Background(), queue.Job{ID: "j5", Prompt: "p", CreatedAt: time.Now().UTC()}, func(c queue.Chunk) error {
		if c.Text == "b" {
			return want
		}
		return nil
	})
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
}

func TestOllamaRunRespectsContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for i := 0; i < 100; i++ {
			_, _ = w.Write([]byte(sseChunk("tok ")))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	t.Cleanup(srv.Close)

	g := &OllamaGenerator{BaseURL: srv.URL, Model: "tiny", Client: srv.Client()}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	emitted := 0
	err := g.Run(ctx, queue.Job{ID: "j6", Prompt: "p", CreatedAt: time.Now().UTC()}, func(c queue.Chunk) error {
		emitted++
		return nil
	})
	if err == nil {
		t.Fatal("expected context error")
	}
	if emitted == 0 {
		t.Error("no chunks emitted before cancel")
	}
}

func TestOllamaModelSelection(t *testing.T) {
	g := &OllamaGenerator{Model: "fallback"}
	if got := g.model(queue.Job{Model: "qwen2.5:0.5b"}); got != "qwen2.5:0.5b" {
		t.Errorf("model = %q, want job model", got)
	}
	if got := g.model(queue.Job{Model: "default"}); got != "fallback" {
		t.Errorf("model = %q, want configured fallback", got)
	}
	if got := g.model(queue.Job{}); got != "fallback" {
		t.Errorf("model = %q, want configured fallback", got)
	}
}
