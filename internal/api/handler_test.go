package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/dbaltas/mastori/internal/config"
	"github.com/dbaltas/mastori/internal/metrics"
	"github.com/dbaltas/mastori/internal/queue"
	"log/slog"
)

type fakeQueue struct {
	mu           sync.Mutex
	enqueued     []queue.Job
	enqueueErr   error
	enqueueDelay time.Duration
	waitFn       func(ctx context.Context, id string) (queue.Result, error)
	pingErr      error
	chunkFn      func(ctx context.Context, id string) (<-chan queue.Chunk, <-chan error)
	abandoned    []string
}

func (f *fakeQueue) Enqueue(_ context.Context, job queue.Job) error {
	if f.enqueueDelay > 0 {
		time.Sleep(f.enqueueDelay)
	}
	if f.enqueueErr != nil {
		return f.enqueueErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueued = append(f.enqueued, job)
	return nil
}

func (f *fakeQueue) WaitResult(ctx context.Context, id string) (queue.Result, error) {
	if f.waitFn != nil {
		return f.waitFn(ctx, id)
	}
	return queue.Result{JobID: id, Text: "ok"}, nil
}

func (f *fakeQueue) ConsumeChunks(ctx context.Context, id string) (<-chan queue.Chunk, <-chan error) {
	if f.chunkFn != nil {
		return f.chunkFn(ctx, id)
	}
	chunks := make(chan queue.Chunk, 1)
	chunks <- queue.Chunk{JobID: id, Text: "ok", Done: true, TokensOut: 5}
	close(chunks)
	errCh := make(chan error)
	return chunks, errCh
}

func (f *fakeQueue) Abandon(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.abandoned = append(f.abandoned, id)
	return nil
}

func (f *fakeQueue) enqueuedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.enqueued)
}

func (f *fakeQueue) Ping(_ context.Context) error { return f.pingErr }

func (f *fakeQueue) Close() error { return nil }

type blockingOnlyQueue struct {
	mu       sync.Mutex
	enqueued []queue.Job
}

func (b *blockingOnlyQueue) Enqueue(_ context.Context, job queue.Job) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.enqueued = append(b.enqueued, job)
	return nil
}

func (*blockingOnlyQueue) WaitResult(_ context.Context, id string) (queue.Result, error) {
	return queue.Result{JobID: id, Text: "ok"}, nil
}

func (*blockingOnlyQueue) Ping(_ context.Context) error { return nil }

func (*blockingOnlyQueue) Close() error { return nil }

func newTestServer(t *testing.T, q queue.Queue, timeout time.Duration) http.Handler {
	t.Helper()
	m := metrics.New()
	h := NewHandler(&config.Config{
		RequestTimeout: timeout,
		MaxInflight:    1000,
		DefaultModel:   "test-model",
	}, q, m, slog.New(slog.DiscardHandler))
	return NewRouter(&config.Config{}, m, slog.New(slog.DiscardHandler), q.Ping, h)
}

func TestInflightCap(t *testing.T) {
	fq := &fakeQueue{
		waitFn: func(ctx context.Context, _ string) (queue.Result, error) {
			select {
			case <-ctx.Done():
				return queue.Result{}, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return queue.Result{Text: "ok"}, nil
			}
		},
	}
	h := NewHandler(&config.Config{
		RequestTimeout: 5 * time.Second,
		MaxInflight:    1,
		DefaultModel:   "test-model",
	}, fq, metrics.New(), slog.New(slog.DiscardHandler))
	srv := NewRouter(&config.Config{}, metrics.New(), slog.New(slog.DiscardHandler), fq.Ping, h)

	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`))
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		recCh <- rec
	}()

	time.Sleep(100 * time.Millisecond)

	req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("second request status = %d, want 429", rec.Code)
	}
	if got := fq.enqueuedCount(); got != 1 {
		t.Errorf("rejected request should not be enqueued; enqueued = %d", got)
	}

	rec1 := <-recCh
	if rec1.Code != http.StatusOK {
		t.Errorf("first request status = %d, want 200", rec1.Code)
	}
}

func TestGenerateSuccess(t *testing.T) {
	fq := &fakeQueue{}
	srv := newTestServer(t, fq, 5*time.Second)

	body := `{"prompt": "hello", "model": "m1"}`
	req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}

	var resp generateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not valid JSON: %v", err)
	}
	if resp.Text != "ok" {
		t.Errorf("text = %q, want %q", resp.Text, "ok")
	}
	if resp.JobID == "" {
		t.Error("job_id is empty")
	}

	if len(fq.enqueued) != 1 {
		t.Fatalf("enqueued jobs = %d, want 1", len(fq.enqueued))
	}
	job := fq.enqueued[0]
	if job.Prompt != "hello" || job.Model != "m1" {
		t.Errorf("enqueued job = %+v, want prompt=hello model=m1", job)
	}
}

func TestGenerateDefaultModel(t *testing.T) {
	fq := &fakeQueue{}
	srv := newTestServer(t, fq, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if fq.enqueued[0].Model != "test-model" {
		t.Errorf("model = %q, want default %q", fq.enqueued[0].Model, "test-model")
	}
}

func TestGenerateInvalidBody(t *testing.T) {
	fq := &fakeQueue{}
	srv := newTestServer(t, fq, 5*time.Second)

	cases := []struct {
		name string
		body string
	}{
		{"bad json", `{"prompt": `},
		{"empty prompt", `{"prompt": ""}`},
		{"missing prompt", `{"model": "m"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
			if len(fq.enqueued) != 0 {
				t.Errorf("unexpected enqueue on invalid request")
			}
		})
	}
}

func TestGenerateEnqueueError(t *testing.T) {
	fq := &fakeQueue{enqueueErr: errors.New("redis down")}
	srv := newTestServer(t, fq, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

func TestGenerateWaitTimeout(t *testing.T) {
	fq := &fakeQueue{
		waitFn: func(ctx context.Context, _ string) (queue.Result, error) {
			<-ctx.Done()
			return queue.Result{}, ctx.Err()
		},
	}
	srv := newTestServer(t, fq, 50*time.Millisecond)

	req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", rec.Code)
	}
}

func TestGenerateWaitError(t *testing.T) {
	fq := &fakeQueue{
		waitFn: func(context.Context, string) (queue.Result, error) {
			return queue.Result{}, errors.New("sub failed")
		},
	}
	srv := newTestServer(t, fq, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

func TestHealthEndpoints(t *testing.T) {
	t.Run("readyz ok", func(t *testing.T) {
		srv := newTestServer(t, &fakeQueue{}, 5*time.Second)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusOK {
			t.Errorf("readyz status = %d, want 200", rec.Code)
		}
	})

	t.Run("readyz ping fails", func(t *testing.T) {
		srv := newTestServer(t, &fakeQueue{pingErr: errors.New("down")}, 5*time.Second)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("readyz status = %d, want 503", rec.Code)
		}
	})

	t.Run("healthz always ok", func(t *testing.T) {
		srv := newTestServer(t, &fakeQueue{pingErr: errors.New("down")}, 5*time.Second)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if rec.Code != http.StatusOK {
			t.Errorf("healthz status = %d, want 200", rec.Code)
		}
	})
}

func TestStreamSuccess(t *testing.T) {
	fq := &fakeQueue{
		chunkFn: func(_ context.Context, id string) (<-chan queue.Chunk, <-chan error) {
			chunks := make(chan queue.Chunk, 3)
			chunks <- queue.Chunk{JobID: id, Text: "hel"}
			chunks <- queue.Chunk{JobID: id, Text: "lo"}
			chunks <- queue.Chunk{JobID: id, Text: "", Done: true, TokensIn: 2, TokensOut: 5, QueueWaitMs: 1, DurationMs: 9}
			close(chunks)
			errCh := make(chan error)
			return chunks, errCh
		},
	}
	srv := newTestServer(t, fq, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate/stream", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %q, want text/event-stream", ct)
	}

	body := rec.Body.String()
	for _, want := range []string{"event: chunk", `"text":"hel"`, `"text":"lo"`, `"done":true`, `"tokens_out":5`, "event: done", "data: [DONE]"} {
		if !strings.Contains(body, want) {
			t.Errorf("stream body missing %q; body:\n%s", want, body)
		}
	}
	if len(fq.abandoned) != 0 {
		t.Errorf("unexpected abandon on successful stream: %v", fq.abandoned)
	}
}

func TestStreamTimeout(t *testing.T) {
	fq := &fakeQueue{
		chunkFn: func(ctx context.Context, _ string) (<-chan queue.Chunk, <-chan error) {
			chunks := make(chan queue.Chunk)
			errCh := make(chan error, 1)
			go func() {
				<-ctx.Done()
				errCh <- ctx.Err()
				close(chunks)
			}()
			return chunks, errCh
		},
	}
	srv := newTestServer(t, fq, 50*time.Millisecond)

	req := httptest.NewRequest(http.MethodPost, "/generate/stream", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (SSE headers already sent)", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "timed out") {
		t.Errorf("stream body missing error event; body:\n%s", body)
	}
	if len(fq.abandoned) != 1 {
		t.Errorf("job not abandoned after timeout")
	}
}

func TestStreamQueueError(t *testing.T) {
	fq := &fakeQueue{
		chunkFn: func(_ context.Context, _ string) (<-chan queue.Chunk, <-chan error) {
			errCh := make(chan error, 1)
			errCh <- errors.New("redis died")
			chunks := make(chan queue.Chunk)
			close(chunks)
			return chunks, errCh
		},
	}
	srv := newTestServer(t, fq, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate/stream", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "event: error") {
		t.Errorf("stream body missing error event; body:\n%s", body)
	}
	if len(fq.abandoned) != 1 {
		t.Errorf("job not abandoned after queue error")
	}
}

func TestStreamNotSupportedByQueue(t *testing.T) {
	srv := newTestServer(t, &blockingOnlyQueue{}, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate/stream", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

func TestStreamInvalidBody(t *testing.T) {
	fq := &fakeQueue{}
	srv := newTestServer(t, fq, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate/stream", strings.NewReader(`{"prompt": ""}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if len(fq.enqueued) != 0 {
		t.Errorf("unexpected enqueue on invalid request")
	}
}

func TestStreamTTFTIncludesParseAndEnqueue(t *testing.T) {
	fq := &fakeQueue{
		enqueueDelay: 100 * time.Millisecond,
		chunkFn: func(_ context.Context, id string) (<-chan queue.Chunk, <-chan error) {
			chunks := make(chan queue.Chunk, 1)
			chunks <- queue.Chunk{JobID: id, Text: "ok", Done: true, TokensOut: 1}
			close(chunks)
			errCh := make(chan error)
			return chunks, errCh
		},
	}
	m := metrics.New()
	h := NewHandler(&config.Config{
		RequestTimeout: 5 * time.Second,
		MaxInflight:    1000,
		DefaultModel:   "test-model",
	}, fq, m, slog.New(slog.DiscardHandler))
	srv := NewRouter(&config.Config{}, m, slog.New(slog.DiscardHandler), fq.Ping, h)

	req := httptest.NewRequest(http.MethodPost, "/generate/stream", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}

	count, sum := ttftSample(m.Registry)
	if count != 1 {
		t.Fatalf("ttft sample count = %d, want 1", count)
	}
	if sum < 0.05 {
		t.Errorf("ttft = %.6fs, want >= 50ms: metric must cover parse and enqueue time from request receipt", sum)
	}
}

func ttftSample(reg *prometheus.Registry) (uint64, float64) {
	families, err := reg.Gather()
	if err != nil {
		return 0, 0
	}
	for _, mf := range families {
		if mf.GetName() != "mastori_api_ttft_seconds" {
			continue
		}
		for _, ms := range mf.GetMetric() {
			if hist := ms.GetHistogram(); hist != nil {
				return hist.GetSampleCount(), hist.GetSampleSum()
			}
		}
	}
	return 0, 0
}

func TestMetricsExposed(t *testing.T) {
	srv := newTestServer(t, &fakeQueue{}, 5*time.Second)

	srv.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`)))

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"mastori_api_jobs_enqueued_total", "mastori_api_requests_total"} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %s", want)
		}
	}
}
