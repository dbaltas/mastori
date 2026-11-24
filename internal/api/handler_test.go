package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/dbaltas/mastori/internal/config"
	"github.com/dbaltas/mastori/internal/metrics"
	"github.com/dbaltas/mastori/internal/queue"
	"github.com/prometheus/client_golang/prometheus"
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

// setupTraceRecorder installs a real tracer provider writing to a shared span
// recorder, plus the W3C propagator. The provider is installed once per test
// binary: the otel global provider only wires package-level tracers to the
// first installed SDK, so per-test installs would leak spans across tests.
// The recorder is reset so each test sees only its own spans.
var (
	traceRecorderOnce sync.Once
	traceRecorder     *tracetest.SpanRecorder
)

func setupTraceRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prevProp := otel.GetTextMapPropagator()
	traceRecorderOnce.Do(func() {
		traceRecorder = tracetest.NewSpanRecorder()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(traceRecorder)))
	})
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prevProp) })
	traceRecorder.Reset()
	return traceRecorder
}

func findSpan(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func spanAttr(span sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	return ""
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

func TestStreamTTFTSkipsEmptyDoneChunk(t *testing.T) {
	// A stream that only produced the terminal Done chunk (no tokens) must
	// not record its e2e duration as TTFT.
	fq := &fakeQueue{
		chunkFn: func(_ context.Context, id string) (<-chan queue.Chunk, <-chan error) {
			chunks := make(chan queue.Chunk, 1)
			chunks <- queue.Chunk{JobID: id, Text: "", Done: true, TokensOut: 0}
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
	if count, _ := ttftSample(m.Registry); count != 0 {
		t.Errorf("ttft sample count = %d, want 0 (no token ever arrived)", count)
	}
}

func TestGenerateJobFailedMapsTo502(t *testing.T) {
	fq := &fakeQueue{
		waitFn: func(context.Context, string) (queue.Result, error) {
			return queue.Result{}, fmt.Errorf("%w: backend exploded", queue.ErrJobFailed)
		},
	}
	srv := newTestServer(t, fq, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "backend exploded") {
		t.Errorf("body = %s, want worker reason", rec.Body.String())
	}
}

func TestGenerateSyncAbandonsOnTimeout(t *testing.T) {
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
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if len(fq.abandoned) != 1 {
		t.Errorf("abandoned jobs = %v, want 1: sync timeout must abandon", fq.abandoned)
	}
}

func TestGenerateSyncAbandonsOnDisconnect(t *testing.T) {
	fq := &fakeQueue{
		waitFn: func(ctx context.Context, _ string) (queue.Result, error) {
			<-ctx.Done()
			return queue.Result{}, ctx.Err()
		},
	}
	srv := newTestServer(t, fq, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	cancel()
	srv.ServeHTTP(rec, req)

	if len(fq.abandoned) != 1 {
		t.Errorf("abandoned jobs = %v, want 1: client disconnect must abandon", fq.abandoned)
	}
}

func TestStreamDisconnectIsNotTimeout(t *testing.T) {
	// Client disconnects while the request is still queued: the common case
	// under load. The abandon reason must be the disconnect, not a timeout.
	fq := &fakeQueue{
		chunkFn: func(ctx context.Context, _ string) (<-chan queue.Chunk, <-chan error) {
			chunks := make(chan queue.Chunk)
			errCh := make(chan error, 1)
			go func() {
				<-ctx.Done()
				errCh <- context.Canceled
				close(chunks)
			}()
			return chunks, errCh
		},
	}
	srv := newTestServer(t, fq, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/generate/stream", strings.NewReader(`{"prompt": "hi"}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	cancel()
	srv.ServeHTTP(rec, req)

	if len(fq.abandoned) != 1 {
		t.Errorf("abandoned jobs = %v, want 1", fq.abandoned)
	}
	if strings.Contains(rec.Body.String(), "timed out") {
		t.Errorf("body = %s, disconnect must not be labeled as timeout", rec.Body.String())
	}
}

func TestStreamJobFailedCarriesReason(t *testing.T) {
	fq := &fakeQueue{
		chunkFn: func(_ context.Context, id string) (<-chan queue.Chunk, <-chan error) {
			chunks := make(chan queue.Chunk, 1)
			chunks <- queue.Chunk{JobID: id, Text: "", Done: true, Error: "backend exploded"}
			close(chunks)
			errCh := make(chan error)
			return chunks, errCh
		},
	}
	srv := newTestServer(t, fq, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate/stream", strings.NewReader(`{"prompt": "hi"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "backend exploded") {
		t.Errorf("body missing worker reason; body:\n%s", body)
	}
	if strings.Contains(body, "timed out") {
		t.Errorf("body = %s, job failure must not be labeled as timeout", body)
	}
	if len(fq.abandoned) != 1 {
		t.Errorf("abandoned jobs = %v, want 1: failed job must be abandoned", fq.abandoned)
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

const testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestGenerateSpanTreeAndTracePropagation(t *testing.T) {
	rec := setupTraceRecorder(t)
	fq := &fakeQueue{}
	srv := newTestServer(t, fq, 5*time.Second)

	req := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(`{"prompt": "hi"}`))
	req.Header.Set("Traceparent", testTraceparent)
	out := httptest.NewRecorder()
	srv.ServeHTTP(out, req)
	if out.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", out.Code, out.Body.String())
	}

	if len(fq.enqueued) != 1 {
		t.Fatalf("enqueued jobs = %d, want 1", len(fq.enqueued))
	}
	job := fq.enqueued[0]
	tpTrace, _, ok := parseTraceparent(job.Traceparent)
	if !ok {
		t.Fatalf("job traceparent = %q, not a valid traceparent", job.Traceparent)
	}
	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if tpTrace != tid {
		t.Errorf("job traceparent trace ID = %s, want %s (job must carry the request's trace)", tpTrace, tid)
	}

	spans := rec.Ended()
	server := findSpan(spans, "POST /generate")
	if server == nil {
		t.Fatalf("no server span named POST /generate; got: %v", spanNames(spans))
	}
	if got := spanAttr(server, "http.route"); got != "/generate" {
		t.Errorf("server span http.route = %q, want /generate", got)
	}
	api := findSpan(spans, "api.generate")
	if api == nil {
		t.Fatalf("no api.generate span; got: %v", spanNames(spans))
	}
	enq := findSpan(spans, "queue.enqueue")
	if enq == nil {
		t.Fatalf("no queue.enqueue span; got: %v", spanNames(spans))
	}
	wait := findSpan(spans, "queue.wait_result")
	if wait == nil {
		t.Fatalf("no queue.wait_result span; got: %v", spanNames(spans))
	}

	tid, _ = trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	for _, s := range []struct {
		name string
		span sdktrace.ReadOnlySpan
	}{{"server", server}, {"api", api}, {"enqueue", enq}, {"wait", wait}} {
		if s.span.SpanContext().TraceID() != tid {
			t.Errorf("%s span trace ID = %s, want %s (all spans must share the incoming trace)", s.name, s.span.SpanContext().TraceID(), tid)
		}
	}
	if api.Parent().SpanID() != server.SpanContext().SpanID() {
		t.Error("api.generate is not a child of the server span")
	}
	if enq.Parent().SpanID() != api.SpanContext().SpanID() {
		t.Error("queue.enqueue is not a child of api.generate")
	}
	if wait.Parent().SpanID() != api.SpanContext().SpanID() {
		t.Error("queue.wait_result is not a child of api.generate")
	}
	if got := spanAttr(enq, "job.id"); got == "" {
		t.Error("queue.enqueue span missing job.id attribute")
	}
}

func TestStreamSpansAndAbandonOnTimeout(t *testing.T) {
	rec := setupTraceRecorder(t)
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
	req.Header.Set("Traceparent", testTraceparent)
	out := httptest.NewRecorder()
	srv.ServeHTTP(out, req)
	if out.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", out.Code)
	}

	spans := rec.Ended()
	server := findSpan(spans, "POST /generate/stream")
	if server == nil {
		t.Fatalf("no server span named POST /generate/stream; got: %v", spanNames(spans))
	}
	if got := spanAttr(server, "http.route"); got != "/generate/stream" {
		t.Errorf("server span http.route = %q, want /generate/stream", got)
	}

	api := findSpan(spans, "api.generate_stream")
	if api == nil {
		t.Fatalf("no api.generate_stream span; got: %v", spanNames(spans))
	}
	consume := findSpan(spans, "queue.consume_chunks")
	if consume == nil {
		t.Fatalf("no queue.consume_chunks span; got: %v", spanNames(spans))
	}
	if got := spanAttr(consume, "job.timed_out"); got != "true" {
		t.Errorf("queue.consume_chunks job.timed_out = %q, want true", got)
	}
	if got := spanAttr(consume, "stream.chunk_count"); got != "0" {
		t.Errorf("queue.consume_chunks stream.chunk_count = %q, want 0", got)
	}
	abandon := findSpan(spans, "api.abandon")
	if abandon == nil {
		t.Fatalf("no api.abandon span; got: %v", spanNames(spans))
	}
	if got := spanAttr(abandon, "abandon.reason"); got != "timeout" {
		t.Errorf("api.abandon reason = %q, want timeout", got)
	}

	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	for name, s := range map[string]sdktrace.ReadOnlySpan{
		"server": server, "api": api, "consume": consume, "abandon": abandon,
	} {
		if s.SpanContext().TraceID() != tid {
			t.Errorf("%s span trace ID = %s, want %s", name, s.SpanContext().TraceID(), tid)
		}
	}
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, 0, len(spans))
	for _, s := range spans {
		names = append(names, fmt.Sprintf("%q", s.Name()))
	}
	return names
}

// parseTraceparent splits a W3C traceparent into its trace ID.
func parseTraceparent(tp string) (trace.TraceID, trace.SpanID, bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 {
		return trace.TraceID{}, trace.SpanID{}, false
	}
	tid, err := trace.TraceIDFromHex(parts[1])
	if err != nil {
		return trace.TraceID{}, trace.SpanID{}, false
	}
	sid, err := trace.SpanIDFromHex(parts[2])
	if err != nil {
		return trace.TraceID{}, trace.SpanID{}, false
	}
	return tid, sid, true
}
