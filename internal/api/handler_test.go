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

	"github.com/dbaltas/mastori/internal/config"
	"github.com/dbaltas/mastori/internal/metrics"
	"github.com/dbaltas/mastori/internal/queue"
	"log/slog"
)

type fakeQueue struct {
	mu         sync.Mutex
	enqueued   []queue.Job
	enqueueErr error
	waitFn     func(ctx context.Context, id string) (queue.Result, error)
	pingErr    error
}

func (f *fakeQueue) Enqueue(_ context.Context, job queue.Job) error {
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

func (f *fakeQueue) Ping(_ context.Context) error { return f.pingErr }

func (f *fakeQueue) Close() error { return nil }

func newTestServer(t *testing.T, q queue.Queue, timeout time.Duration) http.Handler {
	t.Helper()
	m := metrics.New()
	h := NewHandler(&config.Config{
		RequestTimeout: timeout,
		DefaultModel:   "test-model",
	}, q, m, slog.New(slog.DiscardHandler))
	return NewRouter(&config.Config{}, m, slog.New(slog.DiscardHandler), q.Ping, h)
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
