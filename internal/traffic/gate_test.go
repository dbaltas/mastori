package traffic

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

const fakeSchedulerMetrics = `# HELP mastori_scheduler_workers_healthy Workers with a live heartbeat.
# TYPE mastori_scheduler_workers_healthy gauge
mastori_scheduler_workers_healthy 2
# HELP mastori_scheduler_dispatched_total Jobs dispatched, by selected worker.
# TYPE mastori_scheduler_dispatched_total counter
mastori_scheduler_dispatched_total{worker="vllmworker-1"} 3131
mastori_scheduler_dispatched_total{worker="vllmworker-2"} 1565
mastori_scheduler_dispatch_errors_total{reason="no_worker"} 4
`

func TestCheckWorkersHealthyAcceptsExpectedCount(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			t.Errorf("path = %q, want /metrics", r.URL.Path)
		}
		_, _ = w.Write([]byte(fakeSchedulerMetrics))
	}))
	t.Cleanup(srv.Close)

	healthy, err := CheckWorkersHealthy(context.Background(), srv.URL, 2)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if healthy != 2 {
		t.Errorf("healthy = %d, want 2", healthy)
	}
}

func TestCheckWorkersHealthyRejectsMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(fakeSchedulerMetrics))
	}))
	t.Cleanup(srv.Close)

	// The H1 scenario: two stray mock workers inflate the fleet.
	healthy, err := CheckWorkersHealthy(context.Background(), srv.URL, 1)
	if err == nil {
		t.Fatal("expected gate failure for workers_healthy = 2, want 1")
	}
	if healthy != 2 {
		t.Errorf("observed healthy = %d, want 2", healthy)
	}
}

func TestCheckWorkersHealthyFailsClosed(t *testing.T) {
	t.Run("missing metric", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("# nothing relevant\n"))
		}))
		t.Cleanup(srv.Close)
		if _, err := CheckWorkersHealthy(context.Background(), srv.URL, 1); err == nil {
			t.Error("expected error when the gauge is absent")
		}
	})
	t.Run("scrape error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		t.Cleanup(srv.Close)
		if _, err := CheckWorkersHealthy(context.Background(), srv.URL, 1); err == nil {
			t.Error("expected error when the scheduler is unreachable")
		}
	})
}

func TestFetchDispatchedByWorker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(fakeSchedulerMetrics))
	}))
	t.Cleanup(srv.Close)

	dispatched, err := FetchDispatchedByWorker(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(dispatched) != 2 {
		t.Fatalf("workers = %v, want exactly the dispatched_total labels", dispatched)
	}
	if dispatched["vllmworker-1"] != 3131 || dispatched["vllmworker-2"] != 1565 {
		t.Errorf("dispatched = %v, want 3131/1565", dispatched)
	}
	// Labeled counters of other families must not leak into the map.
	if _, leak := dispatched["no_worker"]; leak {
		t.Errorf("dispatch_errors_total leaked into dispatched map: %v", dispatched)
	}
}
