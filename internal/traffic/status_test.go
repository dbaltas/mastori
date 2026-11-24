package traffic

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func readSummaryFile(t *testing.T, path string) Summary {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	var s Summary
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	return s
}

func waitForSummaryFile(t *testing.T, path string) Summary {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return readSummaryFile(t, path)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("summary file %s never appeared", path)
	return Summary{}
}

func TestRunValidStatusAndSchemaVersion(t *testing.T) {
	srv := fakeAPI(t, time.Millisecond, http.StatusOK)
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:    map[string]float64{"d": 1},
		Seed:       1,
		Duration:   100 * time.Millisecond,
		Rate:       50,
		Timeout:    5 * time.Second,
		OutputPath: out,
	}

	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.SchemaVersion != SchemaVersion {
		t.Errorf("summary schema_version = %q, want %q", summary.SchemaVersion, SchemaVersion)
	}
	if summary.Status != StatusValid {
		t.Errorf("status = %q, want valid", summary.Status)
	}
	if summary.RunID != cfg.RunID {
		t.Errorf("summary run_id = %q", summary.RunID)
	}

	final := readSummaryFile(t, SummaryPath(out))
	if final.Status != StatusValid || final.SchemaVersion != SchemaVersion {
		t.Errorf("summary file status/schema = %q/%q", final.Status, final.SchemaVersion)
	}
	if final.RunID != cfg.RunID || final.FinishedAt.IsZero() {
		t.Errorf("summary file incomplete: %+v", final)
	}

	for i, rec := range readRecords(t, out) {
		if rec.SchemaVersion != SchemaVersion {
			t.Errorf("record %d schema_version = %q, want %q", i, rec.SchemaVersion, SchemaVersion)
		}
	}
}

func TestRunMeasuredFailureInvalid(t *testing.T) {
	srv := fakeAPI(t, time.Millisecond, http.StatusServiceUnavailable)
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:    map[string]float64{"d": 1},
		Seed:       1,
		Duration:   100 * time.Millisecond,
		Rate:       50,
		Timeout:    5 * time.Second,
		OutputPath: out,
	}

	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Failures == 0 {
		t.Fatal("expected measured failures")
	}
	if summary.Status != StatusInvalid {
		t.Errorf("status = %q, want invalid", summary.Status)
	}
	if s := readSummaryFile(t, SummaryPath(out)); s.Status != StatusInvalid {
		t.Errorf("summary file status = %q, want invalid", s.Status)
	}
}

func TestRunWarmupOnlyFailuresValid(t *testing.T) {
	// The API fails the first batch of requests and succeeds afterwards.
	// Warmup traffic is fully drained before the measured phase starts, so
	// every failing request is a warmup request as long as the failure
	// budget is below the warmup count: warmup failures must not
	// invalidate the run.
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if served.Add(1) <= 25 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"job_id":"j1","text":"ok","tokens_in":10,"tokens_out":20,"total_ms":5}`)
	}))
	t.Cleanup(srv.Close)
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:    map[string]float64{"d": 1},
		Seed:       1,
		Warmup:     200 * time.Millisecond,
		Duration:   150 * time.Millisecond,
		Rate:       200,
		Timeout:    5 * time.Second,
		OutputPath: out,
	}

	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.WarmupRequests == 0 {
		t.Fatal("no warmup requests recorded")
	}
	if summary.Failures != 0 {
		t.Errorf("warmup failures leaked into measured failures: %d", summary.Failures)
	}
	if summary.Status != StatusValid {
		t.Errorf("status = %q, want valid (warmup failures must not invalidate)", summary.Status)
	}
}

func TestRunInterruptionFlagLifecycle(t *testing.T) {
	mr := miniredis.RunT(t)

	out := t.TempDir() + "/results.jsonl"
	srv := fakeAPI(t, time.Millisecond, http.StatusOK)
	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:    map[string]float64{"d": 1},
		Seed:       1,
		Duration:   100 * time.Millisecond,
		Rate:       50,
		Timeout:    5 * time.Second,
		OutputPath: out,
		RedisAddr:  mr.Addr(),
	}

	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Status != StatusValid {
		t.Errorf("status without flag = %q, want valid", summary.Status)
	}

	// A stale flag from a previous incident is snapshotted and cleared at
	// run start: it must not invalidate this run (03-scheduler-core #3 —
	// one reclaim must not mark every run in the flag's remaining TTL
	// invalid), but it stays visible in the summary.
	mr.Set("run:interrupted", "1")
	summary, err = Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run after stale flag: %v", err)
	}
	if summary.Status != StatusValid {
		t.Errorf("status with stale flag = %q, want valid", summary.Status)
	}
	if !summary.InterruptedBeforeRun {
		t.Error("interrupted_before_run not recorded for the stale flag")
	}
	if mr.Exists("run:interrupted") {
		t.Error("stale flag was not cleared at run start")
	}
	if summary.Failures != 0 {
		t.Errorf("expected zero request failures, got %d", summary.Failures)
	}

	// An interruption during the run still invalidates: the flag is set
	// (as the scheduler would on reclaim) while requests are in flight.
	interruptOnce := make(chan struct{})
	close(interruptOnce)
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-interruptOnce:
			mr.Set("run:interrupted", "1")
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"job_id":"j1","text":"ok","tokens_in":10,"tokens_out":20,"total_ms":5}`)
	}))
	t.Cleanup(srv2.Close)
	cfg.URL = srv2.URL

	out2 := t.TempDir() + "/results.jsonl"
	cfg.OutputPath = out2
	summary, err = Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run with mid-run flag: %v", err)
	}
	if summary.Status != StatusInvalid {
		t.Errorf("status with mid-run flag = %q, want invalid", summary.Status)
	}
	if s := readSummaryFile(t, SummaryPath(out2)); s.Status != StatusInvalid {
		t.Errorf("summary file status = %q, want invalid", s.Status)
	}
	if summary.Errors["interrupted"] != 1 {
		t.Errorf("errors[interrupted] = %d, want 1", summary.Errors["interrupted"])
	}
	if summary.InterruptedBeforeRun {
		t.Error("interrupted_before_run must be false when the flag appears mid-run")
	}
}

func TestRunSummaryStaysRunningWhileDriverAlive(t *testing.T) {
	// Crash simulation: while the driver is still working the summary file
	// says running. A driver that dies mid-run leaves exactly this state.
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-stop:
		case <-time.After(time.Minute):
		}
	}))
	t.Cleanup(func() {
		cancel()
		close(stop)
		hang.Close()
	})
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:         hang.URL,
		Rows:        []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:     map[string]float64{"d": 1},
		Seed:        1,
		Duration:    time.Minute,
		Rate:        20,
		MaxInflight: 5,
		Timeout:     300 * time.Millisecond,
		OutputPath:  out,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = Run(ctx, cfg)
	}()

	// Mid-run: the driver has been sending for a while and the summary
	// file must still say running.
	time.Sleep(150 * time.Millisecond)
	s := readSummaryFile(t, SummaryPath(out))
	if s.Status != StatusRunning {
		t.Errorf("mid-run status = %q, want running", s.Status)
	}
	if s.SchemaVersion != SchemaVersion {
		t.Errorf("mid-run schema_version = %q, want %q", s.SchemaVersion, SchemaVersion)
	}
	if s.RunID != cfg.RunID {
		t.Errorf("mid-run run_id = %q", s.RunID)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestSummaryPath(t *testing.T) {
	if got := SummaryPath("benchmark/results/r1.jsonl"); got != "benchmark/results/r1.summary.json" {
		t.Errorf("SummaryPath = %q", got)
	}
	if got := SummaryPath("r1"); got != "r1.summary.json" {
		t.Errorf("SummaryPath without ext = %q", got)
	}
}

func TestWriteSummaryOverwrites(t *testing.T) {
	path := t.TempDir() + "/nested/run.summary.json"
	if err := WriteSummary(path, &Summary{Status: StatusRunning}); err != nil {
		t.Fatalf("WriteSummary: %v", err)
	}
	if err := WriteSummary(path, &Summary{Status: StatusValid}); err != nil {
		t.Fatalf("WriteSummary rewrite: %v", err)
	}
	if s := readSummaryFile(t, path); s.Status != StatusValid {
		t.Errorf("status after rewrite = %q", s.Status)
	}
}
