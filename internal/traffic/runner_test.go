package traffic

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	rows := []Row{{Dataset: "d", Index: 0, Text: "x"}}

	base := Config{URL: "http://x", Rows: rows, Duration: time.Second, Rate: 10}
	if err := base.Validate(); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}

	noURL := base
	noURL.URL = ""
	if err := noURL.Validate(); err == nil {
		t.Error("missing URL should fail")
	}

	noRows := base
	noRows.Rows = nil
	if err := noRows.Validate(); err == nil {
		t.Error("no rows should fail")
	}

	noDuration := base
	noDuration.Duration = 0
	if err := noDuration.Validate(); err == nil {
		t.Error("zero duration should fail")
	}

	noRate := base
	noRate.Rate = 0
	if err := noRate.Validate(); err == nil {
		t.Error("open loop without rate should fail")
	}

	closed := base
	closed.Rate = 0
	closed.Concurrency = 5
	if err := closed.Validate(); err != nil {
		t.Errorf("closed loop without rate should pass: %v", err)
	}

	badArrival := base
	badArrival.Arrival = "uniform"
	if err := badArrival.Validate(); err == nil {
		t.Error("bad arrival should fail")
	}

	okArrival := base
	okArrival.Arrival = "poisson"
	if err := okArrival.Validate(); err != nil {
		t.Errorf("poisson should pass: %v", err)
	}
}

func TestInflightLimit(t *testing.T) {
	cfg := &Config{Rate: 10}
	if got := cfg.inflightLimit(); got != 40 {
		t.Errorf("default inflight = %d, want 40", got)
	}
	cfg = &Config{Rate: 10, MaxInflight: 7}
	if got := cfg.inflightLimit(); got != 7 {
		t.Errorf("explicit inflight = %d, want 7", got)
	}
	cfg = &Config{Concurrency: 5}
	if got := cfg.inflightLimit(); got != 5 {
		t.Errorf("closed loop inflight = %d, want 5", got)
	}
}

func fakeAPI(t *testing.T, delay time.Duration, status int) *httptest.Server {
	t.Helper()
	var cur atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur.Add(1)
		defer cur.Add(-1)
		time.Sleep(delay)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"job_id":"j1","text":"ok","tokens_in":10,"tokens_out":20,"total_ms":5}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRunOpenLoop(t *testing.T) {
	srv := fakeAPI(t, 5*time.Millisecond, http.StatusOK)
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "short", Index: 0, Text: "p1"}, {Dataset: "long", Index: 1, Text: "p2"}},
		Weights:    map[string]float64{"short": 1, "long": 1},
		Seed:       1,
		Duration:   300 * time.Millisecond,
		Rate:       100,
		Timeout:    5 * time.Second,
		OutputPath: out,
	}

	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Requests == 0 {
		t.Error("no requests recorded")
	}
	if summary.Successes != summary.Requests {
		t.Errorf("successes %d != requests %d", summary.Successes, summary.Requests)
	}
	if summary.RequestedRate != 100 {
		t.Errorf("requested rate = %v", summary.RequestedRate)
	}
	if summary.LatencyMsP50 <= 0 {
		t.Errorf("p50 latency = %v", summary.LatencyMsP50)
	}
	if summary.ByStatus[200] != summary.Requests {
		t.Errorf("by_status wrong: %v", summary.ByStatus)
	}
	if summary.Datasets["short"] == 0 || summary.Datasets["long"] == 0 {
		t.Errorf("both datasets should appear: %v", summary.Datasets)
	}

	// Results file is valid JSONL with the expected fields.
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read results: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != summary.WarmupRequests+summary.Requests {
		t.Errorf("file lines %d != warmup+measured %d", len(lines), summary.WarmupRequests+summary.Requests)
	}
	var rec Record
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if rec.RunID == "" || rec.SendAt.IsZero() {
		t.Errorf("record incomplete: %+v", rec)
	}
}

func TestRunWarmupExcluded(t *testing.T) {
	srv := fakeAPI(t, time.Millisecond, http.StatusOK)
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:    map[string]float64{"d": 1},
		Seed:       1,
		Warmup:     200 * time.Millisecond,
		Duration:   200 * time.Millisecond,
		Rate:       100,
		Timeout:    5 * time.Second,
		OutputPath: out,
	}

	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.WarmupRequests == 0 {
		t.Error("warmup requests not tagged")
	}
	// Measured window counts only post-warmup sends.
	if float64(summary.Requests)/cfg.Duration.Seconds() > cfg.Rate*1.5 {
		t.Errorf("measured rate %.1f too high vs requested %.1f", summary.AchievedSendRate, cfg.Rate)
	}
}

func TestRunBackpressureRespectsMaxInflight(t *testing.T) {
	// Server is slow (50ms) while rate is high: in-flight must stay capped.
	srv := fakeAPI(t, 50*time.Millisecond, http.StatusOK)
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:         srv.URL,
		Rows:        []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:     map[string]float64{"d": 1},
		Seed:        1,
		Duration:    300 * time.Millisecond,
		Rate:        500,
		MaxInflight: 3,
		Timeout:     5 * time.Second,
		OutputPath:  out,
	}

	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// With a 3-slot cap and a ~50ms server, completions in 300ms are bounded
	// by slots x (duration/delay + slack) — far below the ~hundreds that an
	// uncapped 500 req/s schedule would finish.
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		t.Fatal("no records written")
	}
	if n := len(lines); n > 3*(300/50+2) {
		t.Errorf("too many completions (%d) for a 3-slot cap on a 50ms server", n)
	}
}

func TestRunClosedLoop(t *testing.T) {
	srv := fakeAPI(t, 5*time.Millisecond, http.StatusOK)
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:         srv.URL,
		Rows:        []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:     map[string]float64{"d": 1},
		Seed:        1,
		Duration:    200 * time.Millisecond,
		Concurrency: 4,
		Rate:        0,
		Timeout:     5 * time.Second,
		OutputPath:  out,
	}

	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Mode != "closed-loop:4" {
		t.Errorf("mode = %s", summary.Mode)
	}
	if summary.Requests < 20 {
		t.Errorf("closed loop with 4 workers x 5ms should exceed 20 requests, got %d", summary.Requests)
	}
}

func TestRunRecordsFailures(t *testing.T) {
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
	if summary.Successes != 0 {
		t.Errorf("expected no successes, got %d", summary.Successes)
	}
	if summary.Failures != summary.Requests {
		t.Errorf("failures %d != requests %d", summary.Failures, summary.Requests)
	}
	if summary.ByStatus[503] != summary.Requests {
		t.Errorf("by_status should count 503s: %v", summary.ByStatus)
	}
	if len(summary.Errors) == 0 {
		t.Error("errors map should record the error message")
	}
}

func TestRunRecordTransportError(t *testing.T) {
	// Server that closes immediately: transport errors recorded, run still completes.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:    map[string]float64{"d": 1},
		Seed:       1,
		Duration:   100 * time.Millisecond,
		Rate:       50,
		Timeout:    2 * time.Second,
		OutputPath: out,
	}

	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Failures == 0 {
		t.Error("transport failures should be recorded")
	}
}

func TestRunStreaming(t *testing.T) {
	// SSE server matching the API stream contract.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(10 * time.Millisecond) // gap between headers and first chunk -> measurable TTFT
		for i := 0; i < 3; i++ {
			done := i == 2
			usage := ""
			if done {
				usage = `,"tokens_in":10,"tokens_out":20,"duration_ms":5`
			}
			fmt.Fprintf(w, "event: chunk\ndata: {\"text\":\"t%d\",\"done\":%t%s}\n\n", i, done, usage)
			w.(http.Flusher).Flush()
		}
		fmt.Fprint(w, "event: done\ndata: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:    map[string]float64{"d": 1},
		Seed:       1,
		Duration:   200 * time.Millisecond,
		Rate:       50,
		Stream:     true,
		Timeout:    5 * time.Second,
		OutputPath: out,
	}

	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Mode != "open-loop+stream" {
		t.Errorf("mode = %s", summary.Mode)
	}
	if summary.Successes == 0 {
		t.Fatal("no successes")
	}
	if summary.TTFTMsP50 <= 0 {
		t.Errorf("TTFT p50 = %v", summary.TTFTMsP50)
	}

	// Records carry ttft_ms and chunks.
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var rec Record
	if err := json.Unmarshal([]byte(strings.Split(strings.TrimSpace(string(data)), "\n")[0]), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.TTFTMs <= 0 || rec.Chunks != 3 {
		t.Errorf("stream fields missing: %+v", rec)
	}
}

func TestRunHangServerBoundedBySendTimeout(t *testing.T) {
	// Server accepts connections but never responds: each send must time
	// out at cfg.Timeout and the run must still exit with a partial summary.
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Minute):
		}
	}))
	t.Cleanup(hang.Close)
	out := t.TempDir() + "/results.jsonl"

	cfg := &Config{
		URL:         hang.URL,
		Rows:        []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:     map[string]float64{"d": 1},
		Seed:        1,
		Duration:    150 * time.Millisecond,
		Rate:        20,
		MaxInflight: 10,
		Timeout:     300 * time.Millisecond,
		OutputPath:  out,
	}

	start := time.Now()
	summary, err := Run(context.Background(), cfg)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("run took %v, send timeouts should bound it to ~1s", elapsed)
	}
	if summary.Failures != summary.Requests {
		t.Errorf("all requests should fail: %d/%d", summary.Failures, summary.Requests)
	}
}

func TestContextCancelStopsRun(t *testing.T) {
	srv := fakeAPI(t, 10*time.Millisecond, http.StatusOK)
	out := t.TempDir() + "/results.jsonl"

	ctx, cancel := context.WithCancel(context.Background())
	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:    map[string]float64{"d": 1},
		Seed:       1,
		Duration:   time.Minute,
		Rate:       100,
		Timeout:    5 * time.Second,
		OutputPath: out,
	}

	done := make(chan struct{})
	var summary *Summary
	var err error
	go func() {
		summary, err = Run(ctx, cfg)
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
	if err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
	if summary.Requests == 0 {
		t.Error("expected partial results before cancel")
	}
}
