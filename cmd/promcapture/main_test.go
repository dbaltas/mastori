package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// promStub answers the label-values request with names and every
// query_range request with the series whose __name__ equals the queried
// family, recording all queries (goroutine-safe: handlers run on separate
// goroutines per request).
func promStub(t *testing.T, status string, names []string, result []series) (*httptest.Server, *stubQueries) {
	t.Helper()
	q := &stubQueries{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q.record(r.URL.Query())
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/label/__name__/values") {
			json.NewEncoder(w).Encode(map[string]any{"status": status, "data": map[string]any{"result": names}})
			return
		}
		var family []series
		for _, s := range result {
			if strings.Contains(r.URL.Query().Get("query"), `__name__="`+s.Metric["__name__"]+`"`) {
				family = append(family, s)
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"status": status, "data": map[string]any{"resultType": "matrix", "result": family}})
	}))
	t.Cleanup(srv.Close)
	return srv, q
}

type stubQueries struct {
	mu  sync.Mutex
	got []url.Values
}

func (s *stubQueries) record(v url.Values) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, v)
}

func (s *stubQueries) all() []url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]url.Values{}, s.got...)
}

func TestRunGroupsByMetricName(t *testing.T) {
	names := []string{"DCGM_FI_DEV_GPU_UTIL_RATIO", "mastori_api_requests_total", "vllm:generation_tokens_total"}
	result := []series{
		{
			Metric: map[string]string{"__name__": "mastori_api_requests_total", "job": "api"},
			Values: [][2]any{{float64(1700000000), "5"}, {float64(1700000015), "10"}},
		},
	}
	srv, queries := promStub(t, "success", names, result)
	start := time.Date(2026, 11, 23, 15, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)
	out := t.TempDir()

	path, err := run(srv.URL, out, start, end, "15s", defaultMatch, "run-1")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.HasSuffix(path, "prometheus-20261123T150000-20261123T151000.json") {
		t.Errorf("filename = %q, want prometheus-20261123T150000-20261123T151000.json", path)
	}

	qs := queries.all()
	if len(qs) != len(names)+1 {
		t.Fatalf("queries = %d, want %d (label lookup + one per metric family)", len(qs), len(names)+1)
	}
	// First request resolves the matching names.
	if qs[0].Get("match[]") != fmt.Sprintf(`{__name__=~%q}`, defaultMatch) {
		t.Errorf("match[] = %q", qs[0].Get("match[]"))
	}
	// Each family query covers exactly one metric name.
	for _, q := range qs[1:] {
		if !strings.Contains(q.Get("query"), "__name__=") || strings.Contains(q.Get("query"), "=~") {
			t.Errorf("family query = %q, want a single __name__ equality", q.Get("query"))
		}
		if q.Get("start") != start.Format(time.RFC3339) || q.Get("end") != end.Format(time.RFC3339) {
			t.Errorf("window = %s..%s, want %s..%s", q.Get("start"), q.Get("end"), start.Format(time.RFC3339), end.Format(time.RFC3339))
		}
		if q.Get("step") != "15s" {
			t.Errorf("step = %q, want 15s", q.Get("step"))
		}
	}

	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var payload map[string][]series
	if err := json.Unmarshal(buf, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(payload) != 1 {
		t.Fatalf("grouped metrics = %d, want 1 (the stub returns one series per family query)", len(payload))
	}
	apiSeries := payload["mastori_api_requests_total"]
	if len(apiSeries) != 1 {
		t.Fatalf("api series = %d, want 1", len(apiSeries))
	}
}

// TestDefaultMatchAnchoredNames guards the 2026-11-23 bug: Prometheus
// __name__ regexes are fully anchored, so alternatives without a trailing
// wildcard (the old `vllm:|DCGM_|mastori_`) matched zero series and the
// capture came back empty. Every alternative must end in .*
// (or otherwise cover full names) for the real metric families.
func TestDefaultMatchAnchoredNames(t *testing.T) {
	re, err := regexp.Compile(defaultMatch)
	if err != nil {
		t.Fatalf("default match invalid: %v", err)
	}
	real := []string{
		"vllm:generation_tokens_total",
		"vllm:num_requests_waiting",
		"vllm:time_to_first_token_seconds",
		"DCGM_FI_DEV_GPU_UTIL_RATIO",
		"DCGM_FI_PROF_GR_ENGINE_ACTIVE",
		"mastori_api_inflight_requests",
		"mastori_scheduler_queue_depth",
	}
	for _, name := range real {
		if !re.MatchString(name) {
			t.Errorf("default match does not match %q (anchored __name__ regex?)", name)
		}
	}
}

func TestRunQueryError(t *testing.T) {
	srv, _ := promStub(t, "error", []string{"x"}, []series{})
	start := time.Now().UTC().Add(-time.Hour)
	if _, err := run(srv.URL, t.TempDir(), start, start.Add(time.Minute), "15s", "vllm:", ""); err == nil {
		t.Fatal("run succeeded on prometheus error status")
	}
}

func TestRunHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	start := time.Now().UTC().Add(-time.Hour)
	if _, err := run(srv.URL, t.TempDir(), start, start.Add(time.Minute), "15s", "vllm:", ""); err == nil {
		t.Fatal("run succeeded on HTTP 502")
	}
}

func TestRunBadMatch(t *testing.T) {
	start := time.Now().UTC().Add(-time.Hour)
	if _, err := run("http://localhost:1", t.TempDir(), start, start.Add(time.Minute), "15s", "vllm:(", ""); err == nil {
		t.Fatal("run succeeded with invalid -match regex")
	}
}

func TestRunEmptyResult(t *testing.T) {
	srv, _ := promStub(t, "success", []string{}, []series{})
	out := t.TempDir()
	start := time.Date(2026, 11, 23, 15, 0, 0, 0, time.UTC)
	path, err := run(srv.URL, out, start, start.Add(time.Minute), "15s", "vllm:", "")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "{}" {
		t.Errorf("payload = %q, want {}", buf)
	}
}

func TestRunCreatesOutputDir(t *testing.T) {
	srv, _ := promStub(t, "success", []string{}, []series{})
	out := filepath.Join(t.TempDir(), "metrics", "nested")
	start := time.Now().UTC().Add(-time.Hour)
	if _, err := run(srv.URL, out, start, start.Add(time.Minute), "15s", "vllm:", ""); err != nil {
		t.Fatalf("run: %v", err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatalf("outdir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("files in %s = %d, want 1", out, len(entries))
	}
}

func TestSummaryFlagWindow(t *testing.T) {
	// -summary must take the window from the run summary's measured
	// window, not "now-1h", so the capture covers the run's measured
	// phase (B8).
	srv, queries := promStub(t, "success", []string{"mastori_api_requests_total"}, []series{})
	out := t.TempDir()
	start := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	end := start.Add(5 * time.Minute)
	summaryPath := filepath.Join(t.TempDir(), "run.summary.json")
	body := fmt.Sprintf(`{"run_id":"20261123T100000Z-ab","measured_started_at":%q,"finished_at":%q}`,
		start.Format(time.RFC3339), end.Format(time.RFC3339))
	if err := os.WriteFile(summaryPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := run(srv.URL, out, start, end, "15s", defaultMatch, "20261123T100000Z-ab"); err != nil {
		t.Fatalf("run: %v", err)
	}
	qs := queries.all()
	if len(qs) < 2 {
		t.Fatalf("queries = %d, want >= 2", len(qs))
	}
	if qs[1].Get("start") != start.Format(time.RFC3339) || qs[1].Get("end") != end.Format(time.RFC3339) {
		t.Errorf("window = %s..%s, want the run summary's measured window %s..%s",
			qs[1].Get("start"), qs[1].Get("end"), start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
}

func TestSummaryDecode(t *testing.T) {
	// The runSummary decoder must accept the traffic summary schema.
	var rs runSummary
	start := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	body := fmt.Sprintf(`{"run_id":"x","measured_started_at":%q,"finished_at":%q}`,
		start.Format(time.RFC3339), start.Add(time.Minute).Format(time.RFC3339))
	if err := json.Unmarshal([]byte(body), &rs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rs.RunID != "x" || rs.MeasuredStartedAt != start || rs.FinishedAt != start.Add(time.Minute) {
		t.Errorf("decoded %+v", rs)
	}
}
