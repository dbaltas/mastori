package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func promStub(t *testing.T, status string, result any) (*httptest.Server, *[]url.Values) {
	t.Helper()
	var got []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.URL.Query())
		resp := map[string]any{"status": status, "data": map[string]any{"resultType": "matrix", "result": result}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv, &got
}

func TestRunGroupsByMetricName(t *testing.T) {
	result := []series{
		{
			Metric: map[string]string{"__name__": "mastori_api_requests_total", "job": "api"},
			Values: [][2]any{{float64(1700000000), "5"}, {float64(1700000015), "10"}},
		},
		{
			Metric: map[string]string{"__name__": "vllm:generation_tokens_total", "job": "vllm"},
			Values: [][2]any{{float64(1700000000), "1200"}},
		},
		{
			Metric: map[string]string{"__name__": "mastori_api_requests_total", "job": "api", "pod": "api-b"},
			Values: [][2]any{{float64(1700000000), "1"}},
		},
	}
	srv, queries := promStub(t, "success", result)
	start := time.Date(2026, 11, 23, 15, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)
	out := t.TempDir()

	path, err := run(srv.URL, out, start, end, "15s", `vllm:|mastori_`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.HasSuffix(path, "prometheus-20261123-1500-1510.json") {
		t.Errorf("filename = %q, want prometheus-20261123-1500-1510.json", path)
	}

	if len(*queries) != 1 {
		t.Fatalf("queries = %d, want 1", len(*queries))
	}
	q := (*queries)[0]
	if q.Get("query") != `{__name__=~"vllm:|mastori_"}` {
		t.Errorf("query = %q", q.Get("query"))
	}
	if q.Get("start") != start.Format(time.RFC3339) || q.Get("end") != end.Format(time.RFC3339) {
		t.Errorf("window = %s..%s, want %s..%s", q.Get("start"), q.Get("end"), start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	if q.Get("step") != "15s" {
		t.Errorf("step = %q, want 15s", q.Get("step"))
	}

	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var payload map[string][]series
	if err := json.Unmarshal(buf, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(payload) != 2 {
		t.Fatalf("grouped metrics = %d, want 2", len(payload))
	}
	apiSeries := payload["mastori_api_requests_total"]
	if len(apiSeries) != 2 {
		t.Fatalf("api series = %d, want 2", len(apiSeries))
	}
	if _, ok := apiSeries[0].Metric["pod"]; ok {
		t.Errorf("series order = pod-first, want deterministic label sort (job-only series first)")
	}
	if _, ok := payload["DCGM_FI_DEV_GPU_UTIL_RATIO"]; ok {
		t.Errorf("non-matching metric present in output")
	}
}

func TestRunQueryError(t *testing.T) {
	srv, _ := promStub(t, "error", []any{})
	start := time.Now().UTC().Add(-time.Hour)
	if _, err := run(srv.URL, t.TempDir(), start, start.Add(time.Minute), "15s", "vllm:"); err == nil {
		t.Fatal("run succeeded on prometheus error status")
	}
}

func TestRunHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	start := time.Now().UTC().Add(-time.Hour)
	if _, err := run(srv.URL, t.TempDir(), start, start.Add(time.Minute), "15s", "vllm:"); err == nil {
		t.Fatal("run succeeded on HTTP 502")
	}
}

func TestRunBadMatch(t *testing.T) {
	start := time.Now().UTC().Add(-time.Hour)
	if _, err := run("http://localhost:1", t.TempDir(), start, start.Add(time.Minute), "15s", "vllm:("); err == nil {
		t.Fatal("run succeeded with invalid -match regex")
	}
}

func TestRunEmptyResult(t *testing.T) {
	srv, _ := promStub(t, "success", []series{})
	out := t.TempDir()
	start := time.Date(2026, 11, 23, 15, 0, 0, 0, time.UTC)
	path, err := run(srv.URL, out, start, start.Add(time.Minute), "15s", "vllm:")
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
	srv, _ := promStub(t, "success", []series{})
	out := filepath.Join(t.TempDir(), "metrics", "nested")
	start := time.Now().UTC().Add(-time.Hour)
	if _, err := run(srv.URL, out, start, start.Add(time.Minute), "15s", "vllm:"); err != nil {
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
