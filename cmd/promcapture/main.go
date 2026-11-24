// promcapture exports a window of Prometheus series into the benchmark
// results layout: one JSON file keyed by metric name, each series carrying
// its labels and query_range values.
//
// It is the committed replacement for the hand-run export behind
// benchmark/results/20261122-eks-gpu-vllm-knee/metrics/, so a formal rerun
// captures the vllm:* engine counters (prompt/generation token totals,
// TTFT/TPOT histograms) alongside the DCGM and API series — the authoritative
// cross-check against client-side tok/s figures. Run it against a reachable
// Prometheus, e.g. behind `kubectl -n mastori port-forward deploy/prometheus
// 9091:9090`, with -url http://localhost:9091.
//
// A run's window and run id can be taken straight from the run summary with
// -summary <path.summary.json>, so the captured metrics window is the run's
// measured window rather than "whatever the last hour was".
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type series struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"` // [unix ts, sample string] pairs
}

type rangeResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string   `json:"resultType"`
		Result     []series `json:"result"`
	} `json:"data"`
	Warnings []string `json:"warnings"`
}

type namesResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []string `json:"result"`
	} `json:"data"`
	Warnings []string `json:"warnings"`
}

// runSummary is the subset of a traffic run summary promcapture consumes.
type runSummary struct {
	RunID             string    `json:"run_id"`
	MeasuredStartedAt time.Time `json:"measured_started_at"`
	FinishedAt        time.Time `json:"finished_at"`
}

// defaultMatch selects the series captured per run: vllm:* engine counters,
// DCGM_* GPU telemetry, and mastori_* platform gauges. Prometheus __name__
// regexes are fully anchored, so each alternative must end in .* — the
// pre-fix `vllm:|DCGM_|mastori_` matched zero series and captured nothing.
const defaultMatch = `vllm:.*|DCGM_.*|mastori_.*`

func main() {
	urlFlag := flag.String("url", "http://localhost:9091", "Prometheus base URL")
	outFlag := flag.String("out", "", "output directory (default: benchmark/results/<EXPERIMENT>/metrics via EXPERIMENT env)")
	startFlag := flag.String("start", "", "window start, RFC3339 UTC (default: end-1h, or the run summary's measured window with -summary)")
	endFlag := flag.String("end", "", "window end, RFC3339 UTC (default: now, or the run summary's finished_at with -summary)")
	summaryFlag := flag.String("summary", "", "path to a traffic run summary: take the window (measured_started_at..finished_at) and the run id from it")
	stepFlag := flag.String("step", "15s", "query_range step (set to the Prometheus scrape interval for raw-resolution captures)")
	// Prometheus __name__ regexes are fully anchored, so each alternative
	// must end in .* to match a series name.
	matchFlag := flag.String("match", defaultMatch, "regex matched against __name__")
	flag.Parse()

	out := *outFlag
	if out == "" {
		exp := os.Getenv("EXPERIMENT")
		if exp == "" {
			log.Fatal("promcapture: set EXPERIMENT or pass -out")
		}
		out = filepath.Join("benchmark", "results", exp, "metrics")
	}

	end := time.Time{}
	if *endFlag != "" {
		t, err := time.Parse(time.RFC3339, *endFlag)
		if err != nil {
			log.Fatalf("promcapture: -end: %v", err)
		}
		end = t.UTC()
	}
	start := time.Time{}
	if *startFlag != "" {
		t, err := time.Parse(time.RFC3339, *startFlag)
		if err != nil {
			log.Fatalf("promcapture: -start: %v", err)
		}
		start = t.UTC()
	}

	runID := ""
	if *summaryFlag != "" {
		data, err := os.ReadFile(*summaryFlag)
		if err != nil {
			log.Fatalf("promcapture: -summary: %v", err)
		}
		var rs runSummary
		if err := json.Unmarshal(data, &rs); err != nil {
			log.Fatalf("promcapture: -summary: decode: %v", err)
		}
		if rs.RunID != "" {
			runID = rs.RunID
		}
		if !rs.MeasuredStartedAt.IsZero() && start.IsZero() {
			start = rs.MeasuredStartedAt.UTC()
		}
		if !rs.FinishedAt.IsZero() && end.IsZero() {
			end = rs.FinishedAt.UTC()
		}
	}
	if end.IsZero() {
		end = time.Now().UTC()
	}
	if start.IsZero() {
		start = end.Add(-time.Hour)
	}
	if start.After(end) {
		log.Fatal("promcapture: -start is after -end")
	}

	if _, err := run(*urlFlag, out, start, end, *stepFlag, *matchFlag, runID); err != nil {
		log.Fatalf("promcapture: %v", err)
	}
}

// run first resolves the metric names matching the __name__ regex (the label
// API returns names, not series), then issues one query_range per metric
// family. Querying per family keeps every individual query's sample count
// bounded: one query across every series can exceed the server's
// max_samples limit exactly on the long windows a benchmark cares about.
// The result is grouped by metric name and written as
// prometheus-<start>-<end>.json (UTC, second precision) into out. It
// returns the written file path.
func run(baseURL, out string, start, end time.Time, step, match, runID string) (string, error) {
	if _, err := regexp.Compile(match); err != nil {
		return "", fmt.Errorf("-match: %w", err)
	}
	client := &http.Client{Timeout: 2 * time.Minute}
	base := strings.TrimSuffix(baseURL, "/")

	names, err := matchingNames(client, base, match)
	if err != nil {
		return "", err
	}

	grouped := map[string][]series{}
	for _, name := range names {
		ss, err := queryRange(client, base, name, start, end, step)
		if err != nil {
			return "", err
		}
		if len(ss) > 0 {
			grouped[name] = ss
		}
	}
	names = make([]string, 0, len(grouped))
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)

	payload := map[string]any{}
	for _, name := range names {
		ss := grouped[name]
		sort.Slice(ss, func(i, j int) bool {
			return ss[i].Metric["__name__"]+labelKey(ss[i].Metric) < ss[j].Metric["__name__"]+labelKey(ss[j].Metric)
		})
		payload[name] = ss
	}

	startStr := start.UTC().Format("20060102T150405")
	endStr := end.UTC().Format("20060102T150405")
	file := fmt.Sprintf("prometheus-%s-%s.json", startStr, endStr)
	if err := os.MkdirAll(out, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(out, file)
	buf, err := json.MarshalIndent(payload, "", " ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		return "", err
	}
	seriesCount := 0
	for _, ss := range grouped {
		seriesCount += len(ss)
	}
	runNote := ""
	if runID != "" {
		runNote = " run " + runID
	}
	fmt.Printf("wrote %s (%d metrics, %d series, %s..%s step %s%s)\n",
		path, len(names), seriesCount, start.Format(time.RFC3339), end.Format(time.RFC3339), step, runNote)
	return path, nil
}

// matchingNames resolves the metric names whose __name__ matches the regex.
func matchingNames(client *http.Client, base, match string) ([]string, error) {
	q := url.Values{}
	q.Set("match[]", fmt.Sprintf(`{__name__=~%q}`, match))
	u := base + "/api/v1/label/__name__/values?" + q.Encode()
	resp, err := client.Get(u)
	if err != nil {
		return nil, fmt.Errorf("label values: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("label values: status %d", resp.StatusCode)
	}
	var nr namesResponse
	if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if nr.Status != "success" {
		return nil, fmt.Errorf("label values: prometheus status %q", nr.Status)
	}
	logWarnings("label values", nr.Warnings)
	return nr.Data.Result, nil
}

// queryRange captures one metric family over the window.
func queryRange(client *http.Client, base, name string, start, end time.Time, step string) ([]series, error) {
	q := url.Values{}
	q.Set("query", fmt.Sprintf(`{__name__=%q}`, name))
	q.Set("start", start.Format(time.RFC3339))
	q.Set("end", end.Format(time.RFC3339))
	q.Set("step", step)
	u := base + "/api/v1/query_range?" + q.Encode()

	resp, err := client.Get(u)
	if err != nil {
		return nil, fmt.Errorf("query_range %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query_range %s: status %d", name, resp.StatusCode)
	}
	var rr rangeResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if rr.Status != "success" {
		return nil, fmt.Errorf("query_range %s: prometheus status %q", name, rr.Status)
	}
	logWarnings("query_range "+name, rr.Warnings)
	return rr.Data.Result, nil
}

// logWarnings surfaces Prometheus warnings, which used to be dropped: they
// are how Prometheus reports, e.g., skipped series or lookback gaps that
// would silently misdate a capture.
func logWarnings(source string, warnings []string) {
	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "promcapture: warning (%s): %s\n", source, w)
	}
}

func labelKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		if k != "__name__" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ",")
}
