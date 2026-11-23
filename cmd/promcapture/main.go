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
}

// defaultMatch selects the series captured per run: vllm:* engine counters,
// DCGM_* GPU telemetry, and mastori_* platform gauges. Prometheus __name__
// regexes are fully anchored, so each alternative must end in .* — the
// pre-fix `vllm:|DCGM_|mastori_` matched zero series and captured nothing.
const defaultMatch = `vllm:.*|DCGM_.*|mastori_.*`

func main() {
	urlFlag := flag.String("url", "http://localhost:9091", "Prometheus base URL")
	outFlag := flag.String("out", "", "output directory (default: benchmark/results/<EXPERIMENT>/metrics via EXPERIMENT env)")
	startFlag := flag.String("start", "", "window start, RFC3339 UTC (default: end-1h)")
	endFlag := flag.String("end", "", "window end, RFC3339 UTC (default: now)")
	stepFlag := flag.String("step", "15s", "query_range step")
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

	end := time.Now().UTC()
	if *endFlag != "" {
		t, err := time.Parse(time.RFC3339, *endFlag)
		if err != nil {
			log.Fatalf("promcapture: -end: %v", err)
		}
		end = t.UTC()
	}
	start := end.Add(-time.Hour)
	if *startFlag != "" {
		t, err := time.Parse(time.RFC3339, *startFlag)
		if err != nil {
			log.Fatalf("promcapture: -start: %v", err)
		}
		start = t.UTC()
	}
	if start.After(end) {
		log.Fatal("promcapture: -start is after -end")
	}

	if _, err := run(*urlFlag, out, start, end, *stepFlag, *matchFlag); err != nil {
		log.Fatalf("promcapture: %v", err)
	}
}

// run queries Prometheus once for every series whose __name__ matches, groups
// the result by metric name, and writes prometheus-<date>-<start>-<end>.json
// into out. It returns the written file path.
func run(baseURL, out string, start, end time.Time, step, match string) (string, error) {
	if _, err := regexp.Compile(match); err != nil {
		return "", fmt.Errorf("-match: %w", err)
	}
	q := url.Values{}
	q.Set("query", fmt.Sprintf(`{__name__=~%q}`, match))
	q.Set("start", start.Format(time.RFC3339))
	q.Set("end", end.Format(time.RFC3339))
	q.Set("step", step)

	u := strings.TrimSuffix(baseURL, "/") + "/api/v1/query_range?" + q.Encode()
	client := &http.Client{Timeout: 2 * time.Minute}
	resp, err := client.Get(u)
	if err != nil {
		return "", fmt.Errorf("query_range: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("query_range: status %d", resp.StatusCode)
	}

	var rr rangeResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	if rr.Status != "success" {
		return "", fmt.Errorf("query_range: prometheus status %q", rr.Status)
	}

	grouped := map[string][]series{}
	for _, s := range rr.Data.Result {
		name := s.Metric["__name__"]
		grouped[name] = append(grouped[name], series{Metric: s.Metric, Values: s.Values})
	}
	names := make([]string, 0, len(grouped))
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

	day := start.Format("20060102")
	file := fmt.Sprintf("prometheus-%s-%s-%s.json", day, start.Format("1504"), end.Format("1504"))
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
	fmt.Printf("wrote %s (%d metrics, %d series, %s..%s step %s)\n",
		path, len(names), seriesCount, start.Format(time.RFC3339), end.Format(time.RFC3339), step)
	return path, nil
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
