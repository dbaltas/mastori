package traffic

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// This file gates benchmark runs against the scheduler's Prometheus
// /metrics endpoint. The 2026-11-24 benchmark review (H1) found runs whose
// traffic was silently split between the real worker and two mock workers:
// the mock workers' empty model registry matched every model, and nothing in
// the run path noticed. The preflight check makes a half-registered fleet
// abort the run before it starts; the post-run capture records which workers
// actually received dispatches.

const (
	// workersHealthyMetric is the scheduler gauge for workers with a live
	// heartbeat.
	workersHealthyMetric = "mastori_scheduler_workers_healthy"
	// dispatchedTotalMetric is the scheduler per-worker dispatch counter.
	dispatchedTotalMetric = "mastori_scheduler_dispatched_total"
)

// CheckWorkersHealthy fetches the scheduler's /metrics endpoint and verifies
// that the workers_healthy gauge equals want. It returns the observed value;
// any mismatch or scrape failure is an error, so a run gated on the scheduler
// fails closed.
func CheckWorkersHealthy(ctx context.Context, schedulerURL string, want int) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metrics, err := scrapeMetrics(ctx, schedulerURL)
	if err != nil {
		return 0, fmt.Errorf("scheduler gate: %w", err)
	}
	healthy, ok := metrics.gauge(workersHealthyMetric)
	if !ok {
		return 0, fmt.Errorf("scheduler gate: %s not found in %s/metrics", workersHealthyMetric, strings.TrimSuffix(schedulerURL, "/"))
	}
	if int(healthy) != want {
		return int(healthy), fmt.Errorf("scheduler gate: %s = %d, want %d; run aborted: worker fleet does not match the expected topology",
			workersHealthyMetric, int(healthy), want)
	}
	return int(healthy), nil
}

// FetchDispatchedByWorker returns the scheduler's per-worker
// mastori_scheduler_dispatched_total counters, keyed by worker id.
func FetchDispatchedByWorker(ctx context.Context, schedulerURL string) (map[string]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metrics, err := scrapeMetrics(ctx, schedulerURL)
	if err != nil {
		return nil, fmt.Errorf("scheduler dispatch capture: %w", err)
	}
	return metrics.countersByLabel(dispatchedTotalMetric, "worker"), nil
}

// metricsDoc is a minimal parse of the Prometheus text exposition format:
// just enough to read gauges and labeled counters from the scheduler's
// /metrics endpoint without a full exposition-library dependency.
type metricsDoc struct {
	lines []string
}

// scrapeMetrics GETs <base>/metrics and returns its text lines, skipping
// HELP and TYPE comments.
func scrapeMetrics(ctx context.Context, base string) (*metricsDoc, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(base, "/")+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("metrics endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var doc metricsDoc
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		doc.lines = append(doc.lines, line)
	}
	return &doc, nil
}

// gauge returns the value of an unlabeled gauge (first sample wins).
func (d *metricsDoc) gauge(name string) (float64, bool) {
	prefix := name + " "
	for _, line := range d.lines {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, prefix))
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		return v, true
	}
	return 0, false
}

// countersByLabel returns the latest sample of every labeled counter family,
// keyed by the given label's value.
func (d *metricsDoc) countersByLabel(name, label string) map[string]int64 {
	out := map[string]int64{}
	prefix := name + "{"
	for _, line := range d.lines {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		rest := strings.TrimPrefix(line, prefix)
		labelValue, value, ok := splitSampleLine(rest, label)
		if !ok {
			continue
		}
		out[labelValue] = value
	}
	return out
}

// splitSampleLine parses `worker="w1"} 17` (a labeled metric tail) into the
// label value and the sample value.
func splitSampleLine(rest, label string) (string, int64, bool) {
	brace := strings.Index(rest, "}")
	if brace < 0 {
		return "", 0, false
	}
	labels := rest[:brace]
	valuePart := strings.TrimSpace(rest[brace+1:])
	want := label + `="`
	if !strings.HasPrefix(labels, want) {
		return "", 0, false
	}
	labelValue := strings.TrimPrefix(labels, want)
	labelValue = strings.TrimSuffix(labelValue, `"`)
	fields := strings.Fields(valuePart)
	if len(fields) == 0 {
		return "", 0, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "", 0, false
	}
	return labelValue, int64(v), true
}
