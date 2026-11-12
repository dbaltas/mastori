package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/dbaltas/mastori/internal/traffic"
)

const usage = `mastori-traffic — benchmark traffic generator

Replays dataset prompts against the API at a controlled rate and writes
per-request results as JSONL plus a run summary.

Usage:
  mastori-traffic [flags]

Load model:
  open loop (default):   --rate 20 sends 20 req/s regardless of completion
  closed loop:           --concurrency 50 runs 50 in-flight workers

Workload:
  --workload short|long|mixed picks datasets from the local cache
  (--short-dataset, --long-dataset override the names)

Examples:
  mastori-traffic --workload short --rate 10 --duration 5m
  mastori-traffic --workload mixed --short-percentage 80 --rate 50 --duration 10m
  mastori-traffic --workload long --rate 10 --ramp-end-rate 40 --duration 5m
  mastori-traffic --workload mixed --steps "10:60s,50:60s,10:60s"
`

func main() {
	fs := flag.NewFlagSet("mastori-traffic", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }

	url := fs.String("url", "http://localhost:8080", "API base URL")
	model := fs.String("model", "", "model name passed to the API (empty = API default)")
	workload := fs.String("workload", "mixed", "workload profile: short, long, or mixed")
	shortPct := fs.Int("short-percentage", 80, "percentage of short prompts in the mixed workload")
	shortDataset := fs.String("short-dataset", "mmlu-short", "dataset used for short prompts")
	longDataset := fs.String("long-dataset", "sharegpt-long", "dataset used for long prompts")
	cacheDir := fs.String("cache", "benchmark/workloads/cache", "dataset cache directory")

	rate := fs.Float64("rate", 10, "target request rate per second (open loop)")
	arrival := fs.String("arrival", "constant", "arrival process: constant or poisson")
	rampEnd := fs.Float64("ramp-end-rate", 0, "if > 0, ramp the rate from --rate to this value over the run")
	stepsSpec := fs.String("steps", "", "step pattern as rate:duration pairs, e.g. \"10:60s,50:60s\" (overrides rate/arrival/ramp)")
	concurrency := fs.Int("concurrency", 0, "closed-loop worker count (0 = open loop)")
	maxInflight := fs.Int("max-inflight", 0, "cap on in-flight requests (open loop, default 4x rate)")
	duration := fs.Duration("duration", 5*time.Minute, "measured run duration")
	warmup := fs.Duration("warmup", 0, "warmup duration, excluded from the summary")
	timeout := fs.Duration("timeout", 65*time.Second, "per-request timeout")

	seed := fs.Int64("seed", 42, "seed for dataset sampling and arrival jitter")
	stream := fs.Bool("stream", true, "use the SSE streaming endpoint /generate/stream (false = blocking /generate)")
	output := fs.String("output", "", "results JSONL path (default benchmark/results/<run-id>.jsonl)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	rows, weights, err := resolveWorkload(*workload, *shortDataset, *longDataset, *shortPct, *cacheDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	var steps []traffic.StepSeg
	if *stepsSpec != "" {
		steps, err = traffic.ParseSteps(*stepsSpec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	}

	runID := time.Now().UTC().Format("20060102T150405Z") + fmt.Sprintf("-%d", time.Now().UnixNano()%1000)
	outPath := *output
	if outPath == "" {
		outPath = "benchmark/results/" + runID + ".jsonl"
	}

	cfg := &traffic.Config{
		RunID:       runID,
		URL:         *url,
		Model:       *model,
		Rows:        rows,
		Weights:     weights,
		Seed:        *seed,
		Warmup:      *warmup,
		Duration:    *duration,
		Concurrency: *concurrency,
		Rate:        *rate,
		Arrival:     *arrival,
		RampEndRate: *rampEnd,
		Steps:       steps,
		MaxInflight: *maxInflight,
		Stream:      *stream,
		Timeout:     *timeout,
		OutputPath:  outPath,
	}

	ctx, stop := traffic.RunSignalContext(context.Background())
	defer stop()

	fmt.Printf("run %s: workload=%s rate=%.1f duration=%s warmup=%s seed=%d -> %s\n",
		runID, *workload, *rate, *duration, *warmup, *seed, outPath)
	summary, err := traffic.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		fmt.Fprintf(os.Stderr, "error: encode summary: %v\n", err)
		os.Exit(1)
	}
}

// resolveWorkload maps the workload profile to dataset names and weights.
func resolveWorkload(workload, shortName, longName string, shortPct int, cacheDir string) ([]traffic.Row, map[string]float64, error) {
	switch workload {
	case "short":
		rows, err := traffic.LoadRows(cacheDir, []string{shortName})
		if err != nil {
			return nil, nil, err
		}
		return rows, map[string]float64{shortName: 1}, nil
	case "long":
		rows, err := traffic.LoadRows(cacheDir, []string{longName})
		if err != nil {
			return nil, nil, err
		}
		return rows, map[string]float64{longName: 1}, nil
	case "mixed":
		if shortPct < 0 || shortPct > 100 {
			return nil, nil, fmt.Errorf("short-percentage must be in [0, 100]")
		}
		rows, err := traffic.LoadRows(cacheDir, []string{shortName, longName})
		if err != nil {
			return nil, nil, err
		}
		w := map[string]float64{
			shortName: float64(shortPct),
			longName:  float64(100 - shortPct),
		}
		return rows, w, nil
	default:
		return nil, nil, fmt.Errorf("unknown workload %q: expected short, long, or mixed", workload)
	}
}
