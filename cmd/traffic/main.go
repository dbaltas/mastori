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

	opts := registerTrafficFlags(fs)

	if err := applyEnv(fs, os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	rows, weights, err := resolveWorkload(*opts.workload, *opts.shortDataset, *opts.longDataset, *opts.shortPct, *opts.cacheDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	var steps []traffic.StepSeg
	if *opts.stepsSpec != "" {
		steps, err = traffic.ParseSteps(*opts.stepsSpec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	}

	runDuration, err := resolveDuration(steps, *opts.duration)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	runID := time.Now().UTC().Format("20060102T150405Z") + fmt.Sprintf("-%d", time.Now().UnixNano()%1000)
	outPath := *opts.output
	if outPath == "" {
		outPath = "benchmark/results/" + runID + ".jsonl"
	}

	var manifestSHA string
	if *opts.manifest != "" {
		manifestSHA, err = manifestHash(*opts.manifest)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	}

	cfg := &traffic.Config{
		RunID:           runID,
		URL:             *opts.url,
		Model:           *opts.model,
		Rows:            rows,
		Weights:         weights,
		Seed:            *opts.seed,
		Warmup:          *opts.warmup,
		DrainGrace:      *opts.drainGrace,
		Duration:        runDuration,
		Concurrency:     *opts.concurrency,
		Rate:            *opts.rate,
		Arrival:         *opts.arrival,
		RampEndRate:     *opts.rampEnd,
		Steps:           steps,
		MaxInflight:     *opts.maxInflight,
		Stream:          *opts.stream,
		Timeout:         *opts.timeout,
		OutputPath:      outPath,
		RecordsStdout:   *opts.recordsStdout,
		ManifestSHA256:  manifestSHA,
		RedisAddr:       os.Getenv("REDIS_ADDR"),
		SchedulerURL:    *opts.schedulerURL,
		ExpectedWorkers: *opts.expectedWorker,
		Smoke:           *opts.smoke,
	}
	// Generation params: temperature is sent unless the user opted out with
	// a negative value; max_tokens/ignore_eos follow their flags.
	cfg.MaxTokens = *opts.maxTokens
	cfg.IgnoreEOS = *opts.ignoreEOS && *opts.maxTokens > 0
	if *opts.temperature >= 0 {
		cfg.Temperature = opts.temperature
	}
	if cfg.IgnoreEOS && cfg.MaxTokens == 0 {
		fmt.Fprintln(os.Stderr, "error: --ignore-eos needs --max-tokens")
		os.Exit(2)
	}

	ctx, stop := traffic.RunSignalContext(context.Background())
	defer stop()

	fmt.Printf("run %s: workload=%s rate=%.1f duration=%s warmup=%s seed=%d -> %s\n",
		runID, *opts.workload, *opts.rate, runDuration, *opts.warmup, *opts.seed, outPath)
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

// resolveDuration picks the measured run duration: an explicit --duration
// wins, then the sum of step durations when a step pattern is used, then the
// 5m default. Without this, a short step pattern would run for the full
// default duration at the final segment's rate.
func resolveDuration(steps []traffic.StepSeg, explicit time.Duration) (time.Duration, error) {
	if explicit > 0 {
		return explicit, nil
	}
	if len(steps) > 0 {
		var total time.Duration
		for _, s := range steps {
			if s.Duration <= 0 {
				return 0, fmt.Errorf("invalid step duration %q", s.Duration)
			}
			total += s.Duration
		}
		return total, nil
	}
	return 5 * time.Minute, nil
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
