package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

// trafficOpts holds the parsed traffic flags.
type trafficOpts struct {
	url            *string
	model          *string
	workload       *string
	shortPct       *int
	shortDataset   *string
	longDataset    *string
	cacheDir       *string
	rate           *float64
	arrival        *string
	rampEnd        *float64
	stepsSpec      *string
	concurrency    *int
	maxInflight    *int
	duration       *time.Duration
	warmup         *time.Duration
	drainGrace     *time.Duration
	timeout        *time.Duration
	seed           *int64
	stream         *bool
	maxTokens      *int64
	ignoreEOS      *bool
	temperature    *float64
	schedulerURL   *string
	expectedWorker *int
	smoke          *bool
	output         *string
	recordsStdout  *bool
	manifest       *string
}

// registerTrafficFlags defines every traffic flag with its default value.
// Defaults are hardcoded; applyEnv overrides them from TRAFFIC_* variables
// and command-line args win over both.
func registerTrafficFlags(fs *flag.FlagSet) trafficOpts {
	return trafficOpts{
		url:            fs.String("url", "http://localhost:8080", "API base URL"),
		model:          fs.String("model", "", "model name passed to the API (empty = API default)"),
		workload:       fs.String("workload", "mixed", "workload profile: short, long, or mixed"),
		shortPct:       fs.Int("short-percentage", 80, "percentage of short prompts in the mixed workload"),
		shortDataset:   fs.String("short-dataset", "mmlu-short", "dataset used for short prompts"),
		longDataset:    fs.String("long-dataset", "sharegpt-long", "dataset used for long prompts"),
		cacheDir:       fs.String("cache", "benchmark/workloads/cache", "dataset cache directory"),
		rate:           fs.Float64("rate", 10, "target request rate per second (open loop)"),
		arrival:        fs.String("arrival", "constant", "arrival process: constant or poisson"),
		rampEnd:        fs.Float64("ramp-end-rate", 0, "if > 0, ramp the rate from --rate to this value over the run"),
		stepsSpec:      fs.String("steps", "", "step pattern as rate:duration pairs, e.g. \"10:60s,50:60s\" (overrides rate/arrival/ramp)"),
		concurrency:    fs.Int("concurrency", 0, "closed-loop worker count (0 = open loop)"),
		maxInflight:    fs.Int("max-inflight", 0, "cap on in-flight requests (open loop, default 4x rate)"),
		duration:       fs.Duration("duration", 0, "measured run duration (default: sum of --steps durations, else 5m)"),
		warmup:         fs.Duration("warmup", 0, "warmup duration, excluded from the summary"),
		drainGrace:     fs.Duration("warmup-drain-grace", 0, "max wait for in-flight warmup requests to finish before measured traffic starts (default timeout+10s)"),
		timeout:        fs.Duration("timeout", 65*time.Second, "per-request timeout"),
		seed:           fs.Int64("seed", 42, "seed for dataset sampling and arrival jitter"),
		stream:         fs.Bool("stream", true, "use the SSE streaming endpoint /generate/stream (false = blocking /generate)"),
		maxTokens:      fs.Int64("max-tokens", 0, "output-token cap sent with every request (0 = omit, backend default applies; set it so runs measure a known output length)"),
		ignoreEOS:      fs.Bool("ignore-eos", false, "send ignore_eos so the backend generates until --max-tokens (needs --max-tokens)"),
		temperature:    fs.Float64("temperature", 0, "sampling temperature sent with every request (0 = deterministic; negative omits the parameter)"),
		schedulerURL:   fs.String("scheduler-url", "", "scheduler /metrics base URL for the run gate (e.g. http://scheduler.mastori.svc.cluster.local:8081); empty disables the gate"),
		expectedWorker: fs.Int("expected-workers", 0, "required mastori_scheduler_workers_healthy count before the run starts (needs --scheduler-url)"),
		smoke:          fs.Bool("smoke", false, "send one preflight streaming request and assert chunks > 1, TTFT < e2e, and tokens_out == max_tokens (abort the run on failure)"),
		output:         fs.String("output", "", "results JSONL path (default benchmark/results/<run-id>.jsonl)"),
		recordsStdout:  fs.Bool("records-stdout", false, "also stream result records as JSON lines to stdout (for in-cluster runs)"),
		manifest:       fs.String("manifest", "", "dataset manifest path; its SHA-256 is recorded in the summary as seed-data provenance (empty = skip)"),
	}
}

// manifestHash returns the hex-encoded SHA-256 of the dataset manifest file,
// recorded in the run summary so results stay traceable to their seed data.
func manifestHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open manifest: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash manifest: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// envFlag maps a TRAFFIC_* environment variable to a flag name.
type envFlag struct {
	env  string
	flag string
}

// envFlags covers every traffic flag so a Kubernetes Job can configure the
// whole run through the traffic-config ConfigMap. Command-line flags still
// win: values are applied before Parse, which overrides them whenever the
// flag appears in args.
var envFlags = []envFlag{
	{"TRAFFIC_URL", "url"},
	{"TRAFFIC_MODEL", "model"},
	{"TRAFFIC_WORKLOAD", "workload"},
	{"TRAFFIC_SHORT_PERCENTAGE", "short-percentage"},
	{"TRAFFIC_SHORT_DATASET", "short-dataset"},
	{"TRAFFIC_LONG_DATASET", "long-dataset"},
	{"TRAFFIC_CACHE", "cache"},
	{"TRAFFIC_RATE", "rate"},
	{"TRAFFIC_ARRIVAL", "arrival"},
	{"TRAFFIC_RAMP_END_RATE", "ramp-end-rate"},
	{"TRAFFIC_STEPS", "steps"},
	{"TRAFFIC_CONCURRENCY", "concurrency"},
	{"TRAFFIC_MAX_INFLIGHT", "max-inflight"},
	{"TRAFFIC_DURATION", "duration"},
	{"TRAFFIC_WARMUP", "warmup"},
	{"TRAFFIC_WARMUP_DRAIN_GRACE", "warmup-drain-grace"},
	{"TRAFFIC_TIMEOUT", "timeout"},
	{"TRAFFIC_SEED", "seed"},
	{"TRAFFIC_STREAM", "stream"},
	{"TRAFFIC_MAX_TOKENS", "max-tokens"},
	{"TRAFFIC_IGNORE_EOS", "ignore-eos"},
	{"TRAFFIC_TEMPERATURE", "temperature"},
	{"TRAFFIC_SCHEDULER_URL", "scheduler-url"},
	{"TRAFFIC_EXPECTED_WORKERS", "expected-workers"},
	{"TRAFFIC_SMOKE", "smoke"},
	{"TRAFFIC_RECORDS_STDOUT", "records-stdout"},
	{"TRAFFIC_MANIFEST", "manifest"},
	{"TRAFFIC_OUTPUT", "output"},
}

// applyEnv applies TRAFFIC_* environment variables to the flag set. Empty or
// unset variables are skipped, so a ConfigMap can carry placeholder entries
// without overriding flag defaults.
func applyEnv(fs *flag.FlagSet, getenv func(string) string) error {
	for _, ef := range envFlags {
		v := getenv(ef.env)
		if v == "" {
			continue
		}
		if err := fs.Set(ef.flag, v); err != nil {
			return fmt.Errorf("%s: %w", ef.env, err)
		}
	}
	return nil
}
