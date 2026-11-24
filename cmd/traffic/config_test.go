package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestApplyEnvSetsFlags(t *testing.T) {
	fs := flag.NewFlagSet("traffic", flag.ContinueOnError)
	url := fs.String("url", "http://default", "")
	rate := fs.Float64("rate", 10, "")
	duration := fs.Duration("duration", 5*time.Minute, "")
	stream := fs.Bool("stream", true, "")

	env := map[string]string{
		"TRAFFIC_URL":      "http://api.mastori.svc.cluster.local:8080",
		"TRAFFIC_RATE":     "50",
		"TRAFFIC_DURATION": "2m",
		"TRAFFIC_STREAM":   "false",
	}
	if err := applyEnv(fs, func(key string) string { return env[key] }); err != nil {
		t.Fatalf("applyEnv: %v", err)
	}
	if *url != "http://api.mastori.svc.cluster.local:8080" {
		t.Errorf("url = %q, want in-cluster API URL", *url)
	}
	if *rate != 50 {
		t.Errorf("rate = %v, want 50", *rate)
	}
	if *duration != 2*time.Minute {
		t.Errorf("duration = %v, want 2m", *duration)
	}
	if *stream {
		t.Error("stream = true, want false")
	}
}

// Empty env values must be skipped so a ConfigMap can carry placeholder
// entries without overriding flag defaults.
func TestApplyEnvSkipsEmpty(t *testing.T) {
	fs := flag.NewFlagSet("traffic", flag.ContinueOnError)
	url := fs.String("url", "http://default", "")
	rate := fs.Float64("rate", 10, "")

	env := map[string]string{"TRAFFIC_URL": "", "TRAFFIC_RATE": ""}
	if err := applyEnv(fs, func(key string) string { return env[key] }); err != nil {
		t.Fatalf("applyEnv: %v", err)
	}
	if *url != "http://default" {
		t.Errorf("url = %q, want default unchanged", *url)
	}
	if *rate != 10 {
		t.Errorf("rate = %v, want default 10", *rate)
	}
}

// Command-line flags win over the environment: applyEnv runs before Parse.
func TestFlagsOverrideEnv(t *testing.T) {
	fs := flag.NewFlagSet("traffic", flag.ContinueOnError)
	rate := fs.Float64("rate", 10, "")

	env := map[string]string{"TRAFFIC_RATE": "50"}
	if err := applyEnv(fs, func(key string) string { return env[key] }); err != nil {
		t.Fatalf("applyEnv: %v", err)
	}
	if err := fs.Parse([]string{"-rate", "20"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if *rate != 20 {
		t.Errorf("rate = %v, want 20 from args", *rate)
	}
}

func TestApplyEnvInvalidValue(t *testing.T) {
	fs := flag.NewFlagSet("traffic", flag.ContinueOnError)
	fs.Float64("rate", 10, "")

	env := map[string]string{"TRAFFIC_RATE": "not-a-number"}
	if err := applyEnv(fs, func(key string) string { return env[key] }); err == nil {
		t.Error("applyEnv should fail on an unparsable value")
	}
}

// Every env var in the table must match an existing flag name.
func TestEnvFlagsReferenceRealFlags(t *testing.T) {
	fs := flag.NewFlagSet("traffic", flag.ContinueOnError)
	registerTrafficFlags(fs)

	known := map[string]bool{}
	fs.VisitAll(func(f *flag.Flag) { known[f.Name] = true })
	for _, ef := range envFlags {
		if !known[ef.flag] {
			t.Errorf("%s maps to unknown flag %q", ef.env, ef.flag)
		}
	}
}

// The review-fix env vars (output control and run gates) must reach their
// flags through the ConfigMap path.
func TestApplyEnvSetsGenerationAndGateFlags(t *testing.T) {
	fs := flag.NewFlagSet("traffic", flag.ContinueOnError)
	opts := registerTrafficFlags(fs)

	env := map[string]string{
		"TRAFFIC_MAX_TOKENS":       "256",
		"TRAFFIC_IGNORE_EOS":       "true",
		"TRAFFIC_TEMPERATURE":      "0",
		"TRAFFIC_SCHEDULER_URL":    "http://scheduler.mastori.svc.cluster.local:8081",
		"TRAFFIC_EXPECTED_WORKERS": "1",
		"TRAFFIC_SMOKE":            "true",
	}
	if err := applyEnv(fs, func(key string) string { return env[key] }); err != nil {
		t.Fatalf("applyEnv: %v", err)
	}
	if *opts.maxTokens != 256 {
		t.Errorf("max-tokens = %d, want 256", *opts.maxTokens)
	}
	if !*opts.ignoreEOS {
		t.Error("ignore-eos = false, want true")
	}
	if *opts.temperature != 0 {
		t.Errorf("temperature = %v, want 0", *opts.temperature)
	}
	if *opts.schedulerURL != "http://scheduler.mastori.svc.cluster.local:8081" {
		t.Errorf("scheduler-url = %q", *opts.schedulerURL)
	}
	if *opts.expectedWorker != 1 {
		t.Errorf("expected-workers = %d, want 1", *opts.expectedWorker)
	}
	if !*opts.smoke {
		t.Error("smoke = false, want true")
	}
}

func TestManifestHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "datasets.yaml")
	if err := os.WriteFile(path, []byte("datasets:\n  mmlu-short:\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := manifestHash(path)
	if err != nil {
		t.Fatalf("manifestHash: %v", err)
	}
	if len(got) != 64 {
		t.Errorf("hash = %q, want 64 hex chars", got)
	}
	again, err := manifestHash(path)
	if err != nil || again != got {
		t.Errorf("hash not stable: %q vs %q (err %v)", got, again, err)
	}

	if _, err := manifestHash(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("manifestHash should fail on a missing file")
	}
}
