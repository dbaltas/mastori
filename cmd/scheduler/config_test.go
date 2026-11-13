package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	for _, k := range []string{"LISTEN_ADDR", "REDIS_ADDR", "SCHEDULER_POLICY", "SCHEDULER_JOB_TTL", "SCHEDULER_ID", "LOG_LEVEL"} {
		t.Setenv(k, "")
	}
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listenAddr != ":8081" || cfg.redisAddr != "localhost:6379" || cfg.policy != "round-robin" ||
		cfg.stream != "jobs" || cfg.group != "mastori-scheduler" || cfg.jobTTL != 60*time.Second ||
		cfg.workerStreamPrefix != "jobs:worker:" || cfg.registryPrefix != "workers:" {
		t.Errorf("defaults = %+v", cfg)
	}
	if !strings.HasPrefix(cfg.consumerID, "scheduler-") {
		t.Errorf("consumerID = %q, want generated scheduler-* id", cfg.consumerID)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	t.Setenv("SCHEDULER_POLICY", "least-loaded")
	t.Setenv("SCHEDULER_JOB_TTL", "5")
	t.Setenv("SCHEDULER_ID", "s-1")
	t.Setenv("REDIS_ADDR", "redis:6379")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.policy != "least-loaded" || cfg.jobTTL != 5*time.Second || cfg.consumerID != "s-1" || cfg.redisAddr != "redis:6379" {
		t.Errorf("overrides = %+v", cfg)
	}
}

func TestLoadConfigJobTTLZeroDisables(t *testing.T) {
	t.Setenv("SCHEDULER_JOB_TTL", "0")
	cfg, err := loadConfig()
	if err != nil || cfg.jobTTL != 0 {
		t.Errorf("cfg = %+v, err = %v; want ttl 0", cfg, err)
	}
}

func TestLoadConfigInvalidJobTTL(t *testing.T) {
	for _, v := range []string{"abc", "-1", "1.5"} {
		t.Setenv("SCHEDULER_JOB_TTL", v)
		if _, err := loadConfig(); err == nil {
			t.Errorf("SCHEDULER_JOB_TTL=%q: expected error", v)
		}
	}
}
