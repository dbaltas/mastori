package config

import (
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q, want :8080", cfg.ListenAddr)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q", cfg.RedisAddr)
	}
	if cfg.RequestTimeout != 60*time.Second {
		t.Errorf("RequestTimeout = %v", cfg.RequestTimeout)
	}
	if cfg.SamplerRatio != 1.0 {
		t.Errorf("SamplerRatio = %v", cfg.SamplerRatio)
	}
}

func TestOverrides(t *testing.T) {
	t.Setenv("LISTEN_ADDR", ":9090")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("REQUEST_TIMEOUT", "5")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.1")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ListenAddr != ":9090" || cfg.RedisAddr != "redis:6379" {
		t.Errorf("cfg = %+v", cfg)
	}
	if cfg.RequestTimeout != 5*time.Second {
		t.Errorf("RequestTimeout = %v", cfg.RequestTimeout)
	}
	if cfg.SamplerRatio != 0.1 {
		t.Errorf("SamplerRatio = %v", cfg.SamplerRatio)
	}
	if cfg.OTLPEndpoint != "http://collector:4318" {
		t.Errorf("OTLPEndpoint = %q", cfg.OTLPEndpoint)
	}
}

func TestInvalidValues(t *testing.T) {
	t.Run("bad request timeout", func(t *testing.T) {
		t.Setenv("REQUEST_TIMEOUT", "nope")
		if _, err := Load(); err == nil {
			t.Fatal("expected error for invalid REQUEST_TIMEOUT")
		}
	})
	t.Run("bad sampler arg", func(t *testing.T) {
		t.Setenv("OTEL_TRACES_SAMPLER_ARG", "2.0")
		if _, err := Load(); err == nil {
			t.Fatal("expected error for out-of-range sampler arg")
		}
	})
}
