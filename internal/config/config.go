package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ListenAddr     string
	RedisAddr      string
	RedisStream    string
	ResultPrefix   string
	RequestTimeout time.Duration
	DefaultModel   string
	OTLPEndpoint   string
	SamplerRatio   float64
	LogLevel       string
}

func Load() (*Config, error) {
	cfg := &Config{
		ListenAddr:     getenv("LISTEN_ADDR", ":8080"),
		RedisAddr:      getenv("REDIS_ADDR", "localhost:6379"),
		RedisStream:    getenv("REDIS_STREAM", "jobs"),
		ResultPrefix:   getenv("REDIS_RESULT_PREFIX", "jobs:result:"),
		RequestTimeout: 60 * time.Second,
		DefaultModel:   getenv("DEFAULT_MODEL", "default"),
		OTLPEndpoint:   os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		SamplerRatio:   1.0,
		LogLevel:       getenv("LOG_LEVEL", "info"),
	}

	if v := os.Getenv("REQUEST_TIMEOUT"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil || secs <= 0 {
			return nil, fmt.Errorf("invalid REQUEST_TIMEOUT %q: must be a positive integer (seconds)", v)
		}
		cfg.RequestTimeout = time.Duration(secs) * time.Second
	}

	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		ratio, err := strconv.ParseFloat(v, 64)
		if err != nil || ratio < 0 || ratio > 1 {
			return nil, fmt.Errorf("invalid OTEL_TRACES_SAMPLER_ARG %q: must be in [0, 1]", v)
		}
		cfg.SamplerRatio = ratio
	}

	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
