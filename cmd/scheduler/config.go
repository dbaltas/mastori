package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/dbaltas/mastori/internal/registry"
)

type config struct {
	listenAddr         string
	redisAddr          string
	stream             string
	group              string
	consumerID         string
	workerStreamPrefix string
	registryPrefix     string
	policy             string
	jobTTL             time.Duration
	logLevel           string
}

func loadConfig() (*config, error) {
	cfg := &config{
		listenAddr:         getenvOr("LISTEN_ADDR", ":8081"),
		redisAddr:          getenvOr("REDIS_ADDR", "localhost:6379"),
		stream:             getenvOr("REDIS_STREAM", "jobs"),
		group:              getenvOr("SCHEDULER_CONSUMER_GROUP", "mastori-scheduler"),
		consumerID:         getenvOr("SCHEDULER_ID", "scheduler-"+randomID()),
		workerStreamPrefix: getenvOr("REDIS_WORKER_STREAM_PREFIX", registry.DefaultStreamPrefix),
		registryPrefix:     getenvOr("REDIS_WORKER_REGISTRY_PREFIX", registry.DefaultPrefix),
		policy:             getenvOr("SCHEDULER_POLICY", "round-robin"),
		jobTTL:             60 * time.Second,
		logLevel:           getenvOr("LOG_LEVEL", "info"),
	}

	// Jobs older than this are dropped instead of dispatched: by then the API
	// has already timed the request out, so running it only wastes capacity.
	if v := os.Getenv("SCHEDULER_JOB_TTL"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil || secs < 0 {
			return nil, fmt.Errorf("invalid SCHEDULER_JOB_TTL %q: must be a non-negative integer (seconds, 0 disables)", v)
		}
		cfg.jobTTL = time.Duration(secs) * time.Second
	}
	return cfg, nil
}

func getenvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func randomID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "0"
	}
	return hex.EncodeToString(b)
}
