package fakegpu

import (
	"fmt"
	"strconv"
	"time"
)

type Config struct {
	NodeName string
	Count    int64
	Interval time.Duration
}

func ConfigFromEnv(getenv func(string) string) (Config, error) {
	cfg := Config{Count: 1, Interval: DefaultInterval}
	cfg.NodeName = getenv("NODE_NAME")
	if cfg.NodeName == "" {
		return cfg, fmt.Errorf("fakegpu: NODE_NAME is required")
	}
	if raw := getenv("FAKE_GPU_COUNT"); raw != "" {
		count, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || count < 1 {
			return cfg, fmt.Errorf("fakegpu: FAKE_GPU_COUNT %q must be a positive integer", raw)
		}
		cfg.Count = count
	}
	if raw := getenv("FAKE_GPU_INTERVAL"); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval <= 0 {
			return cfg, fmt.Errorf("fakegpu: FAKE_GPU_INTERVAL %q must be a positive duration", raw)
		}
		cfg.Interval = interval
	}
	return cfg, nil
}
