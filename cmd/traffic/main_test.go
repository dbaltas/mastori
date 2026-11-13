package main

import (
	"testing"
	"time"

	"github.com/dbaltas/mastori/internal/traffic"
)

func TestResolveDuration(t *testing.T) {
	steps := []traffic.StepSeg{{Duration: 10 * time.Second}, {Duration: 20 * time.Second}}

	if got, err := resolveDuration(steps, 30*time.Second); err != nil || got != 30*time.Second {
		t.Errorf("explicit duration = %v err = %v, want 30s", got, err)
	}
	if got, err := resolveDuration(steps, 0); err != nil || got != 30*time.Second {
		t.Errorf("steps sum = %v err = %v, want 30s", got, err)
	}
	if got, err := resolveDuration(nil, 0); err != nil || got != 5*time.Minute {
		t.Errorf("default = %v err = %v, want 5m", got, err)
	}
	if _, err := resolveDuration([]traffic.StepSeg{{Duration: -1 * time.Second}}, 0); err == nil {
		t.Error("negative step duration should error")
	}
}
