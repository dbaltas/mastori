package traffic

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

func TestConstantPattern(t *testing.T) {
	p := NewConstantPattern(20) // 20 req/s -> 50ms interval
	want := 50 * time.Millisecond
	for i := 0; i < 10; i++ {
		if got := p.NextDelay(time.Duration(i) * want); got != want {
			t.Errorf("delay %d = %v, want %v", i, got, want)
		}
	}
}

func TestPoissonPattern(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	p := NewPoissonPattern(10, rng) // mean 100ms
	sum := time.Duration(0)
	n := 10000
	for i := 0; i < n; i++ {
		sum += p.NextDelay(0)
	}
	mean := float64(sum) / float64(n)
	want := float64(100 * time.Millisecond)
	if math.Abs(mean-want) > 0.05*want {
		t.Errorf("poisson mean = %v, want ~%v", time.Duration(mean), time.Duration(want))
	}
}

func TestRampPattern(t *testing.T) {
	p := RampPattern{StartRate: 10, EndRate: 110, Duration: time.Minute}
	// At t=0 rate is 10 -> 100ms interval.
	if got := p.NextDelay(0); got != 100*time.Millisecond {
		t.Errorf("start interval = %v, want 100ms", got)
	}
	// At t=60s rate is 110 -> ~9.09ms interval.
	endRate := 110.0
	endInterval := time.Duration(float64(time.Second) / endRate)
	if got := p.NextDelay(time.Minute); got != endInterval {
		t.Errorf("end interval = %v", got)
	}
	// Halfway rate is 60 -> ~16.67ms.
	midRate := 60.0
	mid := time.Duration(float64(time.Second) / midRate)
	if got := p.NextDelay(30 * time.Second); got != mid {
		t.Errorf("mid interval = %v, want %v", got, mid)
	}
	// Past the duration it clamps.
	if got := p.NextDelay(2 * time.Minute); got != endInterval {
		t.Errorf("past-end interval = %v", got)
	}
}

func TestStepPattern(t *testing.T) {
	p := StepPattern{Segments: []StepSeg{
		{Rate: 10, Duration: time.Minute},
		{Rate: 50, Duration: time.Minute},
	}}
	if got := p.NextDelay(0); got != 100*time.Millisecond {
		t.Errorf("first segment interval = %v", got)
	}
	if got := p.NextDelay(90 * time.Second); got != 20*time.Millisecond {
		t.Errorf("second segment interval = %v, want 20ms", got)
	}
	// Past the last segment it holds the final rate.
	if got := p.NextDelay(5 * time.Minute); got != 20*time.Millisecond {
		t.Errorf("tail interval = %v, want 20ms", got)
	}
}

func TestParseSteps(t *testing.T) {
	segs, err := ParseSteps("10:30s, 50:1m,2.5:15s")
	if err != nil {
		t.Fatalf("ParseSteps: %v", err)
	}
	if len(segs) != 3 {
		t.Fatalf("got %d segments, want 3", len(segs))
	}
	if segs[0].Rate != 10 || segs[0].Duration != 30*time.Second {
		t.Errorf("seg 0 wrong: %+v", segs[0])
	}
	if segs[1].Rate != 50 || segs[1].Duration != time.Minute {
		t.Errorf("seg 1 wrong: %+v", segs[1])
	}
	if segs[2].Rate != 2.5 || segs[2].Duration != 15*time.Second {
		t.Errorf("seg 2 wrong: %+v", segs[2])
	}
}

func TestParseStepsErrors(t *testing.T) {
	for _, spec := range []string{"", "10:30s,", "10", "abc:30s", "10:0s", "10:xyz"} {
		if _, err := ParseSteps(spec); err == nil {
			t.Errorf("spec %q should fail", spec)
		}
	}
}
