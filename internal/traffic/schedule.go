package traffic

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// Pattern produces the delay until the next request send, given the elapsed
// run time. All delays are relative to the run start so patterns stay
// deterministic for a seeded RNG.
type Pattern interface {
	NextDelay(elapsed time.Duration) time.Duration

	// Offered returns the expected number of scheduled arrivals over the
	// given window. This is the schedule-implied offered load: arrivals the
	// sender should attempt. Send-side backpressure (the in-flight
	// semaphore) can prevent some of them from ever being sent — silently
	// under the old recompute-from-now loop — so the summary reports
	// offered vs sent (skipped_offered) to make the gap observable.
	Offered(window time.Duration) float64
}

// ConstantPattern sends at a fixed rate.
type ConstantPattern struct {
	Interval time.Duration
}

func NewConstantPattern(ratePerSec float64) ConstantPattern {
	return ConstantPattern{Interval: time.Duration(float64(time.Second) / ratePerSec)}
}

func (p ConstantPattern) NextDelay(time.Duration) time.Duration { return p.Interval }

// Offered returns interval-multiples fitting in the window.
func (p ConstantPattern) Offered(window time.Duration) float64 {
	if p.Interval <= 0 {
		return 0
	}
	return window.Seconds() / p.Interval.Seconds()
}

// PoissonPattern draws exponential inter-arrival times (mean 1/rate).
type PoissonPattern struct {
	Mean time.Duration
	rng  *rand.Rand
}

func NewPoissonPattern(ratePerSec float64, rng *rand.Rand) PoissonPattern {
	return PoissonPattern{
		Mean: time.Duration(float64(time.Second) / ratePerSec),
		rng:  rng,
	}
}

func (p PoissonPattern) NextDelay(time.Duration) time.Duration {
	return time.Duration(p.rng.ExpFloat64() * float64(p.Mean))
}

// Offered returns the expected arrival count (rate x window); actual
// Poisson arrivals vary around it.
func (p PoissonPattern) Offered(window time.Duration) float64 {
	if p.Mean <= 0 {
		return 0
	}
	return window.Seconds() / p.Mean.Seconds()
}

// RampPattern linearly ramps the rate from Start to End over the run.
// The interval is recomputed per send from the instantaneous rate; this
// approximates a continuous ramp and keeps rate(t) exact in aggregate.
type RampPattern struct {
	StartRate float64
	EndRate   float64
	Duration  time.Duration
}

func (p RampPattern) rateAt(elapsed time.Duration) float64 {
	frac := 0.0
	if p.Duration > 0 {
		frac = float64(elapsed) / float64(p.Duration)
	}
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	return p.StartRate + (p.EndRate-p.StartRate)*frac
}

func (p RampPattern) NextDelay(elapsed time.Duration) time.Duration {
	r := p.rateAt(elapsed)
	if r <= 0 {
		return time.Hour
	}
	return time.Duration(float64(time.Second) / r)
}

// Offered integrates the instantaneous rate over the window. Past the
// pattern duration the rate holds at EndRate (matching NextDelay's clamp),
// so a window longer than the ramp keeps counting.
func (p RampPattern) Offered(window time.Duration) float64 {
	w := window
	if p.Duration > 0 && w > p.Duration {
		w = p.Duration
	}
	if w <= 0 {
		return 0
	}
	var n float64
	if w > 0 {
		n += (p.StartRate + p.rateAt(w)) / 2 * w.Seconds()
	}
	if w < window {
		n += p.rateAt(window) * (window - w).Seconds()
	}
	return n
}

// StepSeg is one phase of a step pattern: a fixed rate held for a duration.
type StepSeg struct {
	Rate     float64
	Duration time.Duration
}

// StepPattern holds a constant rate per segment, switching at segment
// boundaries. Past the last segment it holds the final segment's rate.
type StepPattern struct {
	Segments []StepSeg
}

func (p StepPattern) NextDelay(elapsed time.Duration) time.Duration {
	if len(p.Segments) == 0 {
		return time.Hour
	}
	rate := p.Segments[len(p.Segments)-1].Rate
	segElapsed := elapsed
	for _, seg := range p.Segments {
		if segElapsed < seg.Duration {
			rate = seg.Rate
			break
		}
		segElapsed -= seg.Duration
	}
	if rate <= 0 {
		return time.Hour
	}
	return time.Duration(float64(time.Second) / rate)
}

// Offered integrates each segment's rate over its span, truncated at the
// window. Past the last segment the pattern holds the final rate (matching
// NextDelay), so a window longer than the segment total keeps counting at
// that rate.
func (p StepPattern) Offered(window time.Duration) float64 {
	remaining := window
	if remaining <= 0 {
		return 0
	}
	var n float64
	for _, seg := range p.Segments {
		if remaining <= 0 {
			break
		}
		w := seg.Duration
		if w > remaining {
			w = remaining
		}
		n += seg.Rate * w.Seconds()
		remaining -= w
	}
	if remaining > 0 && len(p.Segments) > 0 {
		n += p.Segments[len(p.Segments)-1].Rate * remaining.Seconds()
	}
	return n
}

// ParseSteps parses a step spec like "10:30s,50:60s" into segments.
func ParseSteps(spec string) ([]StepSeg, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, fmt.Errorf("empty step spec")
	}
	var segs []StepSeg
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty segment in step spec %q", spec)
		}
		idx := strings.IndexByte(part, ':')
		if idx < 0 {
			return nil, fmt.Errorf("invalid step %q: expected rate:duration (e.g. 10:30s)", part)
		}
		var rate float64
		if _, err := fmt.Sscanf(part[:idx], "%f", &rate); err != nil {
			return nil, fmt.Errorf("invalid rate in step %q: %w", part, err)
		}
		if rate <= 0 {
			return nil, fmt.Errorf("invalid rate %g in step %q: rates must be positive (a zero rate would hang the schedule)", rate, part)
		}
		d, err := time.ParseDuration(strings.TrimSpace(part[idx+1:]))
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("invalid duration in step %q: %w", part, err)
		}
		segs = append(segs, StepSeg{Rate: rate, Duration: d})
	}
	if len(segs) == 0 {
		return nil, fmt.Errorf("no segments in step spec")
	}
	return segs, nil
}
