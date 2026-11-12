package traffic

import "testing"

const (
	tolerancePct = histRelErrorPct
)

func withinTolerance(got, want float64) bool {
	if want == 0 {
		return got == 0
	}
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	return diff <= want*tolerancePct/100
}

func TestLatencyStats(t *testing.T) {
	var s LatencyStats
	if s.Percentile(95) != 0 || s.Mean() != 0 || s.Min() != 0 || s.Max() != 0 {
		t.Error("empty stats should be zero")
	}

	// 100 values 1..100 ms.
	for i := 1; i <= 100; i++ {
		s.Add(float64(i))
	}
	for _, tc := range []struct {
		p, want float64
	}{
		{0, 1},
		{50, 50},
		{95, 95},
		{99, 99},
		{100, 100},
	} {
		if got := s.Percentile(tc.p); !withinTolerance(got, tc.want) {
			t.Errorf("p%v = %v, want %v (±%v%%)", tc.p, got, tc.want, tolerancePct)
		}
	}
	if got := s.Min(); !withinTolerance(got, 1) {
		t.Errorf("min = %v, want 1", got)
	}
	if got := s.Max(); !withinTolerance(got, 100) {
		t.Errorf("max = %v, want 100", got)
	}
	if got := s.Mean(); !withinTolerance(got, 50.5) {
		t.Errorf("mean = %v, want 50.5", got)
	}
	if s.Count() != 100 {
		t.Errorf("count = %d, want 100", s.Count())
	}
}

func TestLatencyStatsSingleValue(t *testing.T) {
	var s LatencyStats
	s.Add(42)
	if got := s.Percentile(99); !withinTolerance(got, 42) {
		t.Errorf("p99 = %v, want 42", got)
	}
	if got := s.Min(); !withinTolerance(got, 42) {
		t.Errorf("min = %v, want 42", got)
	}
	if got := s.Max(); !withinTolerance(got, 42) {
		t.Errorf("max = %v, want 42", got)
	}
	if got := s.Mean(); !withinTolerance(got, 42) {
		t.Errorf("mean = %v, want 42", got)
	}
	if s.Count() != 1 {
		t.Errorf("count = %d, want 1", s.Count())
	}
}

func TestLatencyStatsSubMillisecond(t *testing.T) {
	var s LatencyStats
	s.Add(0.5)
	if s.Count() != 1 {
		t.Fatalf("count = %d, want 1", s.Count())
	}
	if got := s.Mean(); !withinTolerance(got, 0.5) {
		t.Errorf("mean = %v, want 0.5", got)
	}
	if got := s.Max(); !withinTolerance(got, 0.5) {
		t.Errorf("max = %v, want 0.5", got)
	}
}

func TestLatencyStatsClampsHugeValues(t *testing.T) {
	var s LatencyStats
	s.Add(60 * 60 * 1000) // 1 hour, above the 10 minute trackable range
	if s.Count() != 1 {
		t.Fatalf("count = %d, want 1", s.Count())
	}
	if got := s.Max(); !withinTolerance(got, trackableMaxMs) {
		t.Errorf("max = %v, want clamped to %v (±%v%%)", got, trackableMaxMs, tolerancePct)
	}
}
