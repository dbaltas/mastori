package traffic

import "testing"

func TestLatencyStats(t *testing.T) {
	var s LatencyStats
	if s.Percentile(95) != 0 || s.Mean() != 0 {
		t.Error("empty stats should be zero")
	}

	// 100 values 1..100 ms.
	for i := 1; i <= 100; i++ {
		s.Add(float64(i))
	}
	if got := s.Percentile(0); got != 1 {
		t.Errorf("p0 = %v, want 1", got)
	}
	if got := s.Percentile(50); got != 50 {
		t.Errorf("p50 = %v, want 50", got)
	}
	if got := s.Percentile(95); got != 95 {
		t.Errorf("p95 = %v, want 95", got)
	}
	if got := s.Percentile(99); got != 99 {
		t.Errorf("p99 = %v, want 99", got)
	}
	if got := s.Percentile(100); got != 100 {
		t.Errorf("p100 = %v, want 100", got)
	}
	if got := s.Min(); got != 1 {
		t.Errorf("min = %v", got)
	}
	if got := s.Max(); got != 100 {
		t.Errorf("max = %v", got)
	}
	if got := s.Mean(); got != 50.5 {
		t.Errorf("mean = %v, want 50.5", got)
	}
	if s.Count() != 100 {
		t.Errorf("count = %d", s.Count())
	}
}

func TestLatencyStatsSingleValue(t *testing.T) {
	var s LatencyStats
	s.Add(42)
	if s.Percentile(99) != 42 || s.Min() != 42 || s.Max() != 42 || s.Mean() != 42 {
		t.Error("single value stats wrong")
	}
}
