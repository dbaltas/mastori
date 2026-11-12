package traffic

import (
	"math"
	"sort"
)

// LatencyStats accumulates latencies of successful measured requests and
// computes the standard benchmark percentiles.
type LatencyStats struct {
	values []float64 // milliseconds
}

func (s *LatencyStats) Add(ms float64) { s.values = append(s.values, ms) }

func (s *LatencyStats) Count() int { return len(s.values) }

func (s *LatencyStats) sorted() []float64 {
	out := make([]float64, len(s.values))
	copy(out, s.values)
	sort.Float64s(out)
	return out
}

// Percentile returns the p'th percentile (0..100) using the nearest-rank
// method: the smallest value in the sorted list whose rank is >= ceil(p/100*N).
func (s *LatencyStats) Percentile(p float64) float64 {
	if len(s.values) == 0 {
		return 0
	}
	out := s.sorted()
	rank := int(math.Ceil(float64(len(out)) * p / 100))
	if rank < 1 {
		rank = 1
	}
	if rank > len(out) {
		rank = len(out)
	}
	return out[rank-1]
}

func (s *LatencyStats) Min() float64 {
	if len(s.values) == 0 {
		return 0
	}
	out := s.sorted()
	return out[0]
}

func (s *LatencyStats) Max() float64 {
	if len(s.values) == 0 {
		return 0
	}
	out := s.sorted()
	return out[len(out)-1]
}

func (s *LatencyStats) Mean() float64 {
	if len(s.values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range s.values {
		sum += v
	}
	return sum / float64(len(s.values))
}
