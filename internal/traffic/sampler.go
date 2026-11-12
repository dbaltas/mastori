package traffic

import (
	"math/rand/v2"
	"sync"
)

// Sampler returns dataset row indices in a seeded shuffle-and-repeat cycle:
// all indices appear exactly once before the order is reshuffled, so sampling
// stays uniform over long runs and reproducible for a given seed.
// Safe for concurrent use.
type Sampler struct {
	mu    sync.Mutex
	n     int
	rng   *rand.Rand
	order []int
	pos   int
}

func NewSampler(n int, seed int64) *Sampler {
	if n < 1 {
		n = 1
	}
	s := &Sampler{
		n:   n,
		rng: rand.New(rand.NewPCG(uint64(seed), uint64(seed))),
	}
	s.reshuffle()
	return s
}

func (s *Sampler) reshuffle() {
	s.order = make([]int, s.n)
	for i := range s.order {
		s.order[i] = i
	}
	s.rng.Shuffle(s.n, func(i, j int) { s.order[i], s.order[j] = s.order[j], s.order[i] })
	s.pos = 0
}

// Next returns the next row index in the cycle.
func (s *Sampler) Next() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.order[s.pos]
	s.pos++
	if s.pos >= s.n {
		s.reshuffle()
	}
	return v
}

// WeightedRowSource picks a Row from weighted datasets using one RNG stream,
// so both the dataset choice and the row choice are reproducible per seed.
type WeightedRowSource struct {
	mu       sync.Mutex
	groups   [][]Row
	cum      []float64
	samplers []*Sampler
	rng      *rand.Rand
}

// NewWeightedRowSource groups rows by dataset and assigns each group a weight
// (e.g. 80% short prompts, 20% long prompts).
func NewWeightedRowSource(rows []Row, weights map[string]float64, seed int64) *WeightedRowSource {
	names := groupNames(rows)
	groups := make([][]Row, len(names))
	weightList := make([]float64, len(names))
	for i, name := range names {
		weightList[i] = weights[name]
	}
	for _, r := range rows {
		for i, name := range names {
			if r.Dataset == name {
				groups[i] = append(groups[i], r)
				break
			}
		}
	}

	samplers := make([]*Sampler, len(groups))
	for i, g := range groups {
		samplers[i] = NewSampler(len(g), seed+int64(i)+1)
	}

	total := 0.0
	for _, w := range weightList {
		if w < 0 {
			w = 0
		}
		total += w
	}
	if total == 0 {
		total = 1
	}
	cum := make([]float64, len(weightList))
	run := 0.0
	for i, w := range weightList {
		if w < 0 {
			w = 0
		}
		run += w
		cum[i] = run / total
	}

	return &WeightedRowSource{
		groups:   groups,
		cum:      cum,
		samplers: samplers,
		rng:      rand.New(rand.NewPCG(uint64(seed*31+7), uint64(seed))),
	}
}

func groupNames(rows []Row) []string {
	var names []string
	seen := make(map[string]bool)
	for _, r := range rows {
		if !seen[r.Dataset] {
			seen[r.Dataset] = true
			names = append(names, r.Dataset)
		}
	}
	return names
}

// Next returns the next prompt row according to the dataset weights.
func (s *WeightedRowSource) Next() Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.rng.Float64()
	for i, c := range s.cum {
		if u < c {
			return s.groups[i][s.samplers[i].Next()]
		}
	}
	i := len(s.cum) - 1
	return s.groups[i][s.samplers[i].Next()]
}
