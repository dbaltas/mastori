package scheduler

import (
	"fmt"
	"sync"

	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/registry"
)

// Candidate is a healthy worker that can serve the job being scheduled.
// Load is the number of jobs dispatched to the worker that it has not yet
// finished (queued in its stream plus in progress). It is only populated when
// the policy reports NeedsLoad.
type Candidate struct {
	registry.Worker
	Load int64
}

// Policy picks which worker receives a job.
type Policy interface {
	Name() string
	// NeedsLoad reports whether Select reads Candidate.Load. Policies that do
	// not need it let the scheduler skip the per-dispatch load lookup.
	NeedsLoad() bool
	// Select returns the index of the chosen candidate, or -1 if none can be
	// chosen. Candidates are non-empty and sorted by worker ID.
	Select(job queue.Job, candidates []Candidate) int
}

// NewPolicy returns the policy registered under name.
func NewPolicy(name string) (Policy, error) {
	switch name {
	case "round-robin":
		return &RoundRobin{}, nil
	case "least-loaded":
		return &LeastLoaded{}, nil
	default:
		return nil, fmt.Errorf("scheduler: unknown policy %q (want round-robin or least-loaded)", name)
	}
}

// RoundRobin cycles through workers in ID order. It remembers the last worker
// by ID rather than by position, so the rotation stays fair when workers join
// or leave between jobs.
type RoundRobin struct {
	mu   sync.Mutex
	last string
}

func (*RoundRobin) Name() string    { return "round-robin" }
func (*RoundRobin) NeedsLoad() bool { return false }

func (p *RoundRobin) Select(_ queue.Job, candidates []Candidate) int {
	if len(candidates) == 0 {
		return -1
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	idx := 0 // wrap to the first worker if none sorts after the last pick
	for i, c := range candidates {
		if c.ID > p.last {
			idx = i
			break
		}
	}
	p.last = candidates[idx].ID
	return idx
}

// LeastLoaded picks the candidate with the fewest unfinished jobs. Ties are
// rotated so idle workers share work instead of the lowest ID taking it all.
type LeastLoaded struct {
	mu   sync.Mutex
	next int
}

func (*LeastLoaded) Name() string    { return "least-loaded" }
func (*LeastLoaded) NeedsLoad() bool { return true }

func (p *LeastLoaded) Select(_ queue.Job, candidates []Candidate) int {
	if len(candidates) == 0 {
		return -1
	}
	min := candidates[0].Load
	for _, c := range candidates[1:] {
		if c.Load < min {
			min = c.Load
		}
	}
	var ties []int
	for i, c := range candidates {
		if c.Load == min {
			ties = append(ties, i)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	idx := ties[p.next%len(ties)]
	p.next++
	return idx
}
