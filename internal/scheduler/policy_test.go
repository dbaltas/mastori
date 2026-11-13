package scheduler

import (
	"testing"

	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/registry"
)

func cands(loads ...int64) []Candidate {
	out := make([]Candidate, len(loads))
	for i, l := range loads {
		out[i] = Candidate{Worker: registry.Worker{ID: string(rune('a' + i))}, Load: l}
	}
	return out
}

func TestNewPolicy(t *testing.T) {
	for _, name := range []string{"round-robin", "least-loaded"} {
		p, err := NewPolicy(name)
		if err != nil || p.Name() != name {
			t.Errorf("NewPolicy(%q) = %v, %v", name, p, err)
		}
	}
	if _, err := NewPolicy("random"); err == nil {
		t.Error("expected error for unknown policy")
	}
}

func TestRoundRobinCycles(t *testing.T) {
	p := &RoundRobin{}
	c := cands(0, 0, 0)
	var got []int
	for range 7 {
		got = append(got, p.Select(queue.Job{}, c))
	}
	want := []int{0, 1, 2, 0, 1, 2, 0}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("picks = %v, want %v", got, want)
		}
	}
}

func TestRoundRobinIgnoresLoad(t *testing.T) {
	p := &RoundRobin{}
	if p.NeedsLoad() {
		t.Error("round-robin must not request load")
	}
	c := cands(100, 0)
	if a, b := p.Select(queue.Job{}, c), p.Select(queue.Job{}, c); a != 0 || b != 1 {
		t.Errorf("picks = %d,%d, want 0,1", a, b)
	}
}

func TestRoundRobinSurvivesMembershipChange(t *testing.T) {
	p := &RoundRobin{}
	three := cands(0, 0, 0)      // a b c
	p.Select(queue.Job{}, three) // a
	p.Select(queue.Job{}, three) // b

	// b leaves; next after "b" in the remaining set {a, c} is c.
	remaining := []Candidate{three[0], three[2]}
	if idx := p.Select(queue.Job{}, remaining); remaining[idx].ID != "c" {
		t.Errorf("picked %q, want c", remaining[idx].ID)
	}
	// c was last and is the highest ID, so wrap to a.
	if idx := p.Select(queue.Job{}, remaining); remaining[idx].ID != "a" {
		t.Errorf("picked %q, want a", remaining[idx].ID)
	}
}

func TestRoundRobinNoCandidates(t *testing.T) {
	if idx := (&RoundRobin{}).Select(queue.Job{}, nil); idx != -1 {
		t.Errorf("idx = %d, want -1", idx)
	}
}

func TestLeastLoadedPicksMinimum(t *testing.T) {
	p := &LeastLoaded{}
	if !p.NeedsLoad() {
		t.Error("least-loaded must request load")
	}
	if idx := p.Select(queue.Job{}, cands(18, 4, 9)); idx != 1 {
		t.Errorf("idx = %d, want 1 (load 4)", idx)
	}
}

func TestLeastLoadedRotatesTies(t *testing.T) {
	p := &LeastLoaded{}
	c := cands(5, 2, 2, 7)
	var got []int
	for range 4 {
		got = append(got, p.Select(queue.Job{}, c))
	}
	want := []int{1, 2, 1, 2}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("picks = %v, want %v", got, want)
		}
	}
}

func TestLeastLoadedNoCandidates(t *testing.T) {
	if idx := (&LeastLoaded{}).Select(queue.Job{}, nil); idx != -1 {
		t.Errorf("idx = %d, want -1", idx)
	}
}
