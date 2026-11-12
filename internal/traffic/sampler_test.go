package traffic

import (
	"testing"
)

func TestSamplerCycle(t *testing.T) {
	s := NewSampler(10, 1)
	seen := make(map[int]int)
	for i := 0; i < 10; i++ {
		seen[s.Next()]++
	}
	if len(seen) != 10 {
		t.Errorf("expected all 10 indices in first cycle, got %d unique", len(seen))
	}
	for i, c := range seen {
		if c != 1 {
			t.Errorf("index %d appeared %d times in first cycle", i, c)
		}
	}
	// Second cycle must contain all indices again.
	seen2 := make(map[int]int)
	for i := 0; i < 10; i++ {
		seen2[s.Next()]++
	}
	if len(seen2) != 10 {
		t.Errorf("expected 10 unique in second cycle, got %d", len(seen2))
	}
}

func TestSamplerDeterministic(t *testing.T) {
	a := NewSampler(50, 7)
	b := NewSampler(50, 7)
	for i := 0; i < 100; i++ {
		if a.Next() != b.Next() {
			t.Fatalf("sequences diverge at step %d with the same seed", i)
		}
	}
}

func TestSamplerDifferentSeeds(t *testing.T) {
	a := NewSampler(50, 1)
	b := NewSampler(50, 2)
	same := true
	for i := 0; i < 50; i++ {
		if a.Next() != b.Next() {
			same = false
			break
		}
	}
	if same {
		t.Error("different seeds produced identical sequences")
	}
}

func TestWeightedRowSourceDeterministic(t *testing.T) {
	rows := []Row{}
	for i := 0; i < 10; i++ {
		rows = append(rows, Row{Dataset: "short", Index: i, Text: "s"})
		rows = append(rows, Row{Dataset: "long", Index: i, Text: "l"})
	}
	weights := map[string]float64{"short": 80, "long": 20}

	a := NewWeightedRowSource(rows, weights, 5)
	b := NewWeightedRowSource(rows, weights, 5)
	for i := 0; i < 200; i++ {
		ra, rb := a.Next(), b.Next()
		if ra != rb {
			t.Fatalf("weighted sources diverge at step %d: %+v vs %+v", i, ra, rb)
		}
	}
}

func TestWeightedRowSourceMix(t *testing.T) {
	rows := []Row{}
	for i := 0; i < 100; i++ {
		rows = append(rows, Row{Dataset: "short", Index: i, Text: "s"})
	}
	for i := 0; i < 100; i++ {
		rows = append(rows, Row{Dataset: "long", Index: i, Text: "l"})
	}
	src := NewWeightedRowSource(rows, map[string]float64{"short": 80, "long": 20}, 11)

	counts := make(map[string]int)
	for i := 0; i < 10000; i++ {
		counts[src.Next().Dataset]++
	}
	if counts["short"] < 7300 || counts["short"] > 8700 {
		t.Errorf("short share = %d/10000, expected ~8000", counts["short"])
	}
	if counts["short"]+counts["long"] != 10000 {
		t.Errorf("unexpected dataset label: %+v", counts)
	}
}

func TestWeightedRowSourceAllGroupsRepresented(t *testing.T) {
	rows := []Row{
		{Dataset: "a", Index: 0}, {Dataset: "a", Index: 1},
		{Dataset: "b", Index: 0},
	}
	src := NewWeightedRowSource(rows, map[string]float64{"a": 1, "b": 1}, 3)
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		seen[src.Next().Dataset] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Errorf("expected both datasets, got %v", seen)
	}
}
