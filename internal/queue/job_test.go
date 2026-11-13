package queue

import (
	"testing"
	"time"
)

func TestJobValuesRoundTrip(t *testing.T) {
	in := Job{
		ID:        "j1",
		TraceID:   "t1",
		Model:     "m",
		Prompt:    "hello there",
		CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
	}
	out, ok := JobFromValues(JobValues(in))
	if !ok {
		t.Fatal("round trip reported malformed")
	}
	if out.ID != in.ID || out.TraceID != in.TraceID || out.Model != in.Model || out.Prompt != in.Prompt || !out.CreatedAt.Equal(in.CreatedAt) {
		t.Errorf("round trip = %+v, want %+v", out, in)
	}
}

func TestJobFromValuesMalformed(t *testing.T) {
	for name, values := range map[string]map[string]any{
		"empty":          {},
		"missing id":     {"prompt": "p"},
		"missing prompt": {"job_id": "j"},
		"wrong types":    {"job_id": 1, "prompt": 2},
	} {
		if _, ok := JobFromValues(values); ok {
			t.Errorf("%s: expected malformed", name)
		}
	}
}

func TestJobFromValuesBadCreatedAtFallsBackToNow(t *testing.T) {
	before := time.Now().UTC().Add(-time.Second)
	job, ok := JobFromValues(map[string]any{"job_id": "j", "prompt": "p", "created_at": "garbage"})
	if !ok {
		t.Fatal("unexpected malformed")
	}
	if job.CreatedAt.Before(before) {
		t.Errorf("created_at %v not close to now", job.CreatedAt)
	}
}
