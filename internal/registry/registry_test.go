package registry

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRegistry(t *testing.T, ttl time.Duration) (*Registry, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(client, "", ttl), mr
}

func TestHeartbeatAndList(t *testing.T) {
	r, _ := newTestRegistry(t, time.Second)
	ctx := context.Background()

	for _, w := range []Worker{{ID: "b"}, {ID: "a", Models: []string{"m1", "m2"}}} {
		if err := r.Heartbeat(ctx, w); err != nil {
			t.Fatalf("heartbeat %s: %v", w.ID, err)
		}
	}

	got, err := r.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []Worker{{ID: "a", Models: []string{"m1", "m2"}}, {ID: "b"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("list = %+v, want %+v (sorted by id)", got, want)
	}
}

func TestListEmpty(t *testing.T) {
	r, _ := newTestRegistry(t, time.Second)
	got, err := r.List(context.Background())
	if err != nil || len(got) != 0 {
		t.Errorf("list = %v, %v; want empty", got, err)
	}
}

func TestWorkerExpiresWithoutHeartbeat(t *testing.T) {
	r, mr := newTestRegistry(t, time.Second)
	ctx := context.Background()
	if err := r.Heartbeat(ctx, Worker{ID: "w1"}); err != nil {
		t.Fatal(err)
	}

	mr.FastForward(2 * time.Second)

	got, err := r.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expired worker still listed: %+v", got)
	}
}

func TestHeartbeatRefreshesTTL(t *testing.T) {
	r, mr := newTestRegistry(t, time.Second)
	ctx := context.Background()
	_ = r.Heartbeat(ctx, Worker{ID: "w1"})
	mr.FastForward(800 * time.Millisecond)
	_ = r.Heartbeat(ctx, Worker{ID: "w1"})
	mr.FastForward(800 * time.Millisecond)

	got, _ := r.List(ctx)
	if len(got) != 1 {
		t.Errorf("refreshed worker should still be listed, got %+v", got)
	}
}

func TestDeregister(t *testing.T) {
	r, _ := newTestRegistry(t, time.Second)
	ctx := context.Background()
	_ = r.Heartbeat(ctx, Worker{ID: "w1"})
	_ = r.Heartbeat(ctx, Worker{ID: "w2"})

	if err := r.Deregister(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	got, _ := r.List(ctx)
	if len(got) != 1 || got[0].ID != "w2" {
		t.Errorf("list after deregister = %+v", got)
	}
}

func TestHeartbeatRejectsEmptyID(t *testing.T) {
	r, _ := newTestRegistry(t, time.Second)
	if err := r.Heartbeat(context.Background(), Worker{}); err == nil {
		t.Error("expected error for empty worker id")
	}
}

func TestSupports(t *testing.T) {
	cases := []struct {
		name   string
		worker Worker
		model  string
		want   bool
	}{
		{"no models accepts any", Worker{ID: "a"}, "x", true},
		{"listed model", Worker{ID: "a", Models: []string{"x", "y"}}, "y", true},
		{"unlisted model", Worker{ID: "a", Models: []string{"x"}}, "z", false},
		{"empty request model", Worker{ID: "a", Models: []string{"x"}}, "", true},
	}
	for _, c := range cases {
		if got := c.worker.Supports(c.model); got != c.want {
			t.Errorf("%s: Supports(%q) = %v, want %v", c.name, c.model, got, c.want)
		}
	}
}
