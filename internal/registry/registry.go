// Package registry tracks live inference workers in Redis.
//
// Each worker periodically writes a hash under <prefix><worker id> with a TTL.
// A worker whose heartbeat stops simply expires and drops out of List, so the
// scheduler never needs an explicit failure signal to stop routing to it.
package registry

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// DefaultPrefix is the Redis key prefix for worker heartbeat hashes.
	DefaultPrefix = "workers:"
	// DefaultStreamPrefix is the Redis key prefix for per-worker job streams.
	DefaultStreamPrefix = "jobs:worker:"
	// DefaultTTL is how long a heartbeat stays valid.
	DefaultTTL = 3 * time.Second
)

// Worker describes a live worker. An empty Models list means the worker
// accepts any model.
type Worker struct {
	ID     string
	Models []string
}

// Supports reports whether the worker can serve the given model. An empty
// requested model is served by any worker.
func (w Worker) Supports(model string) bool {
	if len(w.Models) == 0 || model == "" {
		return true
	}
	return slices.Contains(w.Models, model)
}

// Registry reads and writes worker heartbeats.
type Registry struct {
	client redis.Cmdable
	prefix string
	ttl    time.Duration
}

// New builds a Registry. Empty prefix and zero ttl fall back to the defaults.
func New(client redis.Cmdable, prefix string, ttl time.Duration) *Registry {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Registry{client: client, prefix: prefix, ttl: ttl}
}

// Heartbeat registers the worker or refreshes its TTL.
func (r *Registry) Heartbeat(ctx context.Context, w Worker) error {
	if w.ID == "" {
		return fmt.Errorf("registry: worker id is empty")
	}
	key := r.prefix + w.ID
	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, key, "id", w.ID, "models", strings.Join(w.Models, ","))
	pipe.PExpire(ctx, key, r.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Deregister removes the worker immediately, used on graceful shutdown.
func (r *Registry) Deregister(ctx context.Context, id string) error {
	return r.client.Del(ctx, r.prefix+id).Err()
}

// List returns all live workers sorted by ID, so policies see a stable order.
func (r *Registry) List(ctx context.Context) ([]Worker, error) {
	var (
		cursor uint64
		keys   []string
	)
	for {
		batch, next, err := r.client.Scan(ctx, cursor, r.prefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.HGetAll(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(keys))
	workers := make([]Worker, 0, len(keys))
	for _, cmd := range cmds {
		fields := cmd.Val()
		id := fields["id"]
		if id == "" {
			continue // key expired between SCAN and HGETALL, or is malformed
		}
		if _, dup := seen[id]; dup {
			continue // SCAN may return a key more than once
		}
		seen[id] = struct{}{}
		w := Worker{ID: id}
		if m := fields["models"]; m != "" {
			w.Models = strings.Split(m, ",")
		}
		workers = append(workers, w)
	}
	slices.SortFunc(workers, func(a, b Worker) int { return strings.Compare(a.ID, b.ID) })
	return workers, nil
}
