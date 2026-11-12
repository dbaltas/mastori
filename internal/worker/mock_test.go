package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbaltas/mastori/internal/queue"
)

func collectChunks(t *testing.T, g *MockGenerator, prompt string) []queue.Chunk {
	t.Helper()
	var chunks []queue.Chunk
	err := g.Run(context.Background(), queue.Job{
		ID:        "job-1",
		Prompt:    prompt,
		CreatedAt: time.Now().UTC().Add(-10 * time.Millisecond),
	}, func(c queue.Chunk) error {
		chunks = append(chunks, c)
		return nil
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return chunks
}

func TestRunEmitsChunksAndDone(t *testing.T) {
	g := &MockGenerator{
		MinTokens:  5,
		MaxTokens:  5,
		TokenDelay: time.Millisecond,
		FirstDelay: time.Millisecond,
	}
	chunks := collectChunks(t, g, "hello world test")

	if len(chunks) != 6 {
		t.Fatalf("chunks = %d, want 5 text + 1 done", len(chunks))
	}
	last := chunks[len(chunks)-1]
	if !last.Done {
		t.Error("final chunk not marked done")
	}
	if last.TokensIn != 3 {
		t.Errorf("tokens_in = %d, want 3", last.TokensIn)
	}
	if last.TokensOut != 5 {
		t.Errorf("tokens_out = %d, want 5", last.TokensOut)
	}
	if last.QueueWaitMs < 10 {
		t.Errorf("queue_wait_ms = %d, want >= 10 (simulated queue time)", last.QueueWaitMs)
	}
	var sb strings.Builder
	for _, c := range chunks[:len(chunks)-1] {
		sb.WriteString(c.Text)
	}
	if !strings.HasPrefix(sb.String(), "the model ") {
		t.Errorf("unexpected token text: %q", sb.String())
	}
}

func TestTokenCountDeterministicFromPrompt(t *testing.T) {
	g := &MockGenerator{MinTokens: 10, MaxTokens: 110, TokenDelay: time.Millisecond}
	for _, p := range []string{"a", "a much longer prompt about gpu scheduling", "zzz"} {
		n1 := g.tokenCount(p)
		n2 := g.tokenCount(p)
		if n1 != n2 || n1 < 10 || n1 > 110 {
			t.Errorf("tokenCount(%q) = %d vs %d, outside [10,110]", p, n1, n2)
		}
	}
}

func TestRunRespectsContextCancel(t *testing.T) {
	g := &MockGenerator{
		MinTokens:  1000,
		MaxTokens:  1000,
		TokenDelay: 10 * time.Millisecond,
		FirstDelay: 10 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	emitted := 0
	err := g.Run(ctx, queue.Job{ID: "job-x", Prompt: "p", CreatedAt: time.Now().UTC()}, func(c queue.Chunk) error {
		emitted++
		return nil
	})
	if err == nil {
		t.Fatal("expected context error")
	}
	if emitted == 0 {
		t.Error("no chunks emitted before cancel")
	}
}

func TestRunPropagatesChunkError(t *testing.T) {
	g := &MockGenerator{MinTokens: 3, MaxTokens: 3, TokenDelay: time.Millisecond, FirstDelay: time.Millisecond}
	want := errors.New("redis down")
	err := g.Run(context.Background(), queue.Job{ID: "job-y", Prompt: "p", CreatedAt: time.Now().UTC()}, func(c queue.Chunk) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
}
