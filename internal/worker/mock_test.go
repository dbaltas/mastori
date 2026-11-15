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

func TestJitterFactorWithinBounds(t *testing.T) {
	for _, j := range []float64{0, 0.25, 0.5, 1} {
		for i := 0; i < 1000; i++ {
			f := jitterFactor(j)
			if j == 0 && f != 1 {
				t.Fatalf("jitterFactor(0) = %f, want 1", f)
			}
			if f < 1-j || f > 1+j {
				t.Fatalf("jitterFactor(%f) = %f outside [1-j, 1+j]", j, f)
			}
		}
	}
}

func TestJitterDelayWithinBounds(t *testing.T) {
	base := 10 * time.Millisecond
	for _, j := range []float64{0, 0.3, 0.5, 1} {
		for i := 0; i < 1000; i++ {
			d := jitterDelay(base, j)
			if j == 0 && d != base {
				t.Fatalf("jitterDelay(base, 0) = %v, want base", d)
			}
			if d < time.Duration(float64(base)*(1-j)) || d > time.Duration(float64(base)*(1+j))+time.Nanosecond {
				t.Fatalf("jitterDelay(%v, %f) = %v outside bounds", base, j, d)
			}
			if d < 0 {
				t.Fatalf("jitterDelay(%v, %f) = %v, want >= 0", base, j, d)
			}
		}
	}
	if got := jitterDelay(0, 0.5); got != 0 {
		t.Errorf("jitterDelay(0, 0.5) = %v, want 0", got)
	}
}

func TestRunJitteredTokenCountStaysWithinBounds(t *testing.T) {
	base := 50
	g := &MockGenerator{
		MinTokens:        base,
		MaxTokens:        base,
		TokenDelay:       time.Microsecond,
		FirstDelay:       time.Microsecond,
		TokenCountJitter: 0.5,
	}
	for i := 0; i < 50; i++ {
		chunks := collectChunks(t, g, "p")
		n := int(chunks[len(chunks)-1].TokensOut)
		if n < base/2 || n > base+base/2 {
			t.Fatalf("tokens_out = %d, outside jittered [25, 75]", n)
		}
	}
}

func TestRunJitteredDelaysStayWithinBounds(t *testing.T) {
	base := 2 * time.Millisecond
	g := &MockGenerator{
		MinTokens:        5,
		MaxTokens:        5,
		TokenDelay:       base,
		FirstDelay:       base,
		TokenDelayJitter: 1,
		FirstDelayJitter: 1,
	}
	start := time.Now()
	collectChunks(t, g, "p")
	elapsed := time.Since(start)
	if elapsed > 5*base*(1+1)+10*time.Millisecond {
		t.Fatalf("elapsed = %v, want <= 5 * base * (1+j) + scheduling slack", elapsed)
	}
}

func TestRunWithoutJitterPreservesCount(t *testing.T) {
	g := &MockGenerator{
		MinTokens:        7,
		MaxTokens:        7,
		TokenDelay:       time.Microsecond,
		FirstDelay:       time.Microsecond,
		TokenCountJitter: 0,
		TokenDelayJitter: 0,
		FirstDelayJitter: 0,
	}
	chunks := collectChunks(t, g, "p")
	if len(chunks) != 8 {
		t.Fatalf("chunks = %d, want 7 text + 1 done", len(chunks))
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
