package worker

import (
	"context"
	"hash/fnv"
	"strings"
	"time"

	"github.com/dbaltas/mastori/internal/queue"
)

type MockGenerator struct {
	MinTokens  int
	MaxTokens  int
	TokenDelay time.Duration
	FirstDelay time.Duration
}

func (g *MockGenerator) tokenCount(prompt string) int {
	if g.MaxTokens <= g.MinTokens {
		return g.MinTokens
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(prompt))
	span := g.MaxTokens - g.MinTokens
	return g.MinTokens + int(h.Sum32()%uint32(span+1))
}

func tokenText(i int) string {
	words := []string{"the", "model", "streams", "tokens", "quickly", "across", "the", "cluster", "while", "vllm", "batches", "requests", "on", "gpu"}
	return words[i%len(words)] + " "
}

// Run processes the job: emits chunk entries to onChunk until the output
// token count is reached, then emits a final done chunk with usage.
// It stops early if ctx is canceled (abandon/cleanup/shutdown).
func (g *MockGenerator) Run(ctx context.Context, job queue.Job, onChunk func(queue.Chunk) error) error {
	start := time.Now()
	queueWaitMs := time.Since(job.CreatedAt).Milliseconds()

	tokensOut := g.tokenCount(job.Prompt)
	tokensIn := int64(len(strings.Fields(job.Prompt)))
	if tokensIn == 0 {
		tokensIn = 1
	}

	first := g.FirstDelay
	for i := 0; i < tokensOut; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(first):
		}
		first = g.TokenDelay

		chunk := queue.Chunk{JobID: job.ID, Text: tokenText(i)}
		if err := onChunk(chunk); err != nil {
			return err
		}
	}

	done := queue.Chunk{
		JobID:       job.ID,
		Done:        true,
		TokensIn:    tokensIn,
		TokensOut:   int64(tokensOut),
		QueueWaitMs: queueWaitMs,
		DurationMs:  time.Since(start).Milliseconds(),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return onChunk(done)
}
