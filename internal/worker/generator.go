package worker

import (
	"context"

	"github.com/dbaltas/mastori/internal/queue"
)

// Generator produces the completion for a job, streaming Chunk entries
// through onChunk and finishing with a Done chunk that carries usage.
type Generator interface {
	Run(ctx context.Context, job queue.Job, onChunk func(queue.Chunk) error) error
}
