package internal

import (
	"context"
	"flux-workflow/engine"
	"sync"
)

// MemJobQueue implements engine.AsyncJobQueue with an in-memory channel.
// Suitable for single-process (local) usage.
type MemJobQueue struct {
	mu    sync.Mutex
	jobs  chan *engine.AsyncJob
	acked map[string]struct{} // track acked message IDs
}

func NewMemJobQueue(bufferSize int) *MemJobQueue {
	if bufferSize <= 0 {
		bufferSize = 256
	}
	return &MemJobQueue{
		jobs:  make(chan *engine.AsyncJob, bufferSize),
		acked: make(map[string]struct{}),
	}
}

func (q *MemJobQueue) Publish(_ context.Context, job engine.AsyncJob) error {
	cp := job
	select {
	case q.jobs <- &cp:
		return nil
	default:
		// Non-blocking fallback: drop if full
		return nil
	}
}

func (q *MemJobQueue) Consume(ctx context.Context, group string, consumer string) (*engine.AsyncJob, string, error) {
	_ = group
	_ = consumer
	select {
	case job := <-q.jobs:
		return job, "", nil
	case <-ctx.Done():
		return nil, "", ctx.Err()
	}
}

func (q *MemJobQueue) Ack(_ context.Context, id string) error {
	q.mu.Lock()
	q.acked[id] = struct{}{}
	q.mu.Unlock()
	return nil
}

var _ engine.AsyncJobQueue = (*MemJobQueue)(nil)
