package internal

import (
	"context"
	"fmt"
	"github.com/tuxi/flux-workflow/repository"
)

// MemoryQueue implements repository.TaskQueue with an in-memory channel.
// Suitable for single-process (local) usage where Redis is not available.
type MemoryQueue struct {
	tasks chan int64
}

// NewMemoryQueue creates a queue with the given buffer size.
func NewMemoryQueue(bufferSize int) *MemoryQueue {
	if bufferSize <= 0 {
		bufferSize = 1024
	}
	return &MemoryQueue{
		tasks: make(chan int64, bufferSize),
	}
}

func (q *MemoryQueue) Push(_ context.Context, taskID int64) error {
	select {
	case q.tasks <- taskID:
		return nil
	default:
		return fmt.Errorf("memory queue full: cannot push task %d", taskID)
	}
}

func (q *MemoryQueue) PopAndReserve(ctx context.Context) (int64, error) {
	select {
	case taskID := <-q.tasks:
		return taskID, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (q *MemoryQueue) Ack(_ context.Context, taskID int64) error {
	// In-memory queue: task is already dequeued on PopAndReserve.
	// Ack is a no-op.
	_ = taskID
	return nil
}

func (q *MemoryQueue) MoveToDead(_ context.Context, taskID int64) error {
	// In-memory queue: dead letter queue not implemented.
	// Failed tasks are simply discarded.
	_ = taskID
	return nil
}

// ensure interface compliance
var _ repository.TaskQueue = (*MemoryQueue)(nil)
