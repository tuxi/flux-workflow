package internal

import (
	"context"
	"sync"
	"time"

	"github.com/tuxi/flux-workflow/pkg/lock"
)

// MemoryLock implements lock.DistributedLock with an in-memory mutex.
// Suitable for single-process (local) usage where Redis is not available.
// Only one goroutine can hold the lock at a time.
type MemoryLock struct {
	mu sync.Mutex
}

func NewMemoryLock() *MemoryLock {
	return &MemoryLock{}
}

func (l *MemoryLock) Lock(_ context.Context, _ string, _ time.Duration) (bool, func(), error) {
	l.mu.Lock()
	unlock := func() {
		l.mu.Unlock()
	}
	return true, unlock, nil
}

// ensure interface compliance
var _ lock.DistributedLock = (*MemoryLock)(nil)
