package llm

import (
	"sync"
	"time"
)

type memoryBreakerEntry struct {
	state     breakerState
	expiresAt time.Time
}

type MemoryBreakerStore struct {
	mu    sync.Mutex
	items map[string]memoryBreakerEntry
}

func NewMemoryBreakerStore() *MemoryBreakerStore {
	return &MemoryBreakerStore{
		items: map[string]memoryBreakerEntry{},
	}
}

func (s *MemoryBreakerStore) Load(key string) (breakerState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[key]
	if !ok {
		return breakerState{}, false, nil
	}
	if !entry.expiresAt.IsZero() && entry.expiresAt.Before(time.Now()) {
		delete(s.items, key)
		return breakerState{}, false, nil
	}
	return entry.state, true, nil
}

func (s *MemoryBreakerStore) Save(key string, state breakerState, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := memoryBreakerEntry{state: state}
	if ttl > 0 {
		entry.expiresAt = time.Now().Add(ttl)
	}
	s.items[key] = entry
	return nil
}

func (s *MemoryBreakerStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
	return nil
}
