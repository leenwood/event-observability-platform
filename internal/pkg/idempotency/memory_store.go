package idempotency

import (
	"context"
	"sync"
)

// MemoryStore is an in-memory Store used in tests.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string]*Entry)}
}

func (s *MemoryStore) Get(_ context.Context, key string) (*Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.entries[key]
	if !ok || entry.IsExpired() {
		return nil, nil
	}
	return entry, nil
}

func (s *MemoryStore) Set(_ context.Context, key string, entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.entries[key]; exists {
		return nil
	}
	s.entries[key] = entry
	return nil
}
