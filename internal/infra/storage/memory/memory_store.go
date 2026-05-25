package memory

import (
	"context"
	"sync"

	"github.com/leenwood/event-observability-platform/internal/core/dto"
)

// MemoryStore is an in-memory IdempotencyStore used in tests.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]*dto.Entry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string]*dto.Entry)}
}

func (s *MemoryStore) Get(_ context.Context, key string) (*dto.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.entries[key]
	if !ok || entry.IsExpired() {
		return nil, nil
	}
	return entry, nil
}

func (s *MemoryStore) Set(_ context.Context, key string, entry *dto.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.entries[key]; exists {
		return nil
	}
	s.entries[key] = entry
	return nil
}
