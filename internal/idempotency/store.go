package idempotency

import (
	"context"
	"errors"
	"time"
)

var ErrDuplicate = errors.New("idempotency: duplicate request")

type Entry struct {
	EventID   string
	Response  []byte
	ExpiresAt time.Time
}

func (e *Entry) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}

// Store checks and stores idempotency keys.
// Implementations must be safe for concurrent use.
type Store interface {
	// Get returns the entry for the given key, or nil if not found or expired.
	Get(ctx context.Context, key string) (*Entry, error)

	// Set stores the entry under key. If the key already exists, it is a no-op.
	Set(ctx context.Context, key string, entry *Entry) error
}
