package port

import (
	"context"

	"github.com/leenwood/event-observability-platform/internal/core/dto"
)

// IdempotencyStore checks and stores idempotency keys.
// Implementations must be safe for concurrent use.
type IdempotencyStore interface {
	// Get returns the entry for the given key, or nil if not found or expired.
	Get(ctx context.Context, key string) (*dto.Entry, error)

	// Set stores the entry under key. If the key already exists, it is a no-op.
	Set(ctx context.Context, key string, entry *dto.Entry) error
}
