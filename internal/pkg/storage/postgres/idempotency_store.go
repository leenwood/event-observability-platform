package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/leenwood/event-observability-platform/internal/pkg/idempotency"
)

var idemTracer = otel.Tracer("storage/postgres/idempotency")

// IdempotencyStore is the PostgreSQL-backed implementation of idempotency.Store.
type IdempotencyStore struct {
	db *DB
}

func NewIdempotencyStore(db *DB) *IdempotencyStore {
	return &IdempotencyStore{db: db}
}

func (s *IdempotencyStore) Get(ctx context.Context, key string) (*idempotency.Entry, error) {
	ctx, span := idemTracer.Start(ctx, "IdempotencyStore.Get")
	defer span.End()

	span.SetAttributes(attribute.String("idempotency.key", key))

	const q = `
		SELECT event_id, response, expires_at
		FROM idempotency_keys
		WHERE key = $1 AND expires_at > NOW()`

	var entry idempotency.Entry
	err := s.db.Pool.QueryRow(ctx, q, key).Scan(&entry.EventID, &entry.Response, &entry.ExpiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("idempotency get: %w", err)
	}
	return &entry, nil
}

func (s *IdempotencyStore) Set(ctx context.Context, key string, entry *idempotency.Entry) error {
	ctx, span := idemTracer.Start(ctx, "IdempotencyStore.Set")
	defer span.End()

	span.SetAttributes(
		attribute.String("idempotency.key", key),
		attribute.String("idempotency.event_id", entry.EventID),
	)

	const q = `
		INSERT INTO idempotency_keys (key, event_id, response, expires_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (key) DO NOTHING`

	_, err := s.db.Pool.Exec(ctx, q, key, entry.EventID, entry.Response, entry.ExpiresAt)
	if err != nil {
		return fmt.Errorf("idempotency set: %w", err)
	}
	return nil
}

