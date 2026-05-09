package idempotency

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("idempotency/postgres")

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) Get(ctx context.Context, key string) (*Entry, error) {
	ctx, span := tracer.Start(ctx, "IdempotencyStore.Get")
	defer span.End()

	span.SetAttributes(attribute.String("idempotency.key", key))

	const q = `
		SELECT event_id, response, expires_at
		FROM idempotency_keys
		WHERE key = $1 AND expires_at > NOW()`

	var entry Entry
	err := s.pool.QueryRow(ctx, q, key).Scan(&entry.EventID, &entry.Response, &entry.ExpiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("idempotency get: %w", err)
	}
	return &entry, nil
}

func (s *PostgresStore) Set(ctx context.Context, key string, entry *Entry) error {
	ctx, span := tracer.Start(ctx, "IdempotencyStore.Set")
	defer span.End()

	span.SetAttributes(
		attribute.String("idempotency.key", key),
		attribute.String("idempotency.event_id", entry.EventID),
	)

	const q = `
		INSERT INTO idempotency_keys (key, event_id, response, expires_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (key) DO NOTHING`

	_, err := s.pool.Exec(ctx, q, key, entry.EventID, entry.Response, entry.ExpiresAt)
	if err != nil {
		return fmt.Errorf("idempotency set: %w", err)
	}
	return nil
}

// TTL returns the standard expiry time for a new entry.
func TTL(d time.Duration) time.Time {
	return time.Now().Add(d)
}
