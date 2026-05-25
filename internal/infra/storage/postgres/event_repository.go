package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/leenwood/event-observability-platform/internal/core/domain"
)

var tracer = otel.Tracer("storage/postgres")

type EventRepository struct {
	db *DB
}

func NewEventRepository(db *DB) *EventRepository {
	return &EventRepository{db: db}
}

func (r *EventRepository) Insert(ctx context.Context, event *domain.Event) error {
	ctx, span := tracer.Start(ctx, "EventRepository.Insert")
	defer span.End()

	span.SetAttributes(
		attribute.String("event.source", event.Source),
		attribute.String("event.type", event.EventType),
	)

	const q = `
		INSERT INTO webhook_events
			(id, idempotency_key, source, event_type, payload, status, retry_count, created_at)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err := r.db.Pool.Exec(ctx, q,
		event.ID,
		event.IdempotencyKey,
		event.Source,
		event.EventType,
		event.Payload,
		string(event.Status),
		event.RetryCount,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

func (r *EventRepository) FindByID(ctx context.Context, id string) (*domain.Event, error) {
	ctx, span := tracer.Start(ctx, "EventRepository.FindByID")
	defer span.End()

	const q = `
		SELECT id, idempotency_key, source, event_type, payload, status,
		       retry_count, created_at, processed_at
		FROM webhook_events
		WHERE id = $1`

	row := r.db.Pool.QueryRow(ctx, q, id)
	event, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("find event by id: %w", err)
	}
	return event, nil
}

func (r *EventRepository) UpdateStatus(ctx context.Context, id string, status domain.EventStatus) error {
	ctx, span := tracer.Start(ctx, "EventRepository.UpdateStatus")
	defer span.End()

	span.SetAttributes(attribute.String("event.status", string(status)))

	var processedAt *time.Time
	if status == domain.EventStatusProcessed {
		now := time.Now().UTC()
		processedAt = &now
	}

	const q = `
		UPDATE webhook_events
		SET status = $1, processed_at = $2
		WHERE id = $3`

	tag, err := r.db.Pool.Exec(ctx, q, string(status), processedAt, id)
	if err != nil {
		return fmt.Errorf("update event status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (r *EventRepository) IncrementRetry(ctx context.Context, id string) error {
	ctx, span := tracer.Start(ctx, "EventRepository.IncrementRetry")
	defer span.End()

	const q = `UPDATE webhook_events SET retry_count = retry_count + 1 WHERE id = $1`

	tag, err := r.db.Pool.Exec(ctx, q, id)
	if err != nil {
		return fmt.Errorf("increment retry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (r *EventRepository) ListByStatus(ctx context.Context, status domain.EventStatus, limit int) ([]*domain.Event, error) {
	ctx, span := tracer.Start(ctx, "EventRepository.ListByStatus")
	defer span.End()

	span.SetAttributes(attribute.String("event.status", string(status)))

	const q = `
		SELECT id, idempotency_key, source, event_type, payload, status,
		       retry_count, created_at, processed_at
		FROM webhook_events
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2`

	rows, err := r.db.Pool.Query(ctx, q, string(status), limit)
	if err != nil {
		return nil, fmt.Errorf("list events by status: %w", err)
	}
	defer rows.Close()

	var events []*domain.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanEvent(s scanner) (*domain.Event, error) {
	var e domain.Event
	var status string
	err := s.Scan(
		&e.ID,
		&e.IdempotencyKey,
		&e.Source,
		&e.EventType,
		&e.Payload,
		&status,
		&e.RetryCount,
		&e.CreatedAt,
		&e.ProcessedAt,
	)
	if err != nil {
		return nil, err
	}
	e.Status = domain.EventStatus(status)
	return &e, nil
}
