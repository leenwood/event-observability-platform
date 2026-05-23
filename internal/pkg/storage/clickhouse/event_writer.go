package clickhouse

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/leenwood/event-observability-platform/internal/pkg/domain"
)

var writerTracer = otel.Tracer("storage/clickhouse/writer")

// EventWriter writes processed events to ClickHouse events_log table.
// For high-throughput workloads, batch buffering should be added here.
type EventWriter struct {
	db *DB
}

func NewEventWriter(db *DB) *EventWriter {
	return &EventWriter{db: db}
}

func (w *EventWriter) WriteEvent(ctx context.Context, event *domain.Event) error {
	ctx, span := writerTracer.Start(ctx, "clickhouse.WriteEvent")
	defer span.End()

	span.SetAttributes(
		attribute.String("event.id", event.ID),
		attribute.String("event.source", event.Source),
	)

	processedAt := time.Now().UTC()
	if event.ProcessedAt != nil {
		processedAt = *event.ProcessedAt
	}
	date := processedAt.Truncate(24 * time.Hour)

	batch, err := w.db.Conn().PrepareBatch(ctx, "INSERT INTO events_log")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "prepare batch")
		return fmt.Errorf("analytics prepare batch: %w", err)
	}

	if err := batch.Append(
		event.ID,
		event.Source,
		event.EventType,
		string(event.Status),
		processedAt,
		date,
	); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "append row")
		return fmt.Errorf("analytics append row: %w", err)
	}

	if err := batch.Send(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "send batch")
		return fmt.Errorf("analytics send batch: %w", err)
	}

	return nil
}
