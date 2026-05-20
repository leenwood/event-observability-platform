package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/leenwood/event-observability-platform/internal/app"
)

var writerTracer = otel.Tracer("analytics/writer")

// Writer is the interface the worker uses to record processed events.
type Writer interface {
	WriteEvent(ctx context.Context, event *app.Event) error
}

// EventWriter writes processed events to ClickHouse events_log table.
// For high-throughput workloads, batch buffering should be added here.
type EventWriter struct {
	conn driver.Conn
}

func NewEventWriter(conn driver.Conn) *EventWriter {
	return &EventWriter{conn: conn}
}

func (w *EventWriter) WriteEvent(ctx context.Context, event *app.Event) error {
	ctx, span := writerTracer.Start(ctx, "analytics.WriteEvent")
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

	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO events_log")
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
