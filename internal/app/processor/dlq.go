package processor

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/leenwood/event-observability-platform/internal/pkg/domain"
	"github.com/leenwood/event-observability-platform/internal/pkg/messaging"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/logger"
)

var dlqTracer = otel.Tracer("processor/dlq")

// DLQHandler consumes poison messages from the dead letter queue,
// logs structured details, and marks events as failed in PostgreSQL.
type DLQHandler struct {
	consumer *messaging.Consumer
	events   domain.EventRepository
	log      *slog.Logger
}

func NewDLQHandler(
	consumer *messaging.Consumer,
	events domain.EventRepository,
	log *slog.Logger,
) *DLQHandler {
	return &DLQHandler{consumer: consumer, events: events, log: log}
}

func (h *DLQHandler) Run(ctx context.Context) error {
	for {
		msg, err := h.consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			h.log.Error("dlq fetch failed", slog.String("error", err.Error()))
			time.Sleep(time.Second)
			continue
		}

		carrier := messaging.NewHeaderCarrier(msg.Headers)
		msgCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

		msgCtx, span := dlqTracer.Start(msgCtx, "dlq.handle",
			trace.WithSpanKind(trace.SpanKindConsumer),
		)

		evtMsg, err := domain.UnmarshalEventMessage(msg.Value)
		if err != nil {
			h.log.ErrorContext(msgCtx, "dlq: unparseable message",
				slog.String("error", err.Error()),
				slog.String("raw", string(msg.Value)),
			)
			span.End()
			_ = h.consumer.CommitMessages(ctx, msg)
			continue
		}

		span.SetAttributes(
			attribute.String("event.id", evtMsg.EventID),
			attribute.Int("event.attempt", evtMsg.Attempt),
		)

		log := logger.FromContext(msgCtx, h.log)
		log.ErrorContext(msgCtx, "poison message received",
			slog.String("event_id", evtMsg.EventID),
			slog.Int("attempt", evtMsg.Attempt),
			slog.String("topic", msg.Topic),
			slog.Int64("offset", msg.Offset),
			slog.Int("partition", msg.Partition),
		)

		if err := h.events.UpdateStatus(msgCtx, evtMsg.EventID, domain.EventStatusFailed); err != nil {
			log.ErrorContext(msgCtx, "dlq: failed to mark event as failed",
				slog.String("error", err.Error()),
				slog.String("event_id", evtMsg.EventID),
			)
		}

		span.End()

		if err := h.consumer.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			h.log.Error("dlq commit failed", slog.String("error", err.Error()))
		}
	}
}
