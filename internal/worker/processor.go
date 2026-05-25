package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/leenwood/event-observability-platform/internal/app"
	"github.com/leenwood/event-observability-platform/internal/integrations/kafka"
	"github.com/leenwood/event-observability-platform/internal/metrics"
	"github.com/leenwood/event-observability-platform/internal/observability/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var processorTracer = otel.Tracer("worker/processor")

type Topics struct {
	Events string
	DLQ    string
}

type Processor struct {
	consumer   *kafka.Consumer
	producer   *kafka.Producer
	events     app.EventRepository
	metrics    *metrics.Metrics
	log        *slog.Logger
	topics     Topics
	maxRetries int
}

func NewProcessor(
	consumer *kafka.Consumer,
	producer *kafka.Producer,
	events app.EventRepository,
	m *metrics.Metrics,
	log *slog.Logger,
	topics Topics,
	maxRetries int,
) *Processor {
	return &Processor{
		consumer:   consumer,
		producer:   producer,
		events:     events,
		metrics:    m,
		log:        log,
		topics:     topics,
		maxRetries: maxRetries,
	}
}

// Run consumes from the events topic until ctx is cancelled.
// It reports queue lag every 15 seconds via the metrics gauge.
func (p *Processor) Run(ctx context.Context) error {
	go p.reportLag(ctx)

	for {
		msg, err := p.consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.log.Error("fetch message failed", slog.String("error", err.Error()))
			time.Sleep(time.Second)
			continue
		}

		p.processMessage(ctx, msg)

		if err := p.consumer.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.log.Error("commit message failed", slog.String("error", err.Error()))
		}
	}
}

func (p *Processor) processMessage(ctx context.Context, raw kafkago.Message) {
	// Extract trace context from Kafka headers.
	carrier := kafka.NewHeaderCarrier(raw.Headers)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	ctx, span := processorTracer.Start(ctx, "worker.processMessage",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	log := logger.FromContext(ctx, p.log)

	evtMsg, err := UnmarshalEventMessage(raw.Value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "unmarshal failed")
		log.Error("unparseable message — skipping", slog.String("error", err.Error()))
		return
	}

	span.SetAttributes(
		attribute.String("event.id", evtMsg.EventID),
		attribute.Int("event.attempt", evtMsg.Attempt),
	)

	log = log.With(
		slog.String("event_id", evtMsg.EventID),
		slog.Int("attempt", evtMsg.Attempt),
	)

	event, err := p.events.FindByID(ctx, evtMsg.EventID)
	if err != nil {
		if errors.Is(err, app.ErrNotFound) {
			log.Warn("event not found — skipping")
			return
		}
		log.Error("load event failed", slog.String("error", err.Error()))
		p.handleFailure(ctx, evtMsg, err)
		return
	}

	if event.Status == app.EventStatusProcessed || event.Status == app.EventStatusFailed {
		log.Info("event already in terminal state — skipping", slog.String("status", string(event.Status)))
		return
	}

	if err := p.events.UpdateStatus(ctx, event.ID, app.EventStatusProcessing); err != nil {
		log.Error("set processing status failed", slog.String("error", err.Error()))
		p.handleFailure(ctx, evtMsg, err)
		return
	}

	if err := p.process(ctx, event); err != nil {
		log.Error("processing failed", slog.String("error", err.Error()))
		span.RecordError(err)
		p.handleFailure(ctx, evtMsg, err)
		return
	}

	if err := p.events.UpdateStatus(ctx, event.ID, app.EventStatusProcessed); err != nil {
		log.Error("set processed status failed", slog.String("error", err.Error()))
	}

	p.metrics.EventsProcessedTotal.WithLabelValues(event.Source, "processed").Inc()
	span.SetStatus(codes.Ok, "")
	log.Info("event processed successfully")
}

// process contains the actual business logic for an event.
// ClickHouse write is added in iteration 7 — placeholder here.
func (p *Processor) process(ctx context.Context, event *app.Event) error {
	_, span := processorTracer.Start(ctx, "worker.process")
	defer span.End()

	span.SetAttributes(
		attribute.String("event.source", event.Source),
		attribute.String("event.type", event.EventType),
	)

	// TODO(iter7): write to ClickHouse analytics store here.
	return nil
}

func (p *Processor) handleFailure(ctx context.Context, msg EventMessage, cause error) {
	log := logger.FromContext(ctx, p.log).With(slog.String("event_id", msg.EventID))

	if err := p.events.IncrementRetry(ctx, msg.EventID); err != nil {
		log.Error("increment retry count failed", slog.String("error", err.Error()))
	}

	if msg.Attempt >= p.maxRetries {
		log.Warn("max retries exceeded — routing to DLQ",
			slog.Int("attempt", msg.Attempt),
			slog.Int("max_retries", p.maxRetries),
		)
		p.publishToDLQ(ctx, msg, cause)
		p.metrics.EventsFailedTotal.WithLabelValues("unknown", "max_retries_exceeded").Inc()
		return
	}

	retryMsg := EventMessage{EventID: msg.EventID, Attempt: msg.Attempt + 1}
	value, err := MarshalEventMessage(retryMsg)
	if err != nil {
		log.Error("marshal retry message failed", slog.String("error", err.Error()))
		return
	}

	if err := p.producer.Publish(ctx, p.topics.Events, msg.EventID, value); err != nil {
		log.Error("re-publish for retry failed", slog.String("error", err.Error()))
	} else {
		log.Info("event queued for retry",
			slog.Int("next_attempt", retryMsg.Attempt),
		)
	}
}

func (p *Processor) publishToDLQ(ctx context.Context, msg EventMessage, cause error) {
	log := logger.FromContext(ctx, p.log)

	value, err := MarshalEventMessage(msg)
	if err != nil {
		log.Error("marshal DLQ message failed", slog.String("error", err.Error()))
		return
	}

	if err := p.producer.Publish(ctx, p.topics.DLQ, msg.EventID, value); err != nil {
		log.Error("publish to DLQ failed", slog.String("error", err.Error()))
		return
	}

	if err := p.events.UpdateStatus(ctx, msg.EventID, app.EventStatusFailed); err != nil {
		log.Error("set failed status after DLQ publish", slog.String("error", err.Error()))
	}
}

func (p *Processor) reportLag(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lag := p.consumer.Lag()
			p.metrics.QueueLagMessages.WithLabelValues(p.topics.Events).Set(float64(lag))
		}
	}
}
