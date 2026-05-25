package processor

import (
	"context"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/leenwood/event-observability-platform/internal/core/dto"
	"github.com/leenwood/event-observability-platform/internal/core/mapper"
	"github.com/leenwood/event-observability-platform/internal/core/port"
	"github.com/leenwood/event-observability-platform/internal/core/usecase"
	"github.com/leenwood/event-observability-platform/internal/infra/messaging"
	"github.com/leenwood/event-observability-platform/internal/platform/logger"
	"github.com/leenwood/event-observability-platform/internal/platform/metrics"
)

var processorTracer = otel.Tracer("processor")

type Topics struct {
	Events string
	DLQ    string
}

type Processor struct {
	consumer     *messaging.Consumer
	producer     port.Publisher
	processEvent *usecase.ProcessEvent
	metrics      *metrics.Metrics
	log          *slog.Logger
	topics       Topics
}

func NewProcessor(
	consumer *messaging.Consumer,
	producer port.Publisher,
	processEvent *usecase.ProcessEvent,
	m *metrics.Metrics,
	log *slog.Logger,
	topics Topics,
) *Processor {
	return &Processor{
		consumer:     consumer,
		producer:     producer,
		processEvent: processEvent,
		metrics:      m,
		log:          log,
		topics:       topics,
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
				return ctx.Err()
			}
			p.log.Error("fetch message failed", slog.String("error", err.Error()))
			time.Sleep(time.Second)
			continue
		}

		p.processMessage(ctx, msg)

		if err := p.consumer.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.log.Error("commit message failed", slog.String("error", err.Error()))
		}
	}
}

func (p *Processor) processMessage(ctx context.Context, raw kafkago.Message) {
	carrier := messaging.NewHeaderCarrier(raw.Headers)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	ctx, span := processorTracer.Start(ctx, "processor.processMessage",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	log := logger.FromContext(ctx, p.log)

	evtMsg, err := mapper.UnmarshalEventMessage(raw.Value)
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

	result, err := p.processEvent.Execute(ctx, evtMsg)

	switch result.Action {
	case usecase.ProcessActionDone:
		if err == nil && result.EventSource != "" {
			p.metrics.EventsProcessedTotal.WithLabelValues(result.EventSource, "processed").Inc()
			span.SetStatus(codes.Ok, "")
			log.Info("event processed successfully")
		}

	case usecase.ProcessActionRetry:
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Error("processing failed", slog.String("error", err.Error()))
		p.publishRetry(ctx, evtMsg)

	case usecase.ProcessActionDLQ:
		span.RecordError(err)
		log.Warn("max retries exceeded — routing to DLQ",
			slog.Int("attempt", evtMsg.Attempt),
		)
		p.publishToDLQ(ctx, evtMsg)
		p.metrics.EventsFailedTotal.WithLabelValues("unknown", "max_retries_exceeded").Inc()
	}
}

func (p *Processor) publishRetry(ctx context.Context, msg dto.EventMessage) {
	retryMsg := dto.EventMessage{EventID: msg.EventID, Attempt: msg.Attempt + 1}
	value, err := mapper.MarshalEventMessage(retryMsg)
	if err != nil {
		p.log.ErrorContext(ctx, "marshal retry message failed", slog.String("error", err.Error()))
		return
	}

	if err := p.producer.Publish(ctx, p.topics.Events, retryMsg.EventID, value); err != nil {
		p.log.ErrorContext(ctx, "re-publish for retry failed", slog.String("error", err.Error()))
	} else {
		p.log.InfoContext(ctx, "event queued for retry", slog.Int("next_attempt", retryMsg.Attempt))
	}
}

func (p *Processor) publishToDLQ(ctx context.Context, msg dto.EventMessage) {
	value, err := mapper.MarshalEventMessage(msg)
	if err != nil {
		p.log.ErrorContext(ctx, "marshal DLQ message failed", slog.String("error", err.Error()))
		return
	}

	if err := p.producer.Publish(ctx, p.topics.DLQ, msg.EventID, value); err != nil {
		p.log.ErrorContext(ctx, "publish to DLQ failed", slog.String("error", err.Error()))
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
