package kafka

import (
	"context"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var producerTracer = otel.Tracer("integrations/kafka/producer")

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		AllowAutoTopicCreation: false,
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
	}
	return &Producer{writer: w}
}

func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	ctx, span := producerTracer.Start(ctx, "kafka.Publish",
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()

	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", topic),
		attribute.String("messaging.message.id", key),
	)

	carrier := NewHeaderCarrier(nil)
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	msg := kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   value,
		Headers: carrier.Headers(),
		Time:    time.Now().UTC(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("kafka publish to %s: %w", topic, err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
