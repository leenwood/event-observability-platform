package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/leenwood/event-observability-platform/internal/core/domain"
	"github.com/leenwood/event-observability-platform/internal/core/dto"
	"github.com/leenwood/event-observability-platform/internal/core/mapper"
	"github.com/leenwood/event-observability-platform/internal/core/port"
)

type IngestEventInput struct {
	IdempotencyKey string
	Source         string
	EventType      string
	Payload        []byte
}

type IngestEventOutput struct {
	EventID string
}

// IngestEvent persists a new event and dispatches it to the processing queue.
type IngestEvent struct {
	events    port.EventRepository
	publisher port.Publisher
	topic     string
	log       *slog.Logger
}

func NewIngestEvent(
	events port.EventRepository,
	publisher port.Publisher,
	topic string,
	log *slog.Logger,
) *IngestEvent {
	return &IngestEvent{events: events, publisher: publisher, topic: topic, log: log}
}

func (uc *IngestEvent) Execute(ctx context.Context, in IngestEventInput) (IngestEventOutput, error) {
	event := &domain.Event{
		ID:             uuid.NewString(),
		IdempotencyKey: in.IdempotencyKey,
		Source:         in.Source,
		EventType:      in.EventType,
		Payload:        in.Payload,
		Status:         domain.EventStatusPending,
		CreatedAt:      time.Now().UTC(),
	}

	if err := uc.events.Insert(ctx, event); err != nil {
		return IngestEventOutput{}, fmt.Errorf("insert event: %w", err)
	}

	if uc.publisher != nil {
		msg := dto.EventMessage{EventID: event.ID, Attempt: 1}
		value, err := mapper.MarshalEventMessage(msg)
		if err == nil {
			if err := uc.publisher.Publish(ctx, uc.topic, event.ID, value); err != nil {
				uc.log.WarnContext(ctx, "failed to publish event to queue",
					slog.String("error", err.Error()),
					slog.String("event_id", event.ID),
				)
			}
		}
	}

	return IngestEventOutput{EventID: event.ID}, nil
}
