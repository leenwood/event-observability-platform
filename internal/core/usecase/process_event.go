package usecase

import (
	"context"
	"errors"
	"log/slog"

	"github.com/leenwood/event-observability-platform/internal/core/domain"
	"github.com/leenwood/event-observability-platform/internal/core/dto"
	"github.com/leenwood/event-observability-platform/internal/core/port"
)

type ProcessAction int

const (
	ProcessActionDone  ProcessAction = iota
	ProcessActionRetry               // re-enqueue with attempt+1
	ProcessActionDLQ                 // max retries exceeded, route to dead letter queue
)

type ProcessEventResult struct {
	Action      ProcessAction
	EventSource string // populated when Action == ProcessActionDone and event was processed
}

// ProcessEvent contains the business logic for consuming a single event message.
type ProcessEvent struct {
	events     port.EventRepository
	analytics  port.AnalyticsWriter
	maxRetries int
	log        *slog.Logger
}

func NewProcessEvent(
	events port.EventRepository,
	analytics port.AnalyticsWriter,
	maxRetries int,
	log *slog.Logger,
) *ProcessEvent {
	return &ProcessEvent{events: events, analytics: analytics, maxRetries: maxRetries, log: log}
}

func (uc *ProcessEvent) Execute(ctx context.Context, msg dto.EventMessage) (ProcessEventResult, error) {
	event, err := uc.events.FindByID(ctx, msg.EventID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return ProcessEventResult{Action: ProcessActionDone}, nil
		}
		return uc.handleFailure(ctx, msg, err)
	}

	if event.Status == domain.EventStatusProcessed || event.Status == domain.EventStatusFailed {
		return ProcessEventResult{Action: ProcessActionDone}, nil
	}

	if err := uc.events.UpdateStatus(ctx, event.ID, domain.EventStatusProcessing); err != nil {
		return uc.handleFailure(ctx, msg, err)
	}

	if uc.analytics != nil {
		if err := uc.analytics.WriteEvent(ctx, event); err != nil {
			// Analytics write failure is non-fatal: PostgreSQL is the source
			// of truth; ClickHouse can be backfilled from it if needed.
			uc.log.WarnContext(ctx, "analytics write failed",
				slog.String("error", err.Error()),
				slog.String("event_id", event.ID),
			)
		}
	}

	if err := uc.events.UpdateStatus(ctx, event.ID, domain.EventStatusProcessed); err != nil {
		uc.log.ErrorContext(ctx, "set processed status failed", slog.String("error", err.Error()))
	}

	return ProcessEventResult{Action: ProcessActionDone, EventSource: event.Source}, nil
}

func (uc *ProcessEvent) handleFailure(ctx context.Context, msg dto.EventMessage, cause error) (ProcessEventResult, error) {
	if err := uc.events.IncrementRetry(ctx, msg.EventID); err != nil {
		uc.log.ErrorContext(ctx, "increment retry count failed", slog.String("error", err.Error()))
	}

	if msg.Attempt >= uc.maxRetries {
		if err := uc.events.UpdateStatus(ctx, msg.EventID, domain.EventStatusFailed); err != nil {
			uc.log.ErrorContext(ctx, "set failed status", slog.String("error", err.Error()))
		}
		return ProcessEventResult{Action: ProcessActionDLQ}, cause
	}

	return ProcessEventResult{Action: ProcessActionRetry}, cause
}
