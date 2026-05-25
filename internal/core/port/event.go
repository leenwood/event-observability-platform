package port

import (
	"context"

	"github.com/leenwood/event-observability-platform/internal/core/domain"
)

type EventRepository interface {
	Insert(ctx context.Context, event *domain.Event) error
	FindByID(ctx context.Context, id string) (*domain.Event, error)
	UpdateStatus(ctx context.Context, id string, status domain.EventStatus) error
	IncrementRetry(ctx context.Context, id string) error
	ListByStatus(ctx context.Context, status domain.EventStatus, limit int) ([]*domain.Event, error)
}
