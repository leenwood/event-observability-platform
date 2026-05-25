package port

import (
	"context"
	"time"

	"github.com/leenwood/event-observability-platform/internal/core/domain"
	"github.com/leenwood/event-observability-platform/internal/core/dto"
)

// AnalyticsWriter records processed events into the analytics store.
type AnalyticsWriter interface {
	WriteEvent(ctx context.Context, event *domain.Event) error
}

// AnalyticsQuerier reads aggregated event statistics from the analytics store.
type AnalyticsQuerier interface {
	DailyEvents(ctx context.Context, from, to time.Time) ([]dto.DailyEventStat, error)
}
