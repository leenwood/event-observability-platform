package analytics

import (
	"context"
	"time"

	"github.com/leenwood/event-observability-platform/internal/pkg/domain"
)

// Writer is the interface the processor uses to record processed events into the analytics store.
type Writer interface {
	WriteEvent(ctx context.Context, event *domain.Event) error
}

// DailyEventStat is the aggregate returned by analytics queries.
type DailyEventStat struct {
	Date       time.Time `json:"date"`
	Source     string    `json:"source"`
	EventType  string    `json:"event_type"`
	EventCount uint64    `json:"event_count"`
}
