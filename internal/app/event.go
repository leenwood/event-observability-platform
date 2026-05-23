package app

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("not found")

type EventStatus string

const (
	EventStatusPending    EventStatus = "pending"
	EventStatusProcessing EventStatus = "processing"
	EventStatusProcessed  EventStatus = "processed"
	EventStatusFailed     EventStatus = "failed"
)

type Event struct {
	ID             string
	IdempotencyKey string
	Source         string
	EventType      string
	Payload        []byte
	Status         EventStatus
	RetryCount     int
	CreatedAt      time.Time
	ProcessedAt    *time.Time
}

type EventRepository interface {
	Insert(ctx context.Context, event *Event) error
	FindByID(ctx context.Context, id string) (*Event, error)
	UpdateStatus(ctx context.Context, id string, status EventStatus) error
	IncrementRetry(ctx context.Context, id string) error
	ListByStatus(ctx context.Context, status EventStatus, limit int) ([]*Event, error)
}
