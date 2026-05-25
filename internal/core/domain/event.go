package domain

import (
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
