package dto

import "time"

// DailyEventStat is the aggregate returned by analytics queries.
type DailyEventStat struct {
	Date       time.Time `json:"date"`
	Source     string    `json:"source"`
	EventType  string    `json:"event_type"`
	EventCount uint64    `json:"event_count"`
}
