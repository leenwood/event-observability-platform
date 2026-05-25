package dto

import "time"

// Entry is the record stored in the idempotency store.
type Entry struct {
	EventID   string
	Response  []byte
	ExpiresAt time.Time
}

func (e *Entry) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}
