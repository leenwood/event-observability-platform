package service

import (
	"errors"
	"time"
)

var ErrDuplicate = errors.New("idempotency: duplicate request")

// TTL returns the expiry timestamp for a new idempotency entry.
func TTL(d time.Duration) time.Time {
	return time.Now().Add(d)
}
