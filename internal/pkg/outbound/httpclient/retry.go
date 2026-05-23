package httpclient

import (
	"math"
	"math/rand"
	"net/http"
	"time"
)

const (
	baseDelay = 100 * time.Millisecond
	maxDelay  = 30 * time.Second
)

// retryableStatus returns true for responses that should be retried.
func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests ||
		code == http.StatusBadGateway ||
		code == http.StatusServiceUnavailable ||
		code == http.StatusGatewayTimeout ||
		code >= 500
}

// backoff returns the delay before attempt n (0-indexed) using
// exponential backoff with full jitter: delay = random(0, min(cap, base * 2^n)).
func backoff(attempt int) time.Duration {
	exp := math.Pow(2, float64(attempt))
	ceiling := time.Duration(float64(baseDelay) * exp)
	if ceiling > maxDelay {
		ceiling = maxDelay
	}
	//nolint:gosec // jitter doesn't need crypto randomness
	jitter := time.Duration(rand.Int63n(int64(ceiling) + 1))
	return jitter
}
