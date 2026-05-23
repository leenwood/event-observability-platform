package httpclient

import (
	"net/http"
	"testing"
	"time"
)

func TestRetryableStatus(t *testing.T) {
	cases := []struct {
		code      int
		retryable bool
	}{
		{http.StatusOK, false},
		{http.StatusCreated, false},
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusForbidden, false},
		{http.StatusNotFound, false},
		{http.StatusUnprocessableEntity, false},
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true},
		{http.StatusGatewayTimeout, true},
		{599, true},
	}

	for _, tc := range cases {
		got := retryableStatus(tc.code)
		if got != tc.retryable {
			t.Errorf("retryableStatus(%d) = %v, want %v", tc.code, got, tc.retryable)
		}
	}
}

func TestBackoff_StaysWithinBounds(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		d := backoff(attempt)
		if d < 0 {
			t.Errorf("attempt %d: backoff returned negative duration %v", attempt, d)
		}
		if d > maxDelay {
			t.Errorf("attempt %d: backoff %v exceeds maxDelay %v", attempt, d, maxDelay)
		}
	}
}

func TestBackoff_ZeroAttemptIsInstant(t *testing.T) {
	for i := 0; i < 50; i++ {
		d := backoff(0)
		if d > baseDelay {
			t.Errorf("attempt 0: expected delay ≤ baseDelay (%v), got %v", baseDelay, d)
		}
	}
}

func TestBackoff_CapsAtMaxDelay(t *testing.T) {
	for attempt := 10; attempt < 30; attempt++ {
		d := backoff(attempt)
		if d > maxDelay {
			t.Errorf("attempt %d: backoff %v exceeds maxDelay %v", attempt, d, maxDelay)
		}
	}
}

func TestBackoff_GenerallyIncreases(t *testing.T) {
	const samples = 200

	var sumLow, sumHigh time.Duration
	for i := 0; i < samples; i++ {
		sumLow += backoff(0)
		sumHigh += backoff(5)
	}

	avgLow := sumLow / samples
	avgHigh := sumHigh / samples

	if avgHigh <= avgLow {
		t.Errorf("expected higher attempt to have larger average backoff: avg(0)=%v avg(5)=%v",
			avgLow, avgHigh)
	}
}
