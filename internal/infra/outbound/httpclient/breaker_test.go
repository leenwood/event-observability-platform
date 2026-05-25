package httpclient

import (
	"errors"
	"testing"
	"time"
)

func testBreaker() *CircuitBreaker {
	cfg := BreakerConfig{MaxFailures: 3, OpenTimeout: 100 * time.Millisecond, HalfOpenProbes: 2}
	return NewCircuitBreaker(cfg, nil)
}

func TestBreaker_InitiallyClosed(t *testing.T) {
	cb := testBreaker()
	if err := cb.Allow(); err != nil {
		t.Errorf("expected closed breaker to allow, got %v", err)
	}
}

func TestBreaker_OpensAfterMaxFailures(t *testing.T) {
	cb := testBreaker()

	for i := 0; i < 3; i++ {
		_ = cb.Allow()
		cb.RecordFailure()
	}

	if err := cb.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
	if cb.State() != "open" {
		t.Errorf("expected state=open, got %q", cb.State())
	}
}

func TestBreaker_HalfOpenAfterTimeout(t *testing.T) {
	cb := testBreaker()

	for i := 0; i < 3; i++ {
		_ = cb.Allow()
		cb.RecordFailure()
	}

	time.Sleep(150 * time.Millisecond)

	if err := cb.Allow(); err != nil {
		t.Errorf("expected half-open to allow, got %v", err)
	}
	if cb.State() != "half-open" {
		t.Errorf("expected state=half-open, got %q", cb.State())
	}
}

func TestBreaker_ClosesAfterHalfOpenProbes(t *testing.T) {
	cb := testBreaker()

	for i := 0; i < 3; i++ {
		_ = cb.Allow()
		cb.RecordFailure()
	}

	time.Sleep(150 * time.Millisecond)

	for i := 0; i < 2; i++ {
		if err := cb.Allow(); err != nil {
			t.Fatalf("probe %d: unexpected error %v", i, err)
		}
		cb.RecordSuccess()
	}

	if cb.State() != "closed" {
		t.Errorf("expected state=closed after probes, got %q", cb.State())
	}
	if err := cb.Allow(); err != nil {
		t.Errorf("closed breaker should allow, got %v", err)
	}
}

func TestBreaker_ReopensOnHalfOpenFailure(t *testing.T) {
	cb := testBreaker()

	for i := 0; i < 3; i++ {
		_ = cb.Allow()
		cb.RecordFailure()
	}

	time.Sleep(150 * time.Millisecond)
	_ = cb.Allow()
	cb.RecordFailure()

	if cb.State() != "open" {
		t.Errorf("expected breaker to reopen on half-open failure, got %q", cb.State())
	}
}

func TestBreaker_OnStateChangeCallback(t *testing.T) {
	var transitions []string
	cfg := BreakerConfig{MaxFailures: 2, OpenTimeout: 50 * time.Millisecond, HalfOpenProbes: 1}
	cb := NewCircuitBreaker(cfg, func(from, to string) {
		transitions = append(transitions, from+"→"+to)
	})

	_ = cb.Allow()
	cb.RecordFailure()
	_ = cb.Allow()
	cb.RecordFailure()

	time.Sleep(70 * time.Millisecond)
	_ = cb.Allow()
	cb.RecordSuccess()

	want := []string{"closed→open", "open→half-open", "half-open→closed"}
	if len(transitions) != len(want) {
		t.Fatalf("expected transitions %v, got %v", want, transitions)
	}
	for i, tr := range transitions {
		if tr != want[i] {
			t.Errorf("transition[%d]: got %q, want %q", i, tr, want[i])
		}
	}
}
