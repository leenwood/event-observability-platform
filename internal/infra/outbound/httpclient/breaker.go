package httpclient

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker: circuit is open")

type cbState int

const (
	stateClosed   cbState = iota // normal operation
	stateOpen                    // failing fast
	stateHalfOpen                // probing recovery
)

func (s cbState) String() string {
	switch s {
	case stateClosed:
		return "closed"
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// BreakerConfig tunes the circuit breaker behaviour.
type BreakerConfig struct {
	// MaxFailures is the number of consecutive failures that trip the breaker.
	MaxFailures int
	// OpenTimeout is how long the breaker stays open before probing.
	OpenTimeout time.Duration
	// HalfOpenProbes is how many consecutive successes close the breaker again.
	HalfOpenProbes int
}

func defaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		MaxFailures:    5,
		OpenTimeout:    30 * time.Second,
		HalfOpenProbes: 2,
	}
}

// CircuitBreaker implements the Closed → Open → Half-Open → Closed state machine.
// It is safe for concurrent use.
type CircuitBreaker struct {
	mu          sync.Mutex
	state       cbState
	failures    int
	probes      int
	lastFailure time.Time
	cfg         BreakerConfig
	onChange    func(from, to string)
}

func NewCircuitBreaker(cfg BreakerConfig, onChange func(from, to string)) *CircuitBreaker {
	return &CircuitBreaker{cfg: cfg, onChange: onChange}
}

// Allow returns nil if the request may proceed, or ErrCircuitOpen if not.
func (cb *CircuitBreaker) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateClosed:
		return nil

	case stateOpen:
		if time.Since(cb.lastFailure) >= cb.cfg.OpenTimeout {
			cb.transition(stateHalfOpen)
			return nil
		}
		return ErrCircuitOpen

	case stateHalfOpen:
		return nil
	}

	return nil
}

// RecordSuccess notifies the breaker that the last request succeeded.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateClosed:
		cb.failures = 0

	case stateOpen:
		// A success while open is ignored; the state transitions via Allow().

	case stateHalfOpen:
		cb.probes++
		if cb.probes >= cb.cfg.HalfOpenProbes {
			cb.transition(stateClosed)
		}
	}
}

// RecordFailure notifies the breaker that the last request failed.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailure = time.Now()

	switch cb.state {
	case stateClosed:
		cb.failures++
		if cb.failures >= cb.cfg.MaxFailures {
			cb.transition(stateOpen)
		}

	case stateOpen:
		// lastFailure is already updated above; nothing else to do.

	case stateHalfOpen:
		cb.probes = 0
		cb.transition(stateOpen)
	}
}

// State returns the current state name (for metrics / logging).
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state.String()
}

func (cb *CircuitBreaker) transition(next cbState) {
	if cb.state == next {
		return
	}
	from := cb.state.String()
	cb.state = next
	cb.failures = 0
	cb.probes = 0
	if cb.onChange != nil {
		cb.onChange(from, next.String())
	}
}
