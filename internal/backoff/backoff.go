// Package backoff provides the retry delay policy shared by RedLease clients.
package backoff

import (
	"context"
	"math/rand"
	"time"
)

const (
	defaultInitial = 50 * time.Millisecond
	defaultMaximum = 2 * time.Second
	defaultJitter  = 0.2
)

// Exponential calculates exponentially increasing delays with optional jitter.
type Exponential struct {
	initial time.Duration
	maximum time.Duration
	jitter  float64
}

// New constructs an exponential backoff policy.
func New(initial, maximum time.Duration, jitter float64) Exponential {
	return Exponential{
		initial: initial,
		maximum: maximum,
		jitter:  jitter,
	}
}

// Default returns the retry policy used for reconnect, healing and Release.
func Default() Exponential {
	return New(defaultInitial, defaultMaximum, defaultJitter)
}

// Duration returns the randomized delay for a zero-based attempt number.
func (b Exponential) Duration(attempt uint) time.Duration {
	base := min(b.initial, b.maximum)
	for range attempt {
		if base >= b.maximum {
			base = b.maximum
			break
		}
		if base > b.maximum/2 {
			base = b.maximum
			break
		}
		base *= 2
	}

	if b.jitter <= 0 || base <= 0 {
		return base
	}

	factor := 1 - b.jitter + 2*b.jitter*rand.Float64()
	delay := time.Duration(float64(base) * factor)
	if delay < 0 {
		return 0
	}
	if delay > b.maximum {
		return b.maximum
	}
	return delay
}

// Wait blocks for delay or until ctx is cancelled.
func Wait(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
