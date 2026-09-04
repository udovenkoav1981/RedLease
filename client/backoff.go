package client

import (
	"context"
	"math/rand"
	"time"
)

const (
	defaultReconnectInitialBackoff = 50 * time.Millisecond
	defaultReconnectMaxBackoff     = 2 * time.Second
	defaultReconnectJitter         = 0.2
)

type exponentialBackoff struct {
	initial time.Duration
	maximum time.Duration
	jitter  float64
}

func defaultReconnectBackoff() exponentialBackoff {
	return exponentialBackoff{
		initial: defaultReconnectInitialBackoff,
		maximum: defaultReconnectMaxBackoff,
		jitter:  defaultReconnectJitter,
	}
}

func (b exponentialBackoff) duration(attempt uint) time.Duration {
	base := b.initial
	if base > b.maximum {
		base = b.maximum
	}
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

func waitBackoff(ctx context.Context, delay time.Duration) bool {
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
