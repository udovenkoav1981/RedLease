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

type float64Source interface {
	Float64() float64
}

type exponentialBackoff struct {
	initial time.Duration
	maximum time.Duration
	jitter  float64
	random  float64Source
}

func defaultReconnectBackoff() exponentialBackoff {
	return exponentialBackoff{
		initial: defaultReconnectInitialBackoff,
		maximum: defaultReconnectMaxBackoff,
		jitter:  defaultReconnectJitter,
		random:  rand.New(rand.NewSource(time.Now().UnixNano())),
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

	if b.jitter <= 0 || b.random == nil || base <= 0 {
		return base
	}

	factor := 1 - b.jitter + 2*b.jitter*b.random.Float64()
	delay := time.Duration(float64(base) * factor)
	if delay < 0 {
		return 0
	}
	if delay > b.maximum {
		return b.maximum
	}
	return delay
}

type backoffTimer interface {
	wait(context.Context, time.Duration) bool
}

type systemBackoffTimer struct{}

func (systemBackoffTimer) wait(ctx context.Context, delay time.Duration) bool {
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
