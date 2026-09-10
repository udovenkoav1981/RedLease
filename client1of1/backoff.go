package client1of1

import (
	"context"
	"math/rand"
	"time"
)

const (
	reconnectInitialBackoff = 50 * time.Millisecond
	reconnectMaxBackoff     = 2 * time.Second
	reconnectJitter         = 0.2
)

func reconnectBackoff(attempt uint) time.Duration {
	delay := reconnectInitialBackoff
	for range attempt {
		if delay >= reconnectMaxBackoff/2 {
			delay = reconnectMaxBackoff
			break
		}
		delay *= 2
	}
	factor := 1 - reconnectJitter + 2*reconnectJitter*rand.Float64()
	return min(time.Duration(float64(delay)*factor), reconnectMaxBackoff)
}

func waitBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
