package client

import "time"

// Milliseconds is the wire representation of lease TTL values.
type Milliseconds uint64

type wallClock interface {
	now() time.Time
}

type systemWallClock struct{}

func (systemWallClock) now() time.Time {
	return time.Now().Round(0)
}
