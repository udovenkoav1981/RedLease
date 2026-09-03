package client

import "time"

// Milliseconds is the wire representation of lease TTL values.
type Milliseconds uint64

func wallNow() time.Time {
	return time.Now().Round(0)
}
