package client

import (
	"testing"
	"time"
)

func TestExponentialBackoffBounds(t *testing.T) {
	backoff := exponentialBackoff{
		initial: 100 * time.Millisecond,
		maximum: 500 * time.Millisecond,
	}

	tests := []struct {
		attempt uint
		want    time.Duration
	}{
		{attempt: 0, want: 100 * time.Millisecond},
		{attempt: 1, want: 200 * time.Millisecond},
		{attempt: 2, want: 400 * time.Millisecond},
		{attempt: 3, want: 500 * time.Millisecond},
		{attempt: 100, want: 500 * time.Millisecond},
	}

	for _, test := range tests {
		if got := backoff.duration(test.attempt); got != test.want {
			t.Errorf("attempt %d: duration = %v, want %v", test.attempt, got, test.want)
		}
	}
}

func TestExponentialBackoffJitterBounds(t *testing.T) {
	backoff := exponentialBackoff{
		initial: 100 * time.Millisecond,
		maximum: time.Second,
		jitter:  0.25,
	}

	for sample := range 1_000 {
		got := backoff.duration(0)
		if got < 75*time.Millisecond || got > 125*time.Millisecond {
			t.Fatalf("sample %d: duration = %v, want [75ms, 125ms]", sample, got)
		}
	}
}

func TestExponentialBackoffJitterDoesNotExceedMaximum(t *testing.T) {
	backoff := exponentialBackoff{
		initial: 100 * time.Millisecond,
		maximum: 250 * time.Millisecond,
		jitter:  0.25,
	}

	for sample := range 1_000 {
		got := backoff.duration(10)
		if got < 187_500*time.Microsecond || got > 250*time.Millisecond {
			t.Fatalf("sample %d: duration = %v, want [187.5ms, 250ms]", sample, got)
		}
	}
}
