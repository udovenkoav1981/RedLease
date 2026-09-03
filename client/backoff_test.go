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

func TestExponentialBackoffDeterministicJitter(t *testing.T) {
	random := &sequenceFloat64{values: []float64{0, 0.5, 1}}
	backoff := exponentialBackoff{
		initial: 100 * time.Millisecond,
		maximum: time.Second,
		jitter:  0.25,
		random:  random,
	}

	for i, want := range []time.Duration{
		75 * time.Millisecond,
		100 * time.Millisecond,
		125 * time.Millisecond,
	} {
		if got := backoff.duration(0); got != want {
			t.Errorf("sample %d: duration = %v, want %v", i, got, want)
		}
	}
}

func TestExponentialBackoffJitterDoesNotExceedMaximum(t *testing.T) {
	backoff := exponentialBackoff{
		initial: 100 * time.Millisecond,
		maximum: 250 * time.Millisecond,
		jitter:  0.25,
		random:  &sequenceFloat64{values: []float64{1}},
	}

	if got := backoff.duration(10); got != 250*time.Millisecond {
		t.Fatalf("duration = %v, want maximum 250ms", got)
	}
}

type sequenceFloat64 struct {
	values []float64
	next   int
}

func (s *sequenceFloat64) Float64() float64 {
	value := s.values[s.next]
	s.next++
	return value
}
