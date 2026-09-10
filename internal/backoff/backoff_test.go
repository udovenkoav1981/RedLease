package backoff

import (
	"context"
	"testing"
	"time"
)

func TestExponentialBounds(t *testing.T) {
	policy := New(100*time.Millisecond, 500*time.Millisecond, 0)
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
		if got := policy.Duration(test.attempt); got != test.want {
			t.Errorf("attempt %d: Duration = %v, want %v", test.attempt, got, test.want)
		}
	}
}

func TestExponentialJitterBounds(t *testing.T) {
	policy := New(100*time.Millisecond, time.Second, 0.25)
	for sample := range 1_000 {
		got := policy.Duration(0)
		if got < 75*time.Millisecond || got > 125*time.Millisecond {
			t.Fatalf("sample %d: Duration = %v, want [75ms, 125ms]", sample, got)
		}
	}
}

func TestExponentialJitterDoesNotExceedMaximum(t *testing.T) {
	policy := New(100*time.Millisecond, 250*time.Millisecond, 0.25)
	for sample := range 1_000 {
		got := policy.Duration(10)
		if got < 187_500*time.Microsecond || got > 250*time.Millisecond {
			t.Fatalf("sample %d: Duration = %v, want [187.5ms, 250ms]", sample, got)
		}
	}
}

func TestWaitHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Wait(ctx, time.Hour) {
		t.Fatal("Wait returned true for a cancelled context")
	}
}
