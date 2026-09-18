//go:build linux

package boottime

import "testing"

func TestNowIsNondecreasing(t *testing.T) {
	first := Now()
	second := Now()
	if second < first {
		t.Fatalf("CLOCK_BOOTTIME moved backwards from %d to %d", first, second)
	}
}

func TestRemaining(t *testing.T) {
	if got := Remaining(1_500, 1_000); got != 500 {
		t.Fatalf("Remaining = %d, want 500", got)
	}
	if got := Remaining(1_000, 1_000); got != 0 {
		t.Fatalf("Remaining at deadline = %d, want 0", got)
	}
}
