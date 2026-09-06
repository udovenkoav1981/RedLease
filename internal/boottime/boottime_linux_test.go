//go:build linux

package boottime

import (
	"math"
	"testing"
)

func TestNowIsNondecreasing(t *testing.T) {
	first := Now()
	second := Now()
	if second < first {
		t.Fatalf("CLOCK_BOOTTIME moved backwards from %d to %d", first, second)
	}
}

func TestAddAndRemaining(t *testing.T) {
	if got := Add(1_000, 500); got != 1_500 {
		t.Fatalf("Add = %d, want 1500", got)
	}
	if got := Add(math.MaxUint64-10, 20); got != math.MaxUint64 {
		t.Fatalf("overflowing Add = %d, want %d", got, uint64(math.MaxUint64))
	}
	if got := Remaining(1_500, 1_000); got != 500 {
		t.Fatalf("Remaining = %d, want 500", got)
	}
	if got := Remaining(1_000, 1_000); got != 0 {
		t.Fatalf("Remaining at deadline = %d, want 0", got)
	}
}
