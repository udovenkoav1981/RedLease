package client

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestLeaseValidUsesWallClockAndValidityBoundary(t *testing.T) {
	clock := &fixedWallClock{time: time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC)}
	client := &Client{now: clock.now, ctx: context.Background()}
	lease := newLease(client, leaseID{}, []byte("key"), 1_000)
	lease.setAcquireValidity(clock.now().Add(time.Second))

	if !lease.Valid() {
		t.Fatal("lease is not valid before validUntil")
	}
	clock.mu.Lock()
	clock.time = lease.ValidUntil()
	clock.mu.Unlock()
	if lease.Valid() {
		t.Fatal("lease is valid at validUntil boundary")
	}
}

func TestLeaseConfirmationCannotBeShortenedByOlderResponse(t *testing.T) {
	clock := &fixedWallClock{time: time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC)}
	client := &Client{now: clock.now, ctx: context.Background()}
	lease := newLease(client, leaseID{}, []byte("key"), 1_000)
	later := clock.now().Add(2 * time.Second)

	lease.markConfirmed(0, later)
	lease.markConfirmed(0, clock.now().Add(time.Second))

	if got := lease.confirmedUntil[0]; !got.Equal(later) {
		t.Fatalf("confirmed until = %v, want %v", got, later)
	}
}

func TestLeaseImmutableGettersAndConcurrentState(t *testing.T) {
	clock := &fixedWallClock{time: time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC)}
	client := &Client{now: clock.now, ctx: context.Background()}
	key := []byte("key")
	lease := newLease(client, leaseID{clientID: 1, bootID: 2, sequence: 3}, key, 1_000)
	lease.setAcquireValidity(clock.now().Add(time.Second))
	key[0] = 'X'

	const iterations = 1_000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for iteration := range iterations {
			lease.markConfirmed(iteration%ServerCount, clock.now().Add(time.Second))
		}
	}()
	for range iterations {
		_ = lease.ID()
		_ = lease.ValidUntil()
		_ = lease.Valid()
		if !bytes.Equal(lease.Key(), []byte("key")) {
			t.Fatal("lease key mutated")
		}
	}
	<-done
}
