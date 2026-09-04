package client

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestLeaseValidUsesWallClockAndValidityBoundary(t *testing.T) {
	client := &Client{ctx: context.Background()}
	lease := newLease(client, leaseID{}, []byte("key"), 1_000)
	lease.setAcquireValidity(time.Now().Round(0).Add(time.Second))

	if !lease.Valid() {
		t.Fatal("lease is not valid before validUntil")
	}
	lease.setAcquireValidity(time.Now().Round(0))
	if lease.Valid() {
		t.Fatal("lease is valid at validUntil boundary")
	}
}

func TestLeaseConfirmationCannotBeShortenedByOlderResponse(t *testing.T) {
	client := &Client{ctx: context.Background()}
	lease := newLease(client, leaseID{}, []byte("key"), 1_000)
	now := time.Now().Round(0)
	later := now.Add(2 * time.Second)

	lease.markConfirmed(0, later)
	lease.markConfirmed(0, now.Add(time.Second))

	if got := lease.confirmedUntil[0]; !got.Equal(later) {
		t.Fatalf("confirmed until = %v, want %v", got, later)
	}
}

func TestLeaseImmutableGettersAndConcurrentState(t *testing.T) {
	client := &Client{ctx: context.Background()}
	key := []byte("key")
	lease := newLease(client, leaseID{clientID: 1, bootID: 2, sequence: 3}, key, 1_000)
	lease.setAcquireValidity(time.Now().Round(0).Add(time.Second))
	key[0] = 'X'

	const iterations = 1_000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for iteration := range iterations {
			lease.markConfirmed(iteration%ServerCount, time.Now().Round(0).Add(time.Second))
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
