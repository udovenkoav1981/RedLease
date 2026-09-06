package client

import (
	"bytes"
	"context"
	"testing"

	"github.com/udovenkoav1981/RedLease/internal/boottime"
)

func TestLeaseValidityUsesBootTimeBoundary(t *testing.T) {
	client := &Client{ctx: context.Background()}
	lease := newLease(client, leaseID{}, []byte("key"), 1_000)
	lease.setAcquireValidity(boottime.Add(boottime.Now(), 1_000))

	if !lease.Valid() {
		t.Fatal("lease is not valid before validUntil")
	}
	remaining := lease.RemainingTTL()
	if remaining == 0 || remaining > 1_000 {
		t.Fatalf("remaining TTL = %d, want 1..1000", remaining)
	}
	lease.setAcquireValidity(boottime.Now())
	if lease.Valid() {
		t.Fatal("lease is valid at validUntil boundary")
	}
	if remaining := lease.RemainingTTL(); remaining != 0 {
		t.Fatalf("expired lease remaining TTL = %d, want 0", remaining)
	}
}

func TestLeaseConfirmationCannotBeShortenedByOlderResponse(t *testing.T) {
	client := &Client{ctx: context.Background()}
	lease := newLease(client, leaseID{}, []byte("key"), 1_000)
	now := boottime.Now()
	later := boottime.Add(now, 2_000)

	lease.markConfirmed(0, later)
	lease.markConfirmed(0, boottime.Add(now, 1_000))

	if got := lease.confirmedUntil[0]; got != later {
		t.Fatalf("confirmed until = %d, want %d", got, later)
	}
}

func TestLeaseImmutableGettersAndConcurrentState(t *testing.T) {
	client := &Client{ctx: context.Background()}
	key := []byte("key")
	lease := newLease(client, leaseID{clientID: 1, bootID: 2, sequence: 3}, key, 1_000)
	lease.setAcquireValidity(boottime.Add(boottime.Now(), 1_000))
	key[0] = 'X'

	const iterations = 1_000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for iteration := range iterations {
			lease.markConfirmed(iteration%ServerCount, boottime.Add(boottime.Now(), 1_000))
		}
	}()
	for range iterations {
		_ = lease.ID()
		_ = lease.RemainingTTL()
		_ = lease.Valid()
		if !bytes.Equal(lease.Key(), []byte("key")) {
			t.Fatal("lease key mutated")
		}
	}
	<-done
}
