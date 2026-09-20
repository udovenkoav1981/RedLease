package client

import (
	"context"
	"testing"
	"time"
)

func TestLeaseValidityUsesMonotonicTimeBoundary(t *testing.T) {
	client := testLeaseClient()
	lease := newLease(client, 0, uint64(1), 1_000)
	lease.setAcquireValidity(time.Now().Add(time.Second))

	if lease.RemainingTTLms() == 0 {
		t.Fatal("lease is not valid before validUntil")
	}
	remaining := lease.RemainingTTLms()
	if remaining == 0 || remaining > 1_000 {
		t.Fatalf("remaining TTL = %d, want 1..1000", remaining)
	}
	lease.setAcquireValidity(time.Now())
	if lease.RemainingTTLms() != 0 {
		t.Fatal("lease is valid at validUntil boundary")
	}
}

func TestLeaseConfirmationCannotBeShortenedByOlderResponse(t *testing.T) {
	client := testLeaseClient()
	lease := newLease(client, 0, uint64(1), 1_000)
	now := time.Now()
	later := now.Add(2 * time.Second)

	lease.markConfirmed(0, later)
	lease.markConfirmed(0, now.Add(time.Second))

	if got := lease.confirmedUntil[0]; !got.Equal(later) {
		t.Fatalf("confirmed until = %v, want %v", got, later)
	}
}

func TestLeaseGetterAndConcurrentState(t *testing.T) {
	client := testLeaseClient()
	key := uint64(1)
	lease := newLease(client, 3, key, 1_000)
	lease.setAcquireValidity(time.Now().Add(time.Second))

	const iterations = 1_000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for iteration := range iterations {
			lease.markConfirmed(iteration%testServerCount, time.Now().Add(time.Second))
		}
	}()
	for range iterations {
		_ = lease.RemainingTTLms()
		if lease.Key() != key {
			t.Fatalf("lease key = %d, want %d", lease.Key(), key)
		}
	}
	<-done
}

func testLeaseClient() *Client {
	return &Client{
		quorum:   testQuorum,
		replicas: make([]*replicaConn, testServerCount),
		logger:   testLogger,
		ctx:      context.Background(),
	}
}
