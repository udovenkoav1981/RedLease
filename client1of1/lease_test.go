package client1of1

import (
	"context"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

func TestCandidateValidityRejectsTTLAboveProtocolMaximum(t *testing.T) {
	start := time.Now()
	validUntil := candidateValidUntil(start, protocol.MaxTTLMS+1)
	if !validUntil.Equal(start) {
		t.Fatalf("validUntil = %v, want expired at %v", validUntil, start)
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	client := &Client{
		ctx:       context.Background(),
		sendQueue: mpscring.NewNotifying[*outboundConnectionRequest](),
		pending:   newPendingShards(),
	}
	lease := &Lease{
		client:     client,
		key:        42,
		sequence:   1,
		lifecycle:  leaseActive,
		validUntil: time.Now().Add(time.Second),
	}

	lease.Release()
	lease.Release()
	if got := lease.RemainingTTLms(); got != 0 {
		t.Fatalf("RemainingTTLms after Release = %d, want 0", got)
	}
	if got := client.sendQueue.Len(); got != 1 {
		t.Fatalf("queued Release requests = %d, want 1", got)
	}
	queued, ok := client.sendQueue.TryDequeue()
	if !ok {
		t.Fatal("Release request was not queued")
	}
	client.recycleOutboundRequest(queued)
}
