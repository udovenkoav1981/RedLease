package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func TestServerHandlesTenThousandActiveLeases(t *testing.T) {
	const leaseCount = 10_000

	s := newTestServer(t, uint64(ProtocolMaxTTL/time.Millisecond), defaultShardCount)
	activateServer(t, s)
	responses := make(chan *redleasev1.ServerResponse, leaseCount)

	submitAcquireBatch(t, s, leaseCount, 1, responses)
	assertAcquireBatchStatus(
		t,
		responses,
		leaseCount,
		redleasev1.LeaseStatus_LEASE_STATUS_OK,
	)
	if got := s.keys.Load(); got != leaseCount {
		t.Fatalf("resident keys = %d, want %d", got, leaseCount)
	}

	if !s.dispatch(context.Background().Done(), shardJob{
		operation: operation{
			kind:           operationAcquire,
			key:            "over-limit",
			leaseID:        leaseID{clientID: 3, bootID: 3, leaseSeq: 1},
			requestedTTLMS: uint64(ProtocolMaxTTL / time.Millisecond),
		},
		complete: func(response *redleasev1.ServerResponse) {
			responses <- response
		},
	}) {
		t.Fatal("dispatch over-limit Acquire")
	}
	if status := (<-responses).GetAcquire().GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_KEY_LIMIT_REACHED {
		t.Fatalf("10,001st Acquire = %s, want KEY_LIMIT_REACHED", status)
	}

	// Every key must still be owned simultaneously: a different lease ID is
	// rejected for all ten thousand keys.
	submitAcquireBatch(t, s, leaseCount, 2, responses)
	assertAcquireBatchStatus(
		t,
		responses,
		leaseCount,
		redleasev1.LeaseStatus_LEASE_STATUS_BUSY,
	)
}

func submitAcquireBatch(
	t *testing.T,
	s *Server,
	count int,
	clientID uint32,
	responses chan<- *redleasev1.ServerResponse,
) {
	t.Helper()
	ctx := context.Background()
	for sequence := 1; sequence <= count; sequence++ {
		op := operation{
			requestID:      uint64(sequence),
			kind:           operationAcquire,
			key:            strconv.Itoa(sequence),
			leaseID:        leaseID{clientID: clientID, bootID: clientID, leaseSeq: uint64(sequence)},
			requestedTTLMS: uint64(ProtocolMaxTTL / time.Millisecond),
		}
		if !s.dispatch(ctx.Done(), shardJob{
			operation: op,
			complete: func(response *redleasev1.ServerResponse) {
				responses <- response
			},
		}) {
			t.Fatalf("dispatch Acquire %d", sequence)
		}
	}
}

func assertAcquireBatchStatus(
	t *testing.T,
	responses <-chan *redleasev1.ServerResponse,
	count int,
	want redleasev1.LeaseStatus,
) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for received := range count {
		select {
		case response := <-responses:
			if got := response.GetAcquire().GetStatus(); got != want {
				t.Fatalf("response %d status = %s, want %s", received, got, want)
			}
		case <-deadline.C:
			t.Fatalf("received %d/%d responses before timeout", received, count)
		}
	}
}
