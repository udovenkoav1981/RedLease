package server

import (
	"context"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

func TestServerHandlesTenThousandActiveLeases(t *testing.T) {
	const leaseCount = 10_000

	s := newTestServer(t, uint64(ProtocolMaxTTL/time.Millisecond), defaultShardCount)
	activateServer(t, s)
	responses := make(chan protocol.Response, leaseCount)

	submitAcquireBatch(t, s, leaseCount, 1, responses)
	assertAcquireBatchStatus(
		t,
		responses,
		leaseCount,
		protocol.StatusOK,
	)
	if got := s.keys.Load(); got != leaseCount {
		t.Fatalf("resident keys = %d, want %d", got, leaseCount)
	}

	if !s.dispatch(context.Background().Done(), shardJob{
		operation: operation{
			kind:           operationAcquire,
			key:            10001,
			leaseID:        leaseID{clientID: 3, bootID: 3, leaseSeq: 1},
			requestedTTLMS: uint64(ProtocolMaxTTL / time.Millisecond),
		},
		complete: func(response protocol.Response) {
			responses <- response
		},
	}) {
		t.Fatal("dispatch over-limit Acquire")
	}
	if status := (<-responses).Status; status != protocol.StatusKeyLimitReached {
		t.Fatalf("10,001st Acquire = %s, want KEY_LIMIT_REACHED", status)
	}

	// Every key must still be owned simultaneously: a different lease ID is
	// rejected for all ten thousand keys.
	submitAcquireBatch(t, s, leaseCount, 2, responses)
	assertAcquireBatchStatus(
		t,
		responses,
		leaseCount,
		protocol.StatusBusy,
	)
}

func submitAcquireBatch(
	t *testing.T,
	s *Server,
	count int,
	clientID uint32,
	responses chan<- protocol.Response,
) {
	t.Helper()
	ctx := context.Background()
	for sequence := 1; sequence <= count; sequence++ {
		op := operation{
			requestID:      uint64(sequence),
			kind:           operationAcquire,
			key:            uint64(sequence),
			leaseID:        leaseID{clientID: clientID, bootID: clientID, leaseSeq: uint64(sequence)},
			requestedTTLMS: uint64(ProtocolMaxTTL / time.Millisecond),
		}
		if !s.dispatch(ctx.Done(), shardJob{
			operation: op,
			complete: func(response protocol.Response) {
				responses <- response
			},
		}) {
			t.Fatalf("dispatch Acquire %d", sequence)
		}
	}
}

func assertAcquireBatchStatus(
	t *testing.T,
	responses <-chan protocol.Response,
	count int,
	want protocol.Status,
) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for received := range count {
		select {
		case response := <-responses:
			if got := response.Status; got != want {
				t.Fatalf("response %d status = %s, want %s", received, got, want)
			}
		case <-deadline.C:
			t.Fatalf("received %d/%d responses before timeout", received, count)
		}
	}
}
