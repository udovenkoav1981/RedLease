package server

import (
	"context"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

func TestServerHandlesTenThousandActiveLeases(t *testing.T) {
	const leaseCount = 10_000

	s := newTestServer(t, uint64(ProtocolMaxTTL/time.Millisecond), defaultShardCount)
	activateServer(t, s)
	session := newOperationTestSession(s, 16_384)

	submitAcquireBatch(t, s, session, leaseCount, 1)
	assertAcquireBatchStatus(
		t,
		s,
		session,
		leaseCount,
		redleasev1.LeaseStatusOK,
	)
	if got := s.keys.Load(); got != leaseCount {
		t.Fatalf("resident keys = %d, want %d", got, leaseCount)
	}

	dispatchTestOperation(t, s, session, operation{
		kind:           operationAcquire,
		key:            10001,
		leaseID:        leaseID{clientID: 3, bootID: 3, leaseSeq: 1},
		requestedTTLMS: uint64(ProtocolMaxTTL / time.Millisecond),
	})
	if status := receiveTestOperationResponse(t, s, session).Status; status != redleasev1.LeaseStatusKEY_LIMIT_REACHED {
		t.Fatalf("10,001st Acquire = %s, want KEY_LIMIT_REACHED", status)
	}

	// Every key must still be owned simultaneously: a different lease ID is
	// rejected for all ten thousand keys.
	submitAcquireBatch(t, s, session, leaseCount, 2)
	assertAcquireBatchStatus(
		t,
		s,
		session,
		leaseCount,
		redleasev1.LeaseStatusBUSY,
	)
}

func submitAcquireBatch(
	t *testing.T,
	s *Server,
	session *connectionSession,
	count int,
	clientID uint32,
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
		session.pending.Add(1)
		op.session = session
		if !s.dispatch(ctx.Done(), op) {
			session.pending.Done()
			t.Fatalf("dispatch Acquire %d", sequence)
		}
	}
}

func assertAcquireBatchStatus(
	t *testing.T,
	s *Server,
	session *connectionSession,
	count int,
	want redleasev1.LeaseStatus,
) {
	t.Helper()
	for received := range count {
		response := receiveTestOperationResponse(t, s, session)
		if got := response.Status; got != want {
			t.Fatalf("response %d status = %s, want %s", received, got, want)
		}
	}
}

func newOperationTestSession(s *Server, capacity int) *connectionSession {
	return &connectionSession{
		server:    s,
		ctx:       context.Background(),
		respQueue: mpscring.NewNotifying[*outboundResponse](capacity),
	}
}

func dispatchTestOperation(t testing.TB, s *Server, session *connectionSession, op operation) {
	t.Helper()
	session.pending.Add(1)
	op.session = session
	if !s.dispatch(context.Background().Done(), op) {
		session.pending.Done()
		t.Fatal("dispatch operation")
	}
}

func receiveTestOperationResponse(
	t testing.TB,
	s *Server,
	session *connectionSession,
) protocol.Response {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		outbound, ok := session.respQueue.TryDequeue()
		if ok {
			response, err := transport.DecodeResponse(outbound.message.Table().Bytes)
			s.recycleOutboundResponse(outbound)
			if err != nil {
				t.Fatalf("decode queued operation response: %v", err)
			}
			return response
		}
		select {
		case <-session.respQueue.Ready():
		case <-timer.C:
			t.Fatal("queued operation response timeout")
		}
	}
}
