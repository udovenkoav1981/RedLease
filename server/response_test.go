package server

import (
	"context"
	"net"
	"testing"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

func TestOutboundResponseRoundTrip(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	tests := []protocol.Response{
		{RequestID: 1, Operation: protocol.OperationAcquire, Status: protocol.StatusAlreadyOwned, TTLMS: 700},
		{RequestID: 2, Operation: protocol.OperationRenew, Status: protocol.StatusStale, TTLMS: 500},
		{RequestID: 3, Operation: protocol.OperationRelease, Status: protocol.StatusOK},
		{RequestID: 4, Operation: protocol.OperationGetTTL, Status: protocol.StatusOK, TTLMS: 1_000},
	}
	for _, want := range tests {
		outbound, err := s.newOutboundResponse(want)
		if err != nil {
			t.Fatalf("encode %+v: %v", want, err)
		}
		got, err := protocol.DecodeResponse(outbound.message.Table().Bytes)
		s.recycleOutboundResponse(outbound)
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("response = %+v, want %+v", got, want)
		}
	}
}

func TestOutboundResponseRejectsInvalidValues(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	for _, response := range []protocol.Response{
		{Operation: protocol.OperationAcquire, Status: protocol.StatusKeyLimitReached + 1},
		{Operation: protocol.OperationGetTTL + 1, Status: protocol.StatusOK},
	} {
		if outbound, err := s.newOutboundResponse(response); err == nil {
			s.recycleOutboundResponse(outbound)
			t.Fatalf("accepted invalid response %+v", response)
		}
	}
}

func TestOutboundResponseIsCopiedBeforeRecycling(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	activateServer(t, s)
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	session := &connectionSession{
		server: s,
		ctx:    context.Background(),
		slots:  make(chan struct{}, 1),
	}
	writer := transport.NewFrameWriter(serverConn)
	for _, want := range []protocol.Response{
		{RequestID: 10, Operation: protocol.OperationAcquire, Status: protocol.StatusOK, TTLMS: 500},
		{RequestID: 11, Operation: protocol.OperationRelease, Status: protocol.StatusOK},
	} {
		outbound, err := s.newOutboundResponse(want)
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		session.slots <- struct{}{}
		if err := session.bufferResponse(writer, outbound); err != nil {
			t.Fatalf("buffer response: %v", err)
		}
	}
	flushDone := make(chan error, 1)
	go func() { flushDone <- writer.Flush() }()
	connection := transport.NewConnection(clientConn)
	for _, want := range []protocol.Response{
		{RequestID: 10, Operation: protocol.OperationAcquire, Status: protocol.StatusOK, TTLMS: 500},
		{RequestID: 11, Operation: protocol.OperationRelease, Status: protocol.StatusOK},
	} {
		got, err := connection.Recv()
		if err != nil {
			t.Fatalf("receive response: %v", err)
		}
		if got != want {
			t.Fatalf("response = %+v, want %+v", got, want)
		}
	}
	if err := <-flushDone; err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestDiscardResponsesReleasesSlots(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	session := &connectionSession{
		server:    s,
		responses: make(chan *outboundResponse, 2),
		slots:     make(chan struct{}, 2),
	}
	for requestID := uint64(1); requestID <= 2; requestID++ {
		outbound, err := s.newOutboundResponse(protocol.Response{
			RequestID: requestID,
			Operation: protocol.OperationRelease,
			Status:    protocol.StatusOK,
		})
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		session.slots <- struct{}{}
		session.responses <- outbound
	}
	close(session.responses)
	session.discardResponses()
	if got := len(session.slots); got != 0 {
		t.Fatalf("reserved slots after discard = %d, want 0", got)
	}
}
