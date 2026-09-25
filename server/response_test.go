package server

import (
	"context"
	"net"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

func TestOutboundResponseRoundTrip(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	tests := []protocol.Response{
		{RequestID: 1, Operation: redleasev1.ClientOperationACQUIRE, Status: redleasev1.LeaseStatusALREADY_OWNED, TTLMS: 700},
		{RequestID: 2, Operation: redleasev1.ClientOperationRENEW, Status: redleasev1.LeaseStatusSTALE, TTLMS: 500},
		{RequestID: 3, Operation: redleasev1.ClientOperationRELEASE, Status: redleasev1.LeaseStatusOK},
		{RequestID: 4, Operation: redleasev1.ClientOperationGET_TTL, Status: redleasev1.LeaseStatusOK, TTLMS: 1_000},
	}
	for _, want := range tests {
		outbound, err := s.newOutboundResponse(want)
		if err != nil {
			t.Fatalf("encode %+v: %v", want, err)
		}
		got, err := transport.DecodeResponse(outbound.message.Table().Bytes)
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
		{Operation: redleasev1.ClientOperationACQUIRE, Status: redleasev1.LeaseStatusKEY_LIMIT_REACHED + 1},
		{Operation: redleasev1.ClientOperationGET_TTL + 1, Status: redleasev1.LeaseStatusOK},
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
	}
	writer := transport.NewFrameWriter(serverConn)
	for _, want := range []protocol.Response{
		{RequestID: 10, Operation: redleasev1.ClientOperationACQUIRE, Status: redleasev1.LeaseStatusOK, TTLMS: 500},
		{RequestID: 11, Operation: redleasev1.ClientOperationRELEASE, Status: redleasev1.LeaseStatusOK},
	} {
		outbound, err := s.newOutboundResponse(want)
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		if err := session.bufferResponse(writer, outbound); err != nil {
			t.Fatalf("buffer response: %v", err)
		}
	}
	flushDone := make(chan error, 1)
	go func() { flushDone <- writer.Flush() }()
	connection := transport.NewClientConnection(clientConn)
	for _, want := range []protocol.Response{
		{RequestID: 10, Operation: redleasev1.ClientOperationACQUIRE, Status: redleasev1.LeaseStatusOK, TTLMS: 500},
		{RequestID: 11, Operation: redleasev1.ClientOperationRELEASE, Status: redleasev1.LeaseStatusOK},
	} {
		frame, err := connection.Reader.ReadFrame()
		if err != nil {
			t.Fatalf("read response frame: %v", err)
		}
		got, err := transport.DecodeResponse(frame)
		if err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if got != want {
			t.Fatalf("response = %+v, want %+v", got, want)
		}
	}
	if err := <-flushDone; err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestEnqueueResponseWaitsForQueueSpace(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	session := &connectionSession{
		server:    s,
		ctx:       t.Context(),
		respQueue: mpscring.NewNotifying[*outboundResponse](2),
	}
	for requestID := uint64(1); requestID <= 2; requestID++ {
		if !session.enqueueResponse(releaseResponse(requestID, redleasev1.LeaseStatusOK)) {
			t.Fatalf("enqueue response %d", requestID)
		}
	}

	done := make(chan bool, 1)
	go func() {
		done <- session.enqueueResponse(releaseResponse(3, redleasev1.LeaseStatusOK))
	}()
	select {
	case <-done:
		t.Fatal("enqueue completed while response queue was full")
	case <-time.After(10 * time.Millisecond):
	}

	first, ok := session.respQueue.TryDequeue()
	if !ok {
		t.Fatal("dequeue first response")
	}
	s.recycleOutboundResponse(first)
	select {
	case enqueued := <-done:
		if !enqueued {
			t.Fatal("enqueue stopped after queue space became available")
		}
	case <-time.After(time.Second):
		t.Fatal("enqueue did not observe available queue space")
	}

	for {
		response, ok := session.respQueue.TryDequeue()
		if !ok {
			break
		}
		s.recycleOutboundResponse(response)
	}
}

func TestDiscardResponsesDrainsQueue(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	session := &connectionSession{
		server:        s,
		respQueue:     mpscring.NewNotifying[*outboundResponse](responseQueueCapacity),
		responsesDone: make(chan struct{}),
	}
	for requestID := uint64(1); requestID <= 2; requestID++ {
		outbound, err := s.newOutboundResponse(protocol.Response{
			RequestID: requestID,
			Operation: redleasev1.ClientOperationRELEASE,
			Status:    redleasev1.LeaseStatusOK,
		})
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		if !session.respQueue.TryEnqueue(outbound) {
			t.Fatal("enqueue response failed")
		}
	}
	close(session.responsesDone)
	session.discardResponses()
	if got := session.respQueue.Len(); got != 0 {
		t.Fatalf("queued responses after discard = %d, want 0", got)
	}
}
