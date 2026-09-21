package client1of1

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

type observedRequest struct {
	RequestID      uint64
	Operation      protocol.Operation
	Key            uint64
	ClientID       uint32
	BootID         uint32
	LeaseSequence  uint64
	RequestedTTLMS uint64
}

type fakeLeaseConnection struct {
	ctx       context.Context //nolint:containedctx // Test connection owns this context.
	requests  chan observedRequest
	responses chan protocol.Response
	sendStart chan struct{}
	flushes   chan struct{}
}

func (c *fakeLeaseConnection) BufferClientRequest(request *redleasev1.ClientRequest) error {
	decoded, err := observeClientRequest(request)
	if err != nil {
		return err
	}
	if c.sendStart != nil {
		select {
		case c.sendStart <- struct{}{}:
		default:
		}
	}
	select {
	case c.requests <- decoded:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

func observeClientRequest(request *redleasev1.ClientRequest) (observedRequest, error) {
	observed := observedRequest{
		RequestID: request.RequestId(),
		Operation: request.Operation(),
	}
	switch observed.Operation {
	case protocol.OperationAcquire:
		var acquire redleasev1.AcquireRequest
		if request.Acquire(&acquire) == nil {
			return observedRequest{}, errors.New("Acquire payload is missing")
		}
		observed.Key = acquire.Key()
		observed.ClientID = acquire.ClientId()
		observed.BootID = acquire.BootId()
		observed.LeaseSequence = acquire.LeaseSeq()
		observed.RequestedTTLMS = acquire.RequestedTtlMs()
	case protocol.OperationRenew:
		var renew redleasev1.RenewRequest
		if request.Renew(&renew) == nil {
			return observedRequest{}, errors.New("Renew payload is missing")
		}
		observed.Key = renew.Key()
		observed.ClientID = renew.ClientId()
		observed.BootID = renew.BootId()
		observed.LeaseSequence = renew.LeaseSeq()
		observed.RequestedTTLMS = renew.RequestedTtlMs()
	case protocol.OperationRelease:
		var release redleasev1.ReleaseRequest
		if request.Release(&release) == nil {
			return observedRequest{}, errors.New("Release payload is missing")
		}
		observed.Key = release.Key()
		observed.ClientID = release.ClientId()
		observed.BootID = release.BootId()
		observed.LeaseSequence = release.LeaseSeq()
	case protocol.OperationGetTTL:
	default:
		return observedRequest{}, errors.New("unsupported request operation")
	}
	return observed, nil
}

func (c *fakeLeaseConnection) FlushClientRequests() error {
	if c.flushes != nil {
		c.flushes <- struct{}{}
	}
	return nil
}

func (c *fakeLeaseConnection) Recv() (protocol.Response, error) {
	select {
	case response := <-c.responses:
		return response, nil
	case <-c.ctx.Done():
		return protocol.Response{}, io.EOF
	}
}

func (c *fakeLeaseConnection) Close() error { return nil }

func TestStreamGenerationMultiplexesOutOfOrderResponses(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       streamContext,
		requests:  make(chan observedRequest, 2),
		responses: make(chan protocol.Response, 2),
	}
	generation := newConnectionGeneration(connection, cancelStream)
	defer generation.Close()
	client := &Client{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := generation.submit(client.newReleaseRequest(1, 0))
	if err != nil {
		t.Fatalf("submit first: %v", err)
	}
	second, err := generation.submit(client.newReleaseRequest(2, 0))
	if err != nil {
		t.Fatalf("submit second: %v", err)
	}
	firstRequest := <-connection.requests
	secondRequest := <-connection.requests
	if firstRequest.RequestID == secondRequest.RequestID {
		t.Fatal("concurrent connection requests have the same request ID")
	}

	connection.responses <- releaseServerResponse(secondRequest.RequestID)
	connection.responses <- releaseServerResponse(firstRequest.RequestID)
	firstResponse, err := first.await(ctx, nil)
	if err != nil {
		t.Fatalf("await first: %v", err)
	}
	secondResponse, err := second.await(ctx, nil)
	if err != nil {
		t.Fatalf("await second: %v", err)
	}
	if firstResponse.RequestID != firstRequest.RequestID ||
		secondResponse.RequestID != secondRequest.RequestID {
		t.Fatalf("responses were correlated incorrectly: first=%d second=%d",
			firstResponse.RequestID, secondResponse.RequestID)
	}
}

func TestStreamGenerationCancellationUnblocksAwait(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       streamContext,
		requests:  make(chan observedRequest, 1),
		responses: make(chan protocol.Response),
	}
	generation := newConnectionGeneration(connection, cancelStream)
	defer generation.Close()
	client := &Client{}

	ctx, cancel := context.WithCancel(context.Background())
	future, err := generation.submit(client.newReleaseRequest(1, 0))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	cancel()
	if _, err := future.await(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
}

func TestConnectionGenerationFlushesAvailableRequestsAsOneBatch(t *testing.T) {
	connectionContext, cancelConnection := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       connectionContext,
		requests:  make(chan observedRequest),
		responses: make(chan protocol.Response),
		sendStart: make(chan struct{}, 1),
		flushes:   make(chan struct{}, 1),
	}
	generation := newConnectionGeneration(connection, cancelConnection)
	defer generation.Close()
	client := &Client{}

	if err := generation.submitNoResponse(client.newReleaseRequest(1, 1)); err != nil {
		t.Fatalf("submit first request: %v", err)
	}
	select {
	case <-connection.sendStart:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	if err := generation.submitNoResponse(client.newReleaseRequest(2, 2)); err != nil {
		t.Fatalf("submit second request: %v", err)
	}
	if err := generation.submitNoResponse(client.newReleaseRequest(3, 3)); err != nil {
		t.Fatalf("submit third request: %v", err)
	}

	for wantKey := uint64(1); wantKey <= 3; wantKey++ {
		select {
		case request := <-connection.requests:
			if request.Key != wantKey {
				t.Fatalf("request key = %d, want %d", request.Key, wantKey)
			}
		case <-time.After(time.Second):
			t.Fatalf("request %d was not buffered", wantKey)
		}
	}
	select {
	case <-connection.flushes:
	case <-time.After(time.Second):
		t.Fatal("available request batch was not flushed")
	}
	select {
	case <-connection.flushes:
		t.Fatal("available requests were split into multiple flushes")
	default:
	}
}

func TestConnectionFutureResponseTimeoutUnblocksAwait(t *testing.T) {
	connectionContext, cancelConnection := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       connectionContext,
		requests:  make(chan observedRequest, 1),
		responses: make(chan protocol.Response, 2),
	}
	generation := newConnectionGeneration(connection, cancelConnection)
	defer generation.Close()
	client := &Client{responseTimeout: time.Millisecond}

	future, err := generation.submit(client.newReleaseRequest(1, 0))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	firstRequest := <-connection.requests
	if _, err := client.awaitResponse(context.Background(), future); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await error = %v, want context.DeadlineExceeded", err)
	}

	generation.stateMu.Lock()
	_, pending := generation.pending[future.requestID]
	generation.stateMu.Unlock()
	if pending {
		t.Fatal("timed-out request remains pending")
	}

	client.responseTimeout = time.Second
	second, err := generation.submit(client.newReleaseRequest(2, 0))
	if err != nil {
		t.Fatalf("submit after timeout: %v", err)
	}
	secondRequest := <-connection.requests
	connection.responses <- releaseServerResponse(firstRequest.RequestID)
	connection.responses <- releaseServerResponse(secondRequest.RequestID)
	response, err := client.awaitResponse(context.Background(), second)
	if err != nil {
		t.Fatalf("await after late response: %v", err)
	}
	if response.RequestID != secondRequest.RequestID {
		t.Fatalf("response request ID = %d, want %d", response.RequestID, secondRequest.RequestID)
	}
}

func TestAcquireReturnsNotAcquiredWhenSendQueueIsFull(t *testing.T) {
	connectionContext, cancelConnection := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       connectionContext,
		requests:  make(chan observedRequest),
		responses: make(chan protocol.Response),
		sendStart: make(chan struct{}, 1),
	}
	generation := newConnectionGeneration(connection, cancelConnection)
	defer generation.Close()

	clientContext, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	client := &Client{
		clientID:        7,
		bootID:          1,
		responseTimeout: time.Second,
		logger:          slog.New(slog.DiscardHandler),
		ctx:             clientContext,
		cancel:          cancelClient,
		generation:      generation,
		changed:         make(chan struct{}),
	}

	if err := generation.submitNoResponse(client.newReleaseRequest(1, 1)); err != nil {
		t.Fatalf("submit request blocking writer: %v", err)
	}
	select {
	case <-connection.sendStart:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	for request := range uint64(sendQueueCapacity) {
		if err := generation.submitNoResponse(client.newReleaseRequest(request+2, request+2)); err != nil {
			t.Fatalf("fill send queue at request %d: %v", request, err)
		}
	}

	lease, err := client.Acquire(context.Background(), 1000, 1000)
	if lease != nil {
		t.Fatal("Acquire with full send queue returned a lease")
	}
	if !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("Acquire error = %v, want ErrNotAcquired", err)
	}
	if !errors.Is(err, errSendQueueFull) {
		t.Fatalf("Acquire cause = %v, want errSendQueueFull", err)
	}
	select {
	case <-generation.done:
		t.Fatal("full send queue terminated the connection")
	default:
	}
	if generation.err() != nil {
		t.Fatalf("connection error after full send queue = %v", generation.err())
	}
}

func TestFailedAcquireQueuesCleanupRelease(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       streamContext,
		requests:  make(chan observedRequest, 2),
		responses: make(chan protocol.Response, 2),
	}
	generation := newConnectionGeneration(connection, cancelStream)
	defer generation.Close()

	clientContext, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	client := &Client{
		clientID:        7,
		bootID:          1,
		responseTimeout: 10 * time.Millisecond,
		logger:          slog.New(slog.DiscardHandler),
		ctx:             clientContext,
		cancel:          cancelClient,
		generation:      generation,
		changed:         make(chan struct{}),
	}

	acquireSent := make(chan observedRequest, 1)
	go func() {
		request := <-connection.requests
		acquireSent <- request
		connection.responses <- protocol.Response{
			RequestID: request.RequestID,
			Operation: protocol.OperationAcquire,
			Status:    protocol.StatusBusy,
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	lease, err := client.Acquire(ctx, 1, 1000)
	cancel()
	if !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("Acquire error = %v, want ErrNotAcquired", err)
	}
	if lease != nil {
		t.Fatal("failed Acquire returned a lease")
	}

	acquireRequest := <-acquireSent
	select {
	case releaseRequest := <-connection.requests:
		if acquireRequest.ClientID != 7 || acquireRequest.BootID != 1 || acquireRequest.LeaseSequence != 1 {
			t.Fatalf("unexpected first Acquire lease ID: %+v", acquireRequest)
		}
		if releaseRequest.Operation != protocol.OperationRelease {
			t.Fatal("cleanup request is not Release")
		}
		if acquireRequest.ClientID != releaseRequest.ClientID ||
			acquireRequest.BootID != releaseRequest.BootID ||
			acquireRequest.LeaseSequence != releaseRequest.LeaseSequence {
			t.Fatal("cleanup Release used a different lease ID")
		}

		generation.stateMu.Lock()
		_, waitsForResponse := generation.pending[releaseRequest.RequestID]
		generation.stateMu.Unlock()
		if waitsForResponse {
			t.Fatal("cleanup Release waits for a server response")
		}
	case <-time.After(time.Second):
		t.Fatal("failed Acquire did not send cleanup Release")
	}

	select {
	case request := <-connection.requests:
		t.Fatalf("failed Acquire retried cleanup Release: %+v", request)
	case <-time.After(5 * client.responseTimeout):
	}
}

func releaseServerResponse(requestID uint64) protocol.Response {
	return protocol.Response{
		RequestID: requestID,
		Operation: protocol.OperationRelease,
		Status:    protocol.StatusOK,
	}
}
