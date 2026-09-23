package client1of1

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

type observedRequest struct {
	RequestID      uint64
	Operation      redleasev1.ClientOperation
	Key            uint64
	ClientID       uint32
	BootID         uint32
	LeaseSequence  uint64
	RequestedTTLMS uint64
}

type fakeLeaseConnection struct {
	connection *transport.ClientConnection
	readBuffer []byte
	ctx        context.Context //nolint:containedctx // Test connection owns this context.
	cancel     context.CancelFunc
	requests   chan observedRequest
	responses  chan protocol.Response
	sendStart  chan struct{}
	flushes    chan struct{}
}

func (c *fakeLeaseConnection) transportConnection() *transport.ClientConnection {
	if c.connection == nil {
		c.connection = transport.NewClientConnection(c)
	}
	return c.connection
}

func (c *fakeLeaseConnection) Write(batch []byte) (int, error) {
	if c.sendStart != nil {
		select {
		case c.sendStart <- struct{}{}:
		default:
		}
	}
	if c.flushes != nil {
		c.flushes <- struct{}{}
	}
	for offset := 0; offset < len(batch); {
		if len(batch)-offset < flatbuffers.SizeUint32 {
			return 0, io.ErrUnexpectedEOF
		}
		frameSize := flatbuffers.SizeUint32 + int(binary.LittleEndian.Uint32(batch[offset:]))
		if frameSize > len(batch)-offset {
			return 0, io.ErrUnexpectedEOF
		}
		request := redleasev1.GetSizePrefixedRootAsClientRequest(batch[offset:offset+frameSize], 0)
		decoded, err := observeClientRequest(request)
		if err != nil {
			return 0, err
		}
		select {
		case c.requests <- decoded:
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		}
		offset += frameSize
	}
	return len(batch), nil
}

func (c *fakeLeaseConnection) Read(buffer []byte) (int, error) {
	if len(c.readBuffer) == 0 {
		select {
		case response := <-c.responses:
			c.readBuffer = encodeTestResponse(response)
		case <-c.ctx.Done():
			return 0, io.EOF
		}
	}
	n := copy(buffer, c.readBuffer)
	c.readBuffer = c.readBuffer[n:]
	return n, nil
}

func (c *fakeLeaseConnection) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

func (*fakeLeaseConnection) LocalAddr() net.Addr              { return testAddr("local") }
func (*fakeLeaseConnection) RemoteAddr() net.Addr             { return testAddr("remote") }
func (*fakeLeaseConnection) SetDeadline(time.Time) error      { return nil }
func (*fakeLeaseConnection) SetReadDeadline(time.Time) error  { return nil }
func (*fakeLeaseConnection) SetWriteDeadline(time.Time) error { return nil }

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

func observeClientRequest(request *redleasev1.ClientRequest) (observedRequest, error) {
	observed := observedRequest{
		RequestID: request.RequestId(),
		Operation: request.Operation(),
	}
	switch observed.Operation {
	case redleasev1.ClientOperationACQUIRE:
		var acquire redleasev1.AcquireRequest
		if request.Acquire(&acquire) == nil {
			return observedRequest{}, errors.New("Acquire payload is missing")
		}
		observed.Key = acquire.Key()
		observed.ClientID = acquire.ClientId()
		observed.BootID = acquire.BootId()
		observed.LeaseSequence = acquire.LeaseSeq()
		observed.RequestedTTLMS = acquire.RequestedTtlMs()
	case redleasev1.ClientOperationRENEW:
		var renew redleasev1.RenewRequest
		if request.Renew(&renew) == nil {
			return observedRequest{}, errors.New("Renew payload is missing")
		}
		observed.Key = renew.Key()
		observed.ClientID = renew.ClientId()
		observed.BootID = renew.BootId()
		observed.LeaseSequence = renew.LeaseSeq()
		observed.RequestedTTLMS = renew.RequestedTtlMs()
	case redleasev1.ClientOperationRELEASE:
		var release redleasev1.ReleaseRequest
		if request.Release(&release) == nil {
			return observedRequest{}, errors.New("Release payload is missing")
		}
		observed.Key = release.Key()
		observed.ClientID = release.ClientId()
		observed.BootID = release.BootId()
		observed.LeaseSequence = release.LeaseSeq()
	case redleasev1.ClientOperationGET_TTL:
	default:
		return observedRequest{}, errors.New("unsupported request operation")
	}
	return observed, nil
}

func encodeTestResponse(response protocol.Response) []byte {
	builder := flatbuffers.NewBuilder(transport.InitialBufferSize)
	redleasev1.ServerResponseStart(builder)
	redleasev1.ServerResponseAddRequestId(builder, response.RequestID)
	switch response.Operation {
	case redleasev1.ClientOperationACQUIRE:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultACQUIRE)
		redleasev1.ServerResponseAddAcquire(builder, redleasev1.CreateAcquireResponse(builder, response.Status, response.TTLMS))
	case redleasev1.ClientOperationRELEASE:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultRELEASE)
		redleasev1.ServerResponseAddRelease(builder, redleasev1.CreateReleaseResponse(builder, response.Status))
	default:
		panic("unsupported test response operation")
	}
	root := redleasev1.ServerResponseEnd(builder)
	redleasev1.FinishSizePrefixedServerResponseBuffer(builder, root)
	return builder.FinishedBytes()
}

func startTestConnection(t *testing.T, connection *fakeLeaseConnection, timeout time.Duration) (*Client, <-chan error) {
	t.Helper()
	clientContext, cancelClient := context.WithCancel(context.Background())
	client := &Client{
		clientID:        7,
		bootID:          1,
		responseTimeout: timeout,
		logger:          slog.New(slog.DiscardHandler),
		ctx:             clientContext,
		cancel:          cancelClient,
		connection:      connection.transportConnection(),
		changed:         make(chan struct{}),
		sendQueue:       mpscring.NewNotifying[*outboundConnectionRequest](),
		pending:         newPendingShards(),
	}
	done := make(chan error, 1)
	client.manager.Go(func() {
		done <- client.runConnection(connection.transportConnection())
		close(done)
	})
	t.Cleanup(func() {
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("connection workers did not stop")
		}
	})
	return client, done
}

func testPendingContains(client *Client, requestID uint64) bool {
	shard := client.pending[pendingShardIndex(requestID)]
	shard.mu.Lock()
	_, exists := shard.pending[requestID]
	shard.mu.Unlock()
	return exists
}

func testPendingCount(client *Client) int {
	count := 0
	for _, shard := range &client.pending {
		shard.mu.Lock()
		count += len(shard.pending)
		shard.mu.Unlock()
	}
	return count
}

func TestPendingShardIndex(t *testing.T) {
	for _, test := range []struct {
		requestID uint64
		want      uint8
	}{
		{requestID: 0, want: 0},
		{requestID: 1, want: 1},
		{requestID: 15, want: 15},
		{requestID: 16, want: 0},
		{requestID: 255, want: 15},
		{requestID: 256, want: 0},
		{requestID: 257, want: 1},
		{requestID: ^uint64(0), want: 15},
	} {
		if got := pendingShardIndex(test.requestID); got != test.want {
			t.Errorf("pendingShardIndex(%d) = %d, want %d", test.requestID, got, test.want)
		}
	}
}

func TestConnectionMultiplexesOutOfOrderResponses(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       streamContext,
		requests:  make(chan observedRequest, 2),
		responses: make(chan protocol.Response, 2),
	}
	connection.cancel = cancelStream
	client, _ := startTestConnection(t, connection, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := client.submit(client.newReleaseRequest(1, 0))
	if err != nil {
		t.Fatalf("submit first: %v", err)
	}
	second, err := client.submit(client.newReleaseRequest(2, 0))
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

func TestConnectionCancellationUnblocksAwait(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       streamContext,
		requests:  make(chan observedRequest, 1),
		responses: make(chan protocol.Response),
	}
	connection.cancel = cancelStream
	client, _ := startTestConnection(t, connection, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	future, err := client.submit(client.newReleaseRequest(1, 0))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	cancel()
	if _, err := future.await(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
}

func TestConnectionFlushesAvailableRequestsAsOneBatch(t *testing.T) {
	connectionContext, cancelConnection := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       connectionContext,
		cancel:    cancelConnection,
		requests:  make(chan observedRequest, 3),
		responses: make(chan protocol.Response),
		flushes:   make(chan struct{}, 1),
	}
	clientContext, cancelClient := context.WithCancel(context.Background())
	client := &Client{
		clientID:        7,
		bootID:          1,
		responseTimeout: time.Second,
		logger:          slog.New(slog.DiscardHandler),
		ctx:             clientContext,
		cancel:          cancelClient,
		connection:      connection.transportConnection(),
		changed:         make(chan struct{}),
		sendQueue:       mpscring.NewNotifying[*outboundConnectionRequest](),
		pending:         newPendingShards(),
	}
	for key := uint64(1); key <= 3; key++ {
		if err := client.submitNoResponse(client.newReleaseRequest(key, key)); err != nil {
			t.Fatalf("submit request %d: %v", key, err)
		}
	}
	done := make(chan error, 1)
	client.manager.Go(func() { done <- client.runConnection(connection.transportConnection()) })
	t.Cleanup(func() {
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("connection workers did not stop")
		}
	})

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

func TestConnectionWakesForRequestAfterIdle(t *testing.T) {
	connectionContext, cancelConnection := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       connectionContext,
		requests:  make(chan observedRequest, 2),
		responses: make(chan protocol.Response),
		flushes:   make(chan struct{}, 2),
	}
	connection.cancel = cancelConnection
	client, _ := startTestConnection(t, connection, time.Second)

	for key := uint64(1); key <= 2; key++ {
		if key == 2 {
			time.Sleep(5 * time.Millisecond)
		}
		if err := client.submitNoResponse(client.newReleaseRequest(key, key)); err != nil {
			t.Fatalf("submit request %d: %v", key, err)
		}
		select {
		case request := <-connection.requests:
			if request.Key != key {
				t.Fatalf("request key = %d, want %d", request.Key, key)
			}
		case <-time.After(time.Second):
			t.Fatalf("request %d was not sent", key)
		}
		select {
		case <-connection.flushes:
		case <-time.After(time.Second):
			t.Fatalf("request %d was not flushed", key)
		}
	}
}

func TestConnectionFutureResponseTimeoutUnblocksAwait(t *testing.T) {
	connectionContext, cancelConnection := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       connectionContext,
		requests:  make(chan observedRequest, 1),
		responses: make(chan protocol.Response, 2),
	}
	connection.cancel = cancelConnection
	client, _ := startTestConnection(t, connection, time.Millisecond)

	future, err := client.submit(client.newReleaseRequest(1, 0))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	requestID := future.requestID
	firstRequest := <-connection.requests
	if _, err := client.awaitResponse(context.Background(), future); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await error = %v, want context.DeadlineExceeded", err)
	}

	if testPendingContains(client, requestID) {
		t.Fatal("timed-out request remains pending")
	}

	client.responseTimeout = time.Second
	second, err := client.submit(client.newReleaseRequest(2, 0))
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
	connection.cancel = cancelConnection
	client, done := startTestConnection(t, connection, time.Second)

	if err := client.submitNoResponse(client.newReleaseRequest(1, 1)); err != nil {
		t.Fatalf("submit request blocking writer: %v", err)
	}
	select {
	case <-connection.sendStart:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	for request := range uint64(mpscring.Capacity) {
		if err := client.submitNoResponse(client.newReleaseRequest(request+2, request+2)); err != nil {
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
	if !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("Acquire cause = %v, want ErrSendQueueFull", err)
	}
	select {
	case <-done:
		t.Fatal("full send queue terminated the connection")
	default:
	}
}

func TestFailedAcquireQueuesCleanupRelease(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx:       streamContext,
		requests:  make(chan observedRequest, 2),
		responses: make(chan protocol.Response, 2),
	}
	connection.cancel = cancelStream
	client, _ := startTestConnection(t, connection, 10*time.Millisecond)

	acquireSent := make(chan observedRequest, 1)
	go func() {
		request := <-connection.requests
		acquireSent <- request
		connection.responses <- protocol.Response{
			RequestID: request.RequestID,
			Operation: redleasev1.ClientOperationACQUIRE,
			Status:    redleasev1.LeaseStatusBUSY,
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
		if releaseRequest.Operation != redleasev1.ClientOperationRELEASE {
			t.Fatal("cleanup request is not Release")
		}
		if acquireRequest.ClientID != releaseRequest.ClientID ||
			acquireRequest.BootID != releaseRequest.BootID ||
			acquireRequest.LeaseSequence != releaseRequest.LeaseSequence {
			t.Fatal("cleanup Release used a different lease ID")
		}

		if testPendingContains(client, releaseRequest.RequestID) {
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

func TestReconnectKeepsQueuedRequestAndPendingResponses(t *testing.T) {
	firstContext, cancelFirst := context.WithCancel(context.Background())
	first := &fakeLeaseConnection{
		ctx: firstContext, cancel: cancelFirst,
		requests: make(chan observedRequest), responses: make(chan protocol.Response),
		sendStart: make(chan struct{}, 1),
	}
	client, firstDone := startTestConnection(t, first, time.Second)
	firstFuture, err := client.submit(client.newReleaseRequest(1, 1))
	if err != nil {
		t.Fatalf("submit first: %v", err)
	}
	select {
	case <-first.sendStart:
	case <-time.After(time.Second):
		t.Fatal("first writer did not start")
	}
	secondFuture, err := client.submit(client.newReleaseRequest(2, 2))
	if err != nil {
		t.Fatalf("submit queued request: %v", err)
	}
	_ = first.Close()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("old reader or writer did not finish")
	}
	if got := client.sendQueue.Len(); got != 1 {
		t.Fatalf("queue after disconnect = %d, want 1", got)
	}
	pendingCount := testPendingCount(client)
	if pendingCount != 2 {
		t.Fatalf("pending after disconnect = %d, want 2", pendingCount)
	}
	thirdFuture, err := client.submit(client.newReleaseRequest(3, 3))
	if err != nil {
		t.Fatalf("submit while disconnected: %v", err)
	}

	secondContext, cancelSecond := context.WithCancel(context.Background())
	second := &fakeLeaseConnection{
		ctx: secondContext, cancel: cancelSecond,
		requests: make(chan observedRequest, 2), responses: make(chan protocol.Response, 2),
	}
	client.stateMu.Lock()
	client.connection = second.transportConnection()
	client.stateMu.Unlock()
	secondDone := make(chan struct{})
	client.manager.Go(func() {
		_ = client.runConnection(second.transportConnection())
		close(secondDone)
	})
	request := <-second.requests
	if request.Key != 2 {
		t.Fatalf("reconnected writer sent key %d, want 2", request.Key)
	}
	thirdRequest := <-second.requests
	if thirdRequest.Key != 3 {
		t.Fatalf("reconnected writer sent next key %d, want 3", thirdRequest.Key)
	}
	second.responses <- releaseServerResponse(request.RequestID)
	second.responses <- releaseServerResponse(thirdRequest.RequestID)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := secondFuture.await(ctx, nil)
	if err != nil || response.RequestID != request.RequestID {
		t.Fatalf("queued response = (%+v, %v)", response, err)
	}
	response, err = thirdFuture.await(ctx, nil)
	if err != nil || response.RequestID != thirdRequest.RequestID {
		t.Fatalf("offline queued response = (%+v, %v)", response, err)
	}
	_, err = firstFuture.await(context.Background(), time.After(10*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost request error = %v, want timeout", err)
	}
	_ = second.Close()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("new reader or writer did not finish")
	}
}

func TestClientCloseWakesPendingAndDiscardsQueue(t *testing.T) {
	connectionContext, cancelConnection := context.WithCancel(context.Background())
	connection := &fakeLeaseConnection{
		ctx: connectionContext, cancel: cancelConnection,
		requests: make(chan observedRequest), responses: make(chan protocol.Response),
		sendStart: make(chan struct{}, 1),
	}
	client, _ := startTestConnection(t, connection, time.Second)
	first, err := client.submit(client.newReleaseRequest(1, 1))
	if err != nil {
		t.Fatalf("submit first: %v", err)
	}
	select {
	case <-connection.sendStart:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	second, err := client.submit(client.newReleaseRequest(2, 2))
	if err != nil {
		t.Fatalf("submit second: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, future := range []*connectionFuture{first, second} {
		if _, err := future.await(context.Background(), nil); !errors.Is(err, ErrClientClosed) {
			t.Fatalf("pending result after Close = %v, want ErrClientClosed", err)
		}
	}
	if got := client.sendQueue.Len(); got != 0 {
		t.Fatalf("queue after Close = %d, want 0", got)
	}
}

func releaseServerResponse(requestID uint64) protocol.Response {
	return protocol.Response{
		RequestID: requestID,
		Operation: redleasev1.ClientOperationRELEASE,
		Status:    redleasev1.LeaseStatusOK,
	}
}
