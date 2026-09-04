package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func TestReplicaConnRetriesOpenFailure(t *testing.T) {
	openFailure := errors.New("open failure")
	factory := newScriptedStreamFactory()
	factory.results <- streamFactoryResult{err: openFailure}
	connection := newTestReplicaConn(t, factory)
	waitForReplicaError(t, connection, openFailure)

	_, err := connection.call(context.Background(), acquireStreamRequest("unavailable"))
	assertReplicaUnavailableCause(t, err, openFailure)

	stream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: stream}
	waitForReplicaState(t, connection, true, false)

	if opens := factory.openCalls.Load(); opens != 2 {
		t.Fatalf("factory open calls = %d, want 2", opens)
	}
}

func TestReplicaConnReconnectsAfterGenerationFailure(t *testing.T) {
	factory := newScriptedStreamFactory()
	firstStream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: firstStream}
	connection := newTestReplicaConn(t, factory)
	waitForReplicaState(t, connection, true, false)

	secondStream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: secondStream}
	receiveFailure := errors.New("stream disconnected")
	firstStream.receive <- fakeReceive{err: receiveFailure}
	waitForReplicaState(t, connection, false, false)
	waitForReplicaState(t, connection, true, false)

	result := startReplicaCall(connection, acquireStreamRequest("after reconnect"))
	request := receiveSentRequest(t, secondStream)
	secondStream.receive <- fakeReceive{
		response: streamResponse(request.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK),
	}
	if received := receiveCallResult(t, result); received.err != nil {
		t.Fatalf("call after reconnect failed: %v", received.err)
	}
}

func TestReplicaConnReconnectsWhenRequestDeadlineBreaksBlockedSend(t *testing.T) {
	factory := newScriptedStreamFactory()
	firstStream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: firstStream}
	connection := newTestReplicaConn(t, factory)
	waitForReplicaState(t, connection, true, false)

	secondStream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: secondStream}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan streamCallResult, 1)
	go func() {
		response, err := connection.call(ctx, acquireStreamRequest("blocked-send"))
		result <- streamCallResult{response: response, err: err}
	}()
	firstStream.waitForSendAttempt(t)
	if err := receiveCallResult(t, result).err; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked call error = %v, want deadline exceeded", err)
	}
	waitForReplicaState(t, connection, false, false)
	waitForReplicaState(t, connection, true, false)

	secondResult := startReplicaCall(connection, acquireStreamRequest("after blocked send"))
	request := receiveSentRequest(t, secondStream)
	secondStream.receive <- fakeReceive{
		response: streamResponse(request.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK),
	}
	if received := receiveCallResult(t, secondResult); received.err != nil {
		t.Fatalf("call after reconnect failed: %v", received.err)
	}
}

func TestReplicaConnCallWhenUnavailable(t *testing.T) {
	factory := newScriptedStreamFactory()
	connection := newTestReplicaConn(t, factory)

	_, err := connection.call(context.Background(), acquireStreamRequest("key"))
	var unavailable *replicaUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("error %v is not replicaUnavailableError", err)
	}
}

func TestReplicaConnCloseStopsPendingCallAndFactory(t *testing.T) {
	closeFailure := errors.New("connection close failure")
	factory := newScriptedStreamFactory()
	factory.closeErr = closeFailure
	stream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: stream}
	connection := newTestReplicaConnWithoutCleanup(factory)
	waitForReplicaState(t, connection, true, false)

	result := startReplicaCall(connection, acquireStreamRequest("pending"))
	receiveSentRequest(t, stream)

	if err := connection.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("Close error = %v, want %v", err, closeFailure)
	}
	assertTransportCause(t, receiveCallResult(t, result).err, errStreamClosed)
	waitForReplicaState(t, connection, false, true)

	if err := connection.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("second Close error = %v, want %v", err, closeFailure)
	}
	if calls := factory.closeCalls.Load(); calls != 1 {
		t.Fatalf("factory close calls = %d, want 1", calls)
	}
	if calls := stream.closeSendCalls.Load(); calls != 1 {
		t.Fatalf("stream CloseSend calls = %d, want 1", calls)
	}

	_, err := connection.call(context.Background(), acquireStreamRequest("after close"))
	assertReplicaUnavailableCause(t, err, errReplicaClosed)
}

func TestReplicaConnConcurrentCalls(t *testing.T) {
	factory := newScriptedStreamFactory()
	stream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: stream}
	connection := newTestReplicaConn(t, factory)
	waitForReplicaState(t, connection, true, false)

	const calls = 64
	results := make([]<-chan streamCallResult, calls)
	for i := range calls {
		results[i] = startReplicaCall(connection, acquireStreamRequest("key"))
	}

	requests := make([]*redleasev1.ClientRequest, calls)
	for i := range calls {
		requests[i] = receiveSentRequest(t, stream)
	}
	for i := calls - 1; i >= 0; i-- {
		stream.receive <- fakeReceive{
			response: streamResponse(requests[i].GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK),
		}
	}
	for _, result := range results {
		if received := receiveCallResult(t, result); received.err != nil {
			t.Fatalf("concurrent replica call failed: %v", received.err)
		}
	}
}

type streamFactoryResult struct {
	stream *fakeLeaseClientStream
	err    error
}

type scriptedStreamFactory struct {
	results chan streamFactoryResult

	openCalls  atomic.Int32
	closeCalls atomic.Int32
	closeErr   error
}

func newScriptedStreamFactory() *scriptedStreamFactory {
	return &scriptedStreamFactory{results: make(chan streamFactoryResult, 16)}
}

func (f *scriptedStreamFactory) open(ctx context.Context) (leaseClientStream, error) {
	f.openCalls.Add(1)
	select {
	case result := <-f.results:
		if result.stream != nil {
			result.stream.ctx = ctx
		}
		return result.stream, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *scriptedStreamFactory) close() error {
	f.closeCalls.Add(1)
	return f.closeErr
}

func newTestReplicaConn(
	t *testing.T,
	factory streamFactory,
) *replicaConn {
	t.Helper()
	connection := newTestReplicaConnWithoutCleanup(factory)
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func newTestReplicaConnWithoutCleanup(factory streamFactory) *replicaConn {
	return newReplicaConn(factory)
}

func newReplicaFakeStream() *fakeLeaseClientStream {
	return &fakeLeaseClientStream{
		sent:        make(chan *redleasev1.ClientRequest),
		receive:     make(chan fakeReceive, 256),
		sendAttempt: make(chan struct{}),
	}
}

func startReplicaCall(
	connection *replicaConn,
	request *redleasev1.ClientRequest,
) <-chan streamCallResult {
	result := make(chan streamCallResult, 1)
	go func() {
		response, err := connection.call(context.Background(), request)
		result <- streamCallResult{response: response, err: err}
	}()
	return result
}

func waitForReplicaState(t *testing.T, connection *replicaConn, wantReady, wantClosed bool) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		ready, closed, changed := connection.readiness()
		if ready == wantReady && closed == wantClosed {
			return
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf(
				"timed out waiting for replica state ready=%v closed=%v; got ready=%v closed=%v",
				wantReady,
				wantClosed,
				ready,
				closed,
			)
		}
	}
}

func waitForReplicaError(t *testing.T, connection *replicaConn, want error) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		connection.stateMu.Lock()
		got := connection.lastErr
		connection.stateMu.Unlock()
		if errors.Is(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for replica error %v; got %v", want, got)
		}
		time.Sleep(time.Millisecond)
	}
}

func assertReplicaUnavailableCause(t *testing.T, err, cause error) {
	t.Helper()
	var unavailable *replicaUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("error %v is not replicaUnavailableError", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error %v does not wrap %v", err, cause)
	}
}

// Assert that the test-only stream still satisfies the production factory
// result type after concurrent lifecycle tests evolve.
var _ leaseClientStream = (*fakeLeaseClientStream)(nil)
var _ streamFactory = (*scriptedStreamFactory)(nil)
