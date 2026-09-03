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
	timer := newControlledBackoffTimer()
	connection := newTestReplicaConn(t, factory, timer)

	wait := timer.nextWait(t)
	if wait.delay != 10*time.Millisecond {
		t.Fatalf("retry delay = %v, want 10ms", wait.delay)
	}

	_, err := connection.call(context.Background(), acquireStreamRequest("unavailable"))
	assertReplicaUnavailableCause(t, err, openFailure)

	stream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: stream}
	close(wait.proceed)
	waitForReplicaState(t, connection, true, false)

	if opens := factory.openCalls.Load(); opens != 2 {
		t.Fatalf("factory open calls = %d, want 2", opens)
	}
}

func TestReplicaConnReconnectsAfterGenerationFailure(t *testing.T) {
	factory := newScriptedStreamFactory()
	firstStream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: firstStream}
	timer := newControlledBackoffTimer()
	connection := newTestReplicaConn(t, factory, timer)
	waitForReplicaState(t, connection, true, false)

	receiveFailure := errors.New("stream disconnected")
	firstStream.receive <- fakeReceive{err: receiveFailure}
	waitForReplicaState(t, connection, false, false)

	wait := timer.nextWait(t)
	if wait.delay != 10*time.Millisecond {
		t.Fatalf("reconnect delay = %v, want 10ms", wait.delay)
	}
	secondStream := newReplicaFakeStream()
	factory.results <- streamFactoryResult{stream: secondStream}
	close(wait.proceed)
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

func TestReplicaConnCallWhenUnavailable(t *testing.T) {
	factory := newScriptedStreamFactory()
	timer := newControlledBackoffTimer()
	connection := newTestReplicaConn(t, factory, timer)

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
	timer := newControlledBackoffTimer()
	connection := newTestReplicaConnWithoutCleanup(factory, timer)
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
	timer := newControlledBackoffTimer()
	connection := newTestReplicaConn(t, factory, timer)
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

type controlledBackoffWait struct {
	delay   time.Duration
	proceed chan struct{}
}

type controlledBackoffTimer struct {
	waits chan controlledBackoffWait
}

func newControlledBackoffTimer() *controlledBackoffTimer {
	return &controlledBackoffTimer{waits: make(chan controlledBackoffWait, 16)}
}

func (t *controlledBackoffTimer) wait(ctx context.Context, delay time.Duration) bool {
	wait := controlledBackoffWait{delay: delay, proceed: make(chan struct{})}
	t.waits <- wait
	select {
	case <-wait.proceed:
		return true
	case <-ctx.Done():
		return false
	}
}

func (t *controlledBackoffTimer) nextWait(testingT *testing.T) controlledBackoffWait {
	testingT.Helper()
	select {
	case wait := <-t.waits:
		return wait
	case <-time.After(time.Second):
		testingT.Fatal("timed out waiting for reconnect backoff")
		return controlledBackoffWait{}
	}
}

func newTestReplicaConn(
	t *testing.T,
	factory streamFactory,
	timer backoffTimer,
) *replicaConn {
	t.Helper()
	connection := newTestReplicaConnWithoutCleanup(factory, timer)
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func newTestReplicaConnWithoutCleanup(factory streamFactory, timer backoffTimer) *replicaConn {
	return newReplicaConnWithConfig(replicaConnConfig{
		factory: factory,
		backoff: exponentialBackoff{
			initial: 10 * time.Millisecond,
			maximum: 80 * time.Millisecond,
		},
		timer: timer,
	})
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
