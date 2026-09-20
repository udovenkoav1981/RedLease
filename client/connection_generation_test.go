package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

func TestStreamGenerationCorrelatesOutOfOrderResponses(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	firstResult := startStreamCall(generation, acquireStreamRequest(1))
	firstRequest := receiveSentRequest(t, stream)
	secondResult := startStreamCall(generation, acquireStreamRequest(2))
	secondRequest := receiveSentRequest(t, stream)

	stream.receive <- fakeReceive{
		response: streamResponse(secondRequest.RequestID, protocol.StatusBusy),
	}
	stream.receive <- fakeReceive{
		response: streamResponse(firstRequest.RequestID, protocol.StatusOK),
	}

	second := receiveCallResult(t, secondResult)
	first := receiveCallResult(t, firstResult)
	if second.err != nil || second.response.Status != protocol.StatusBusy {
		t.Fatalf("unexpected second result: %+v", second)
	}
	if first.err != nil || first.response.Status != protocol.StatusOK {
		t.Fatalf("unexpected first result: %+v", first)
	}
	if firstRequest.RequestID >= secondRequest.RequestID {
		t.Fatalf("request IDs are not increasing: %d, %d", firstRequest.RequestID, secondRequest.RequestID)
	}
}

func TestStreamGenerationCallRemainsSubmitAndAwaitWrapper(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	result := startStreamCall(generation, acquireStreamRequest(1))
	request := receiveSentRequest(t, stream)
	stream.receive <- fakeReceive{
		response: streamResponse(request.RequestID, protocol.StatusOK),
	}

	received := receiveCallResult(t, result)
	if received.err != nil {
		t.Fatalf("call: %v", received.err)
	}
	if received.response.RequestID != request.RequestID {
		t.Fatalf(
			"response request ID = %d, want %d",
			received.response.RequestID,
			request.RequestID,
		)
	}
}

func TestStreamFutureBuffersResponseBeforeAwait(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	future, err := generation.submit(context.Background(), acquireStreamRequest(1))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	request := receiveSentRequest(t, stream)
	stream.receive <- fakeReceive{
		response: streamResponse(request.RequestID, protocol.StatusOK),
	}

	// Observe that Recv completed the buffered future before await is invoked,
	// then put the result back for the real await call.
	var buffered connectionCallResult
	select {
	case buffered = <-future.pending.result:
	case <-time.After(time.Second):
		t.Fatal("response was not buffered before await")
	}
	future.pending.result <- buffered

	response, err := future.await(context.Background())
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if response.RequestID != request.RequestID {
		t.Fatalf("response request ID = %d, want %d", response.RequestID, request.RequestID)
	}
}

func TestStreamSubmitReturnsAfterWriterAcceptanceBeforeSendCompletes(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	submission := startStreamSubmit(generation, context.Background(), acquireStreamRequest(1))
	stream.waitForSendAttempt(t)

	// fake Send cannot complete until the test receives from stream.sent.
	// Submission must nevertheless complete because the single writer has
	// already accepted the request into FIFO order.
	submitted := receiveSubmitResult(t, submission)
	if submitted.err != nil {
		t.Fatalf("submit: %v", submitted.err)
	}
	if submitted.future == nil {
		t.Fatal("submit returned a nil future")
	}

	request := receiveSentRequest(t, stream)
	stream.receive <- fakeReceive{
		response: streamResponse(request.RequestID, protocol.StatusOK),
	}
	if _, err := submitted.future.await(context.Background()); err != nil {
		t.Fatalf("await: %v", err)
	}
}

func TestStreamSubmitSendFailureCompletesAcceptedFuture(t *testing.T) {
	sendFailure := errors.New("send failure")
	generation, stream := newTestStreamGenerationWithOptions(t, fakeStreamOptions{sendErr: sendFailure})

	submission := startStreamSubmit(generation, context.Background(), acquireStreamRequest(1))
	stream.waitForSendAttempt(t)
	submitted := receiveSubmitResult(t, submission)
	if submitted.err != nil {
		t.Fatalf("accepted submit returned error: %v", submitted.err)
	}
	if submitted.future == nil {
		t.Fatal("accepted submit returned nil future")
	}

	_, err := submitted.future.await(context.Background())
	assertTransportCause(t, err, sendFailure)
}

func TestStreamSubmitCancellationBeforeWriterAcceptanceDoesNotSend(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	first, err := generation.submit(context.Background(), acquireStreamRequest(1))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	stream.waitForSendAttempt(t)

	secondContext, cancelSecond := context.WithCancel(context.Background())
	secondSubmission := startStreamSubmit(generation, secondContext, acquireStreamRequest(2))
	cancelSecond()
	second := receiveSubmitResult(t, secondSubmission)
	if !errors.Is(second.err, context.Canceled) {
		t.Fatalf("second submit error = %v, want context canceled", second.err)
	}
	if second.future != nil {
		t.Fatal("unaccepted canceled submit returned a future")
	}

	firstRequest := receiveSentRequest(t, stream)
	stream.receive <- fakeReceive{
		response: streamResponse(firstRequest.RequestID, protocol.StatusOK),
	}
	if _, err := first.await(context.Background()); err != nil {
		t.Fatalf("first await: %v", err)
	}

	select {
	case unexpected := <-stream.sent:
		t.Fatalf("canceled request was sent: %+v", unexpected)
	default:
	}
}

func TestStreamSubmitCancellationAfterWriterAcceptanceReturnsFuture(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	ctx, cancel := context.WithCancel(context.Background())
	submission := startStreamSubmit(generation, ctx, acquireStreamRequest(1))
	stream.waitForSendAttempt(t)
	cancel()

	submitted := receiveSubmitResult(t, submission)
	if submitted.err != nil {
		t.Fatalf("accepted submit returned error after cancellation: %v", submitted.err)
	}
	if submitted.future == nil {
		t.Fatal("accepted submit returned nil future after cancellation")
	}

	request := receiveSentRequest(t, stream)
	stream.receive <- fakeReceive{
		response: streamResponse(request.RequestID, protocol.StatusOK),
	}
	if _, err := submitted.future.await(context.Background()); err != nil {
		t.Fatalf("await: %v", err)
	}
}

func TestStreamResponseTimeoutTerminatesBlockedSend(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	result := startStreamCallWithContext(generation, ctx, acquireStreamRequest(1))
	stream.waitForSendAttempt(t)
	received := receiveCallResult(t, result)
	if !errors.Is(received.err, context.DeadlineExceeded) {
		t.Fatalf("blocked Send error = %v, want deadline exceeded", received.err)
	}

	select {
	case <-generation.done:
	case <-time.After(time.Second):
		t.Fatal("response timeout did not terminate blocked generation")
	}
}

func TestStreamGenerationTimeoutAndLateResponseDoNotBlockAnotherCall(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	timeoutContext, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	firstResult := startStreamCallWithContext(generation, timeoutContext, acquireStreamRequest(1))
	firstRequest := receiveSentRequest(t, stream)

	first := receiveCallResult(t, firstResult)
	if !errors.Is(first.err, context.DeadlineExceeded) {
		t.Fatalf("first call error = %v, want context deadline exceeded", first.err)
	}

	// The response arrives after its pending entry has been removed and must be
	// ignored. A later request on the same generation still completes normally.
	stream.receive <- fakeReceive{response: streamResponse(firstRequest.RequestID, protocol.StatusOK)}

	secondResult := startStreamCall(generation, acquireStreamRequest(2))
	secondRequest := receiveSentRequest(t, stream)
	stream.receive <- fakeReceive{response: streamResponse(secondRequest.RequestID, protocol.StatusOK)}

	second := receiveCallResult(t, secondResult)
	if second.err != nil {
		t.Fatalf("second call failed after late response: %v", second.err)
	}
}

func TestStreamGenerationReceiveFailureCompletesAllPendingCalls(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	firstResult := startStreamCall(generation, acquireStreamRequest(1))
	receiveSentRequest(t, stream)
	secondResult := startStreamCall(generation, acquireStreamRequest(2))
	receiveSentRequest(t, stream)

	receiveFailure := errors.New("receive failure")
	stream.receive <- fakeReceive{err: receiveFailure}

	assertTransportCause(t, receiveCallResult(t, firstResult).err, receiveFailure)
	assertTransportCause(t, receiveCallResult(t, secondResult).err, receiveFailure)

	_, err := generation.call(context.Background(), acquireStreamRequest(1))
	assertTransportCause(t, err, receiveFailure)
}

func TestStreamGenerationSendFailureIsTransportError(t *testing.T) {
	sendFailure := errors.New("send failure")
	generation, stream := newTestStreamGenerationWithOptions(t, fakeStreamOptions{sendErr: sendFailure})

	result := startStreamCall(generation, acquireStreamRequest(1))
	stream.waitForSendAttempt(t)
	assertTransportCause(t, receiveCallResult(t, result).err, sendFailure)
}

func TestStreamGenerationCloseFailureCompletesPendingAndIsIdempotent(t *testing.T) {
	closeFailure := errors.New("close failure")
	generation, stream := newTestStreamGenerationWithOptions(t, fakeStreamOptions{closeErr: closeFailure})

	result := startStreamCall(generation, acquireStreamRequest(1))
	receiveSentRequest(t, stream)

	if err := generation.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("Close error = %v, want %v", err, closeFailure)
	}
	assertTransportCause(t, receiveCallResult(t, result).err, errConnectionClosed)
	if err := generation.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("second Close error = %v, want %v", err, closeFailure)
	}
	if calls := stream.closeCalls.Load(); calls != 1 {
		t.Fatalf("Close called %d times, want 1", calls)
	}
}

func TestStreamGenerationConcurrentCalls(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	const calls = 128
	results := make([]<-chan connectionCallResult, calls)
	for i := range calls {
		results[i] = startStreamCall(generation, acquireStreamRequest(uint64(i+1)))
	}

	requests := make([]protocol.Request, calls)
	for i := range calls {
		requests[i] = receiveSentRequest(t, stream)
	}
	for i := calls - 1; i >= 0; i-- {
		stream.receive <- fakeReceive{
			response: streamResponse(requests[i].RequestID, protocol.StatusOK),
		}
	}
	for _, resultChannel := range results {
		result := receiveCallResult(t, resultChannel)
		if result.err != nil {
			t.Fatalf("concurrent call failed: %v", result.err)
		}
	}
}

type fakeReceive struct {
	response protocol.Response
	err      error
}

type streamSubmitResult struct {
	future *connectionFuture
	err    error
}

type fakeStreamOptions struct {
	sendErr  error
	closeErr error
}

type fakeLeaseClientStream struct {
	ctx context.Context //nolint:containedctx // Test stream owns this context.

	sent        chan protocol.Request
	receive     chan fakeReceive
	sendAttempt chan struct{}
	closed      chan struct{}

	sendErr  error
	closeErr error

	closeCalls      atomic.Int32
	closeOnce       sync.Once
	sendAttemptOnce sync.Once
}

func (s *fakeLeaseClientStream) Send(request protocol.Request) error {
	s.sendAttemptOnce.Do(func() { close(s.sendAttempt) })
	if s.sendErr != nil {
		return s.sendErr
	}

	select {
	case s.sent <- request:
		return nil
	case <-s.closed:
		return errConnectionClosed
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *fakeLeaseClientStream) Recv() (protocol.Response, error) {
	select {
	case result := <-s.receive:
		return result.response, result.err
	case <-s.closed:
		return protocol.Response{}, errConnectionClosed
	case <-s.ctx.Done():
		return protocol.Response{}, s.ctx.Err()
	}
}

func (s *fakeLeaseClientStream) Close() error {
	s.closeCalls.Add(1)
	s.closeOnce.Do(func() { close(s.closed) })
	return s.closeErr
}

func (s *fakeLeaseClientStream) waitForSendAttempt(t *testing.T) {
	t.Helper()
	select {
	case <-s.sendAttempt:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Send")
	}
}

func newTestStreamGeneration(t *testing.T) (*connectionGeneration, *fakeLeaseClientStream) {
	t.Helper()
	return newTestStreamGenerationWithOptions(t, fakeStreamOptions{})
}

func newTestStreamGenerationWithOptions(
	t *testing.T,
	options fakeStreamOptions,
) (*connectionGeneration, *fakeLeaseClientStream) {
	t.Helper()

	streamContext, cancel := context.WithCancel(context.Background())
	stream := &fakeLeaseClientStream{
		ctx:         streamContext,
		sent:        make(chan protocol.Request),
		receive:     make(chan fakeReceive, 256),
		sendAttempt: make(chan struct{}),
		closed:      make(chan struct{}),
		sendErr:     options.sendErr,
		closeErr:    options.closeErr,
	}
	generation := newConnectionGeneration(stream, cancel)
	t.Cleanup(func() { _ = generation.Close() })
	return generation, stream
}

func startStreamCall(
	generation *connectionGeneration,
	request protocol.Request,
) <-chan connectionCallResult {
	return startStreamCallWithContext(generation, context.Background(), request)
}

func startStreamCallWithContext(
	generation *connectionGeneration,
	ctx context.Context,
	request protocol.Request,
) <-chan connectionCallResult {
	result := make(chan connectionCallResult, 1)
	go func() {
		response, err := generation.call(ctx, request)
		result <- connectionCallResult{response: response, err: err}
	}()
	return result
}

func startStreamSubmit(
	generation *connectionGeneration,
	ctx context.Context,
	request protocol.Request,
) <-chan streamSubmitResult {
	result := make(chan streamSubmitResult, 1)
	go func() {
		future, err := generation.submit(ctx, request)
		result <- streamSubmitResult{future: future, err: err}
	}()
	return result
}

func receiveSubmitResult(t *testing.T, result <-chan streamSubmitResult) streamSubmitResult {
	t.Helper()
	select {
	case received := <-result:
		return received
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for submit result")
		return streamSubmitResult{}
	}
}

func receiveSentRequest(t *testing.T, stream *fakeLeaseClientStream) protocol.Request {
	t.Helper()
	select {
	case request := <-stream.sent:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sent request")
		return protocol.Request{}
	}
}

func receiveCallResult(t *testing.T, result <-chan connectionCallResult) connectionCallResult {
	t.Helper()
	select {
	case received := <-result:
		return received
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for call result")
		return connectionCallResult{}
	}
}

func assertTransportCause(t *testing.T, err, cause error) {
	t.Helper()
	if _, ok := errors.AsType[*connectionTransportError](err); !ok {
		t.Fatalf("error %v is not a stream transport error", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error %v does not wrap %v", err, cause)
	}
}

func acquireStreamRequest(key uint64) protocol.Request {
	return protocol.Request{Operation: protocol.OperationAcquire, Key: key}
}

func streamResponse(requestID uint64, status protocol.Status) protocol.Response {
	return protocol.Response{
		RequestID: requestID,
		Operation: protocol.OperationAcquire,
		Status:    status,
	}
}
