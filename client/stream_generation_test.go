package client

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func TestStreamGenerationCorrelatesOutOfOrderResponses(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	firstResult := startStreamCall(generation, acquireStreamRequest("first"))
	firstRequest := receiveSentRequest(t, stream)
	secondResult := startStreamCall(generation, acquireStreamRequest("second"))
	secondRequest := receiveSentRequest(t, stream)

	stream.receive <- fakeReceive{
		response: streamResponse(secondRequest.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_BUSY),
	}
	stream.receive <- fakeReceive{
		response: streamResponse(firstRequest.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK),
	}

	second := receiveCallResult(t, secondResult)
	first := receiveCallResult(t, firstResult)
	if second.err != nil || second.response.GetAcquire().GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_BUSY {
		t.Fatalf("unexpected second result: %+v", second)
	}
	if first.err != nil || first.response.GetAcquire().GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("unexpected first result: %+v", first)
	}
	if firstRequest.GetRequestId() >= secondRequest.GetRequestId() {
		t.Fatalf("request IDs are not increasing: %d, %d", firstRequest.GetRequestId(), secondRequest.GetRequestId())
	}
}

func TestStreamGenerationCallRemainsSubmitAndAwaitWrapper(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	result := startStreamCall(generation, acquireStreamRequest("wrapper"))
	request := receiveSentRequest(t, stream)
	stream.receive <- fakeReceive{
		response: streamResponse(request.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK),
	}

	received := receiveCallResult(t, result)
	if received.err != nil {
		t.Fatalf("call: %v", received.err)
	}
	if received.response.GetRequestId() != request.GetRequestId() {
		t.Fatalf(
			"response request ID = %d, want %d",
			received.response.GetRequestId(),
			request.GetRequestId(),
		)
	}
}

func TestStreamFutureBuffersResponseBeforeAwait(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	future, err := generation.submit(context.Background(), acquireStreamRequest("buffered"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	request := receiveSentRequest(t, stream)
	stream.receive <- fakeReceive{
		response: streamResponse(request.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK),
	}

	// Observe that Recv completed the buffered future before await is invoked,
	// then put the result back for the real await call.
	var buffered streamCallResult
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
	if response.GetRequestId() != request.GetRequestId() {
		t.Fatalf("response request ID = %d, want %d", response.GetRequestId(), request.GetRequestId())
	}
}

func TestStreamSubmitReturnsAfterWriterAcceptanceBeforeSendCompletes(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	submission := startStreamSubmit(generation, context.Background(), acquireStreamRequest("barrier"))
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
		response: streamResponse(request.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK),
	}
	if _, err := submitted.future.await(context.Background()); err != nil {
		t.Fatalf("await: %v", err)
	}
}

func TestStreamSubmitSendFailureCompletesAcceptedFuture(t *testing.T) {
	sendFailure := errors.New("send failure")
	generation, stream := newTestStreamGenerationWithOptions(t, fakeStreamOptions{sendErr: sendFailure})

	submission := startStreamSubmit(generation, context.Background(), acquireStreamRequest("request"))
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

	first, err := generation.submit(context.Background(), acquireStreamRequest("first"))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	stream.waitForSendAttempt(t)

	secondContext, cancelSecond := context.WithCancel(context.Background())
	secondSubmission := startStreamSubmit(generation, secondContext, acquireStreamRequest("canceled"))
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
		response: streamResponse(firstRequest.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK),
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
	submission := startStreamSubmit(generation, ctx, acquireStreamRequest("accepted"))
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
		response: streamResponse(request.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK),
	}
	if _, err := submitted.future.await(context.Background()); err != nil {
		t.Fatalf("await: %v", err)
	}
}

func TestStreamGenerationTimeoutAndLateResponseDoNotBlockAnotherCall(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	timeoutContext, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	firstResult := startStreamCallWithContext(generation, timeoutContext, acquireStreamRequest("omitted"))
	firstRequest := receiveSentRequest(t, stream)

	first := receiveCallResult(t, firstResult)
	if !errors.Is(first.err, context.DeadlineExceeded) {
		t.Fatalf("first call error = %v, want context deadline exceeded", first.err)
	}

	// The response arrives after its pending entry has been removed and must be
	// ignored. A later request on the same generation still completes normally.
	stream.receive <- fakeReceive{response: streamResponse(firstRequest.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK)}

	secondResult := startStreamCall(generation, acquireStreamRequest("next"))
	secondRequest := receiveSentRequest(t, stream)
	stream.receive <- fakeReceive{response: streamResponse(secondRequest.GetRequestId(), redleasev1.LeaseStatus_LEASE_STATUS_OK)}

	second := receiveCallResult(t, secondResult)
	if second.err != nil {
		t.Fatalf("second call failed after late response: %v", second.err)
	}
}

func TestStreamGenerationReceiveFailureCompletesAllPendingCalls(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	firstResult := startStreamCall(generation, acquireStreamRequest("first"))
	receiveSentRequest(t, stream)
	secondResult := startStreamCall(generation, acquireStreamRequest("second"))
	receiveSentRequest(t, stream)

	receiveFailure := errors.New("receive failure")
	stream.receive <- fakeReceive{err: receiveFailure}

	assertTransportCause(t, receiveCallResult(t, firstResult).err, receiveFailure)
	assertTransportCause(t, receiveCallResult(t, secondResult).err, receiveFailure)

	_, err := generation.call(context.Background(), acquireStreamRequest("after failure"))
	assertTransportCause(t, err, receiveFailure)
}

func TestStreamGenerationSendFailureIsTransportError(t *testing.T) {
	sendFailure := errors.New("send failure")
	generation, stream := newTestStreamGenerationWithOptions(t, fakeStreamOptions{sendErr: sendFailure})

	result := startStreamCall(generation, acquireStreamRequest("request"))
	stream.waitForSendAttempt(t)
	assertTransportCause(t, receiveCallResult(t, result).err, sendFailure)
}

func TestStreamGenerationCloseFailureCompletesPendingAndIsIdempotent(t *testing.T) {
	closeFailure := errors.New("close failure")
	generation, stream := newTestStreamGenerationWithOptions(t, fakeStreamOptions{closeErr: closeFailure})

	result := startStreamCall(generation, acquireStreamRequest("pending"))
	receiveSentRequest(t, stream)

	if err := generation.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("Close error = %v, want %v", err, closeFailure)
	}
	assertTransportCause(t, receiveCallResult(t, result).err, errStreamClosed)
	if err := generation.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("second Close error = %v, want %v", err, closeFailure)
	}
	if calls := stream.closeSendCalls.Load(); calls != 1 {
		t.Fatalf("CloseSend called %d times, want 1", calls)
	}
}

func TestStreamGenerationRequestIDExhaustionFailsGenerationWithoutReuse(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)
	generation.nextRequestID = math.MaxUint64

	lastResult := startStreamCall(generation, acquireStreamRequest("last"))
	lastRequest := receiveSentRequest(t, stream)
	if lastRequest.GetRequestId() != math.MaxUint64 {
		t.Fatalf("last request ID = %d, want %d", lastRequest.GetRequestId(), uint64(math.MaxUint64))
	}

	_, err := generation.call(context.Background(), acquireStreamRequest("overflow"))
	assertTransportCause(t, err, errRequestIDExhausted)
	assertTransportCause(t, receiveCallResult(t, lastResult).err, errRequestIDExhausted)
}

func TestStreamGenerationConcurrentCalls(t *testing.T) {
	generation, stream := newTestStreamGeneration(t)

	const calls = 128
	results := make([]<-chan streamCallResult, calls)
	for i := range calls {
		results[i] = startStreamCall(generation, acquireStreamRequest("key"))
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
	for _, resultChannel := range results {
		result := receiveCallResult(t, resultChannel)
		if result.err != nil {
			t.Fatalf("concurrent call failed: %v", result.err)
		}
	}
}

type fakeReceive struct {
	response *redleasev1.ServerResponse
	err      error
}

type streamSubmitResult struct {
	future *streamFuture
	err    error
}

type fakeStreamOptions struct {
	sendErr  error
	closeErr error
}

type fakeLeaseClientStream struct {
	ctx context.Context

	sent        chan *redleasev1.ClientRequest
	receive     chan fakeReceive
	sendAttempt chan struct{}

	sendErr  error
	closeErr error

	closeSendCalls  atomic.Int32
	sendAttemptOnce sync.Once
}

func (s *fakeLeaseClientStream) Send(request *redleasev1.ClientRequest) error {
	s.sendAttemptOnce.Do(func() { close(s.sendAttempt) })
	if s.sendErr != nil {
		return s.sendErr
	}

	requestCopy := &redleasev1.ClientRequest{
		RequestId: request.RequestId,
		Operation: request.Operation,
	}
	select {
	case s.sent <- requestCopy:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *fakeLeaseClientStream) Recv() (*redleasev1.ServerResponse, error) {
	select {
	case result := <-s.receive:
		return result.response, result.err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *fakeLeaseClientStream) CloseSend() error {
	s.closeSendCalls.Add(1)
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

func newTestStreamGeneration(t *testing.T) (*streamGeneration, *fakeLeaseClientStream) {
	t.Helper()
	return newTestStreamGenerationWithOptions(t, fakeStreamOptions{})
}

func newTestStreamGenerationWithOptions(
	t *testing.T,
	options fakeStreamOptions,
) (*streamGeneration, *fakeLeaseClientStream) {
	t.Helper()

	streamContext, cancel := context.WithCancel(context.Background())
	stream := &fakeLeaseClientStream{
		ctx:         streamContext,
		sent:        make(chan *redleasev1.ClientRequest),
		receive:     make(chan fakeReceive, 256),
		sendAttempt: make(chan struct{}),
		sendErr:     options.sendErr,
		closeErr:    options.closeErr,
	}
	generation := newStreamGeneration(stream, cancel)
	t.Cleanup(func() { _ = generation.Close() })
	return generation, stream
}

func startStreamCall(
	generation *streamGeneration,
	request *redleasev1.ClientRequest,
) <-chan streamCallResult {
	return startStreamCallWithContext(generation, context.Background(), request)
}

func startStreamCallWithContext(
	generation *streamGeneration,
	ctx context.Context,
	request *redleasev1.ClientRequest,
) <-chan streamCallResult {
	result := make(chan streamCallResult, 1)
	go func() {
		response, err := generation.call(ctx, request)
		result <- streamCallResult{response: response, err: err}
	}()
	return result
}

func startStreamSubmit(
	generation *streamGeneration,
	ctx context.Context,
	request *redleasev1.ClientRequest,
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

func receiveSentRequest(t *testing.T, stream *fakeLeaseClientStream) *redleasev1.ClientRequest {
	t.Helper()
	select {
	case request := <-stream.sent:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sent request")
		return nil
	}
}

func receiveCallResult(t *testing.T, result <-chan streamCallResult) streamCallResult {
	t.Helper()
	select {
	case received := <-result:
		return received
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for call result")
		return streamCallResult{}
	}
}

func assertTransportCause(t *testing.T, err, cause error) {
	t.Helper()
	var transportErr *streamTransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("error %v is not a stream transport error", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error %v does not wrap %v", err, cause)
	}
}

func acquireStreamRequest(key string) *redleasev1.ClientRequest {
	return &redleasev1.ClientRequest{
		Operation: &redleasev1.ClientRequest_Acquire{
			Acquire: &redleasev1.AcquireRequest{Key: []byte(key)},
		},
	}
}

func streamResponse(requestID uint64, status redleasev1.LeaseStatus) *redleasev1.ServerResponse {
	return &redleasev1.ServerResponse{
		RequestId: requestID,
		Result: &redleasev1.ServerResponse_Acquire{
			Acquire: &redleasev1.AcquireResponse{Status: status},
		},
	}
}
