package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

var errConnectionClosed = errors.New("connection generation closed")

const initialSendBatchCapacity = 64

type connectionTransportError struct {
	cause error
}

func (e *connectionTransportError) Error() string {
	return "connection transport: " + e.cause.Error()
}

func (e *connectionTransportError) Unwrap() error {
	return e.cause
}

type connectionCallResult struct {
	response protocol.Response
	err      error
}

type pendingConnectionCall struct {
	result chan connectionCallResult
}

type connectionFuture struct {
	generation *connectionGeneration
	requestID  uint64
	pending    *pendingConnectionCall
	outbound   *outboundConnectionRequest
}

func (f *connectionFuture) await(ctx context.Context) (protocol.Response, error) {
	select {
	case result := <-f.pending.result:
		return result.response, result.err
	case <-ctx.Done():
		// A response deadline may be earlier than the submission deadline. An
		// ordinary cancellation only abandons this response; the independent
		// submission-deadline watchdog still breaks a genuinely stuck Send.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) && !f.outbound.sendComplete() {
			f.generation.terminate(fmt.Errorf("send deadline: %w", ctx.Err()))
		}
		f.generation.complete(f.requestID, connectionCallResult{err: ctx.Err()})
		result := <-f.pending.result
		return result.response, result.err
	}
}

type outboundRequestState uint8

const (
	outboundRequestQueued outboundRequestState = iota
	outboundRequestAccepted
	outboundRequestCanceled
)

type outboundConnectionRequest struct {
	request     redleasev1.ClientRequest
	builder     *flatbuffers.Builder
	builderPool *sync.Pool

	mu       sync.Mutex
	state    outboundRequestState
	accepted chan struct{}
	sent     chan struct{}
	deadline time.Time
}

func (c *Client) newOutboundRequest() *outboundConnectionRequest {
	var builder *flatbuffers.Builder
	if pooled := c.requestBuilderPool.Get(); pooled != nil {
		if reusable, ok := pooled.(*flatbuffers.Builder); ok {
			builder = reusable
			builder.Reset()
		}
	}
	if builder == nil {
		builder = flatbuffers.NewBuilder(protocol.NewBuilderSize)
	}
	return &outboundConnectionRequest{
		builder:     builder,
		builderPool: &c.requestBuilderPool,
		state:       outboundRequestQueued,
		accepted:    make(chan struct{}),
		sent:        make(chan struct{}),
	}
}

func (*Client) finishOutboundRequest(outbound *outboundConnectionRequest) {
	root := redleasev1.ClientRequestEnd(outbound.builder)
	redleasev1.FinishSizePrefixedClientRequestBuffer(outbound.builder, root)
	frame := outbound.builder.FinishedBytes()
	rootOffset := flatbuffers.GetUOffsetT(frame[flatbuffers.SizeUint32:]) +
		flatbuffers.UOffsetT(flatbuffers.SizeUint32)
	outbound.request.Init(frame, rootOffset)
}

func (r *outboundConnectionRequest) releaseRequest() {
	r.request = redleasev1.ClientRequest{}
	if r.builder == nil {
		return
	}
	builder := r.builder
	r.builder = nil
	if r.builderPool != nil {
		r.builderPool.Put(builder)
	}
}

func (r *outboundConnectionRequest) finishSend() {
	close(r.sent)
}

func (r *outboundConnectionRequest) sendComplete() bool {
	select {
	case <-r.sent:
		return true
	default:
		return false
	}
}

func (r *outboundConnectionRequest) beginSend() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == outboundRequestCanceled {
		return false
	}
	r.state = outboundRequestAccepted
	// Closing accepted is the submission barrier: the single writer has accepted
	// this request into connection order before it buffers the FlatBuffer frame.
	close(r.accepted)
	return true
}

func (r *outboundConnectionRequest) cancelBeforeSend() outboundRequestState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == outboundRequestQueued {
		r.state = outboundRequestCanceled
	}
	return r.state
}

func (r *outboundConnectionRequest) currentState() outboundRequestState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

type connectionGeneration struct {
	connection transport.LeaseConnection
	cancel     context.CancelFunc

	sendQueue chan *outboundConnectionRequest
	done      chan struct{}

	pendingMu   sync.Mutex
	pending     map[uint64]*pendingConnectionCall
	terminalErr error

	terminateOnce      sync.Once
	closeOnce          sync.Once
	workers            sync.WaitGroup
	closeConnectionErr error
}

func newConnectionGeneration(
	connection transport.LeaseConnection,
	cancel context.CancelFunc,
) *connectionGeneration {
	generation := &connectionGeneration{
		connection: connection,
		cancel:     cancel,
		sendQueue:  make(chan *outboundConnectionRequest),
		done:       make(chan struct{}),
		pending:    make(map[uint64]*pendingConnectionCall),
	}

	generation.workers.Add(2)
	go generation.sendLoop()
	go generation.recvLoop()

	return generation
}

func (g *connectionGeneration) call(
	ctx context.Context,
	request *outboundConnectionRequest,
) (protocol.Response, error) {
	future, err := g.submit(ctx, request)
	if err != nil {
		return protocol.Response{}, err
	}
	return future.await(ctx)
}

func (g *connectionGeneration) submit(
	ctx context.Context,
	outbound *outboundConnectionRequest,
) (*connectionFuture, error) {
	requestID := outbound.request.RequestId()

	call := &pendingConnectionCall{result: make(chan connectionCallResult, 1)}
	if err := g.register(requestID, call); err != nil {
		outbound.releaseRequest()
		return nil, err
	}
	outbound.deadline, _ = ctx.Deadline()
	future := &connectionFuture{
		generation: g,
		requestID:  requestID,
		pending:    call,
		outbound:   outbound,
	}

	if err := ctx.Err(); err != nil {
		g.complete(requestID, connectionCallResult{err: err})
		outbound.releaseRequest()
		return nil, err
	}

	select {
	case g.sendQueue <- outbound:
	case <-ctx.Done():
		g.complete(requestID, connectionCallResult{err: ctx.Err()})
		outbound.releaseRequest()
		return nil, ctx.Err()
	case <-g.done:
		outbound.releaseRequest()
		return nil, g.err()
	}

	select {
	case <-outbound.accepted:
		return g.submissionOutcome(future, outbound)
	case <-ctx.Done():
		return g.cancelSubmission(ctx.Err(), future, outbound)
	case <-g.done:
		return g.cancelSubmission(g.err(), future, outbound)
	}
}

func (g *connectionGeneration) submissionOutcome(
	future *connectionFuture,
	outbound *outboundConnectionRequest,
) (*connectionFuture, error) {
	if outbound.currentState() == outboundRequestAccepted {
		return future, nil
	}
	if terminalErr := g.err(); terminalErr != nil {
		return nil, terminalErr
	}
	return nil, &connectionTransportError{cause: errConnectionClosed}
}

func (g *connectionGeneration) cancelSubmission(
	cause error,
	future *connectionFuture,
	outbound *outboundConnectionRequest,
) (*connectionFuture, error) {
	state := outbound.cancelBeforeSend()
	switch state {
	case outboundRequestCanceled:
		g.complete(future.requestID, connectionCallResult{err: cause})
		return nil, cause
	case outboundRequestAccepted:
		return future, nil
	default:
		panic("unexpected outbound request state")
	}
}

func (g *connectionGeneration) Close() error {
	g.terminate(errConnectionClosed)
	g.workers.Wait()
	return g.closeConnectionErr
}

func (g *connectionGeneration) register(requestID uint64, call *pendingConnectionCall) error {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	if g.terminalErr != nil {
		return g.terminalErr
	}
	g.pending[requestID] = call
	return nil
}

func (g *connectionGeneration) complete(requestID uint64, result connectionCallResult) {
	g.pendingMu.Lock()
	call := g.pending[requestID]
	if call != nil {
		delete(g.pending, requestID)
	}
	g.pendingMu.Unlock()

	if call == nil {
		return
	}
	call.result <- result
}

func (g *connectionGeneration) sendLoop() {
	defer g.workers.Done()
	batch := make([]*outboundConnectionRequest, 0, initialSendBatchCapacity)

	for {
		var outbound *outboundConnectionRequest
		select {
		case <-g.done:
			return
		case outbound = <-g.sendQueue:
		}

		batch = batch[:0]
		for {
			if outbound.beginSend() {
				if !outbound.deadline.IsZero() {
					go g.watchSendDeadline(outbound)
				}
				batch = append(batch, outbound)

				err := g.connection.BufferClientRequest(&outbound.request)
				outbound.releaseRequest()
				if err != nil {
					finishSendBatch(batch)
					g.terminate(fmt.Errorf("send: %w", err))
					return
				}
			} else {
				outbound.releaseRequest()
			}

			select {
			case outbound = <-g.sendQueue:
				continue
			default:
			}
			if len(batch) == 0 {
				break
			}
			if err := g.connection.FlushClientRequests(); err != nil {
				finishSendBatch(batch)
				g.terminate(fmt.Errorf("flush send batch: %w", err))
				return
			}
			finishSendBatch(batch)
			break
		}
	}
}

func finishSendBatch(batch []*outboundConnectionRequest) {
	for _, outbound := range batch {
		outbound.finishSend()
	}
}

func (g *connectionGeneration) watchSendDeadline(outbound *outboundConnectionRequest) {
	delay := max(time.Until(outbound.deadline), 0)
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-outbound.sent:
	case <-g.done:
	case <-timer.C:
		if !outbound.sendComplete() {
			g.terminate(fmt.Errorf("send deadline: %w", context.DeadlineExceeded))
		}
	}
}

func (g *connectionGeneration) recvLoop() {
	defer g.workers.Done()

	for {
		response, err := g.connection.Recv()
		if err != nil {
			g.terminate(fmt.Errorf("receive: %w", err))
			return
		}
		g.complete(response.RequestID, connectionCallResult{response: response})
	}
}

func (g *connectionGeneration) terminate(cause error) {
	g.terminateOnce.Do(func() {
		transportErr := &connectionTransportError{cause: cause}

		g.pendingMu.Lock()
		g.terminalErr = transportErr
		pending := g.pending
		g.pending = make(map[uint64]*pendingConnectionCall)
		g.pendingMu.Unlock()

		close(g.done)
		g.cancel()
		g.closeConnection()

		result := connectionCallResult{err: transportErr}
		for _, call := range pending {
			call.result <- result
		}
	})
}

func (g *connectionGeneration) closeConnection() {
	g.closeOnce.Do(func() {
		g.closeConnectionErr = g.connection.Close()
	})
}

func (g *connectionGeneration) err() error {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	return g.terminalErr
}
