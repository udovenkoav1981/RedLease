package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

var errConnectionClosed = errors.New("connection generation closed")

// leaseConnection permits one Send goroutine and one Recv goroutine. Closing
// it must unblock both operations.
type leaseConnection interface {
	Send(request protocol.Request) error
	Recv() (protocol.Response, error)
	Close() error
}

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
	request protocol.Request

	mu       sync.Mutex
	state    outboundRequestState
	accepted chan struct{}
	sent     chan struct{}
	deadline time.Time
}

func newOutboundConnectionRequest(
	request protocol.Request,
	deadline time.Time,
) *outboundConnectionRequest {
	return &outboundConnectionRequest{
		request:  request,
		state:    outboundRequestQueued,
		accepted: make(chan struct{}),
		sent:     make(chan struct{}),
		deadline: deadline,
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
	// this request into connection order before it invokes Send.
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
	connection leaseConnection
	cancel     context.CancelFunc

	sendQueue chan *outboundConnectionRequest
	done      chan struct{}

	requestIDMu   sync.Mutex
	nextRequestID uint64

	pendingMu   sync.Mutex
	pending     map[uint64]*pendingConnectionCall
	terminalErr error

	terminateOnce      sync.Once
	closeOnce          sync.Once
	workers            sync.WaitGroup
	closeConnectionErr error
}

func newConnectionGeneration(connection leaseConnection, cancel context.CancelFunc) *connectionGeneration {
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
	request protocol.Request,
) (protocol.Response, error) {
	future, err := g.submit(ctx, request)
	if err != nil {
		return protocol.Response{}, err
	}
	return future.await(ctx)
}

func (g *connectionGeneration) submit(
	ctx context.Context,
	request protocol.Request,
) (*connectionFuture, error) {
	requestID := g.allocateRequestID()

	call := &pendingConnectionCall{result: make(chan connectionCallResult, 1)}
	if err := g.register(requestID, call); err != nil {
		return nil, err
	}
	requestCopy := request
	requestCopy.RequestID = requestID
	deadline, _ := ctx.Deadline()
	outbound := newOutboundConnectionRequest(requestCopy, deadline)
	future := &connectionFuture{
		generation: g,
		requestID:  requestID,
		pending:    call,
		outbound:   outbound,
	}

	if err := ctx.Err(); err != nil {
		g.complete(requestID, connectionCallResult{err: err})
		return nil, err
	}

	select {
	case g.sendQueue <- outbound:
	case <-ctx.Done():
		g.complete(requestID, connectionCallResult{err: ctx.Err()})
		return nil, ctx.Err()
	case <-g.done:
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

func (g *connectionGeneration) allocateRequestID() uint64 {
	g.requestIDMu.Lock()
	defer g.requestIDMu.Unlock()

	requestID := g.nextRequestID
	g.nextRequestID++
	return requestID
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

	for {
		select {
		case <-g.done:
			return
		case outbound := <-g.sendQueue:
			if !outbound.beginSend() {
				continue
			}
			if !outbound.deadline.IsZero() {
				go g.watchSendDeadline(outbound)
			}
			err := g.connection.Send(outbound.request)
			outbound.finishSend()
			if err != nil {
				g.terminate(fmt.Errorf("send: %w", err))
				return
			}
		}
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
