package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

var (
	errNilStreamRequest   = errors.New("nil stream request")
	errNilStreamResponse  = errors.New("nil stream response")
	errRequestIDExhausted = errors.New("stream request ID exhausted")
	errStreamClosed       = errors.New("stream generation closed")
)

// leaseClientStream is implemented by the generated gRPC bidirectional client
// stream. The cancel function passed to newStreamGeneration owns the context
// used to create the stream and must unblock Send and Recv.
type leaseClientStream interface {
	Send(*redleasev1.ClientRequest) error
	Recv() (*redleasev1.ServerResponse, error)
	CloseSend() error
}

var _ leaseClientStream = redleasev1.RedLease_LeaseStreamClient(nil)

type streamTransportError struct {
	cause error
}

func (e *streamTransportError) Error() string {
	return "stream transport: " + e.cause.Error()
}

func (e *streamTransportError) Unwrap() error {
	return e.cause
}

type streamCallResult struct {
	response *redleasev1.ServerResponse
	err      error
}

type pendingStreamCall struct {
	result chan streamCallResult
}

type outboundStreamRequest struct {
	request *redleasev1.ClientRequest
}

type streamGeneration struct {
	stream leaseClientStream
	cancel context.CancelFunc

	sendQueue chan outboundStreamRequest
	done      chan struct{}

	requestIDMu        sync.Mutex
	nextRequestID      uint64
	requestIDExhausted bool

	pendingMu   sync.Mutex
	pending     map[uint64]*pendingStreamCall
	terminalErr error

	terminateOnce sync.Once
	workers       sync.WaitGroup
	closeSendErr  error
}

func newStreamGeneration(stream leaseClientStream, cancel context.CancelFunc) *streamGeneration {
	generation := &streamGeneration{
		stream:    stream,
		cancel:    cancel,
		sendQueue: make(chan outboundStreamRequest),
		done:      make(chan struct{}),
		pending:   make(map[uint64]*pendingStreamCall),
	}

	generation.workers.Add(2)
	go generation.sendLoop()
	go generation.recvLoop()

	return generation
}

func (g *streamGeneration) call(
	ctx context.Context,
	request *redleasev1.ClientRequest,
) (*redleasev1.ServerResponse, error) {
	if request == nil {
		return nil, errNilStreamRequest
	}

	requestID, err := g.allocateRequestID()
	if err != nil {
		g.terminate(err)
		return nil, g.err()
	}

	call := &pendingStreamCall{result: make(chan streamCallResult, 1)}
	if err := g.register(requestID, call); err != nil {
		return nil, err
	}

	requestCopy := &redleasev1.ClientRequest{
		RequestId: requestID,
		Operation: request.Operation,
	}

	if err := ctx.Err(); err != nil {
		g.complete(requestID, streamCallResult{err: err})
		result := <-call.result
		return result.response, result.err
	}

	select {
	case g.sendQueue <- outboundStreamRequest{request: requestCopy}:
	case <-ctx.Done():
		g.complete(requestID, streamCallResult{err: ctx.Err()})
		result := <-call.result
		return result.response, result.err
	case <-g.done:
		result := <-call.result
		return result.response, result.err
	}

	select {
	case result := <-call.result:
		return result.response, result.err
	case <-ctx.Done():
		g.complete(requestID, streamCallResult{err: ctx.Err()})
		result := <-call.result
		return result.response, result.err
	}
}

func (g *streamGeneration) Close() error {
	g.terminate(errStreamClosed)
	g.workers.Wait()
	return g.closeSendErr
}

func (g *streamGeneration) allocateRequestID() (uint64, error) {
	g.requestIDMu.Lock()
	defer g.requestIDMu.Unlock()

	if g.requestIDExhausted {
		return 0, errRequestIDExhausted
	}

	requestID := g.nextRequestID
	if requestID == math.MaxUint64 {
		g.requestIDExhausted = true
	} else {
		g.nextRequestID++
	}
	return requestID, nil
}

func (g *streamGeneration) register(requestID uint64, call *pendingStreamCall) error {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	if g.terminalErr != nil {
		return g.terminalErr
	}
	g.pending[requestID] = call
	return nil
}

func (g *streamGeneration) complete(requestID uint64, result streamCallResult) bool {
	g.pendingMu.Lock()
	call := g.pending[requestID]
	if call != nil {
		delete(g.pending, requestID)
	}
	g.pendingMu.Unlock()

	if call == nil {
		return false
	}
	call.result <- result
	return true
}

func (g *streamGeneration) sendLoop() {
	defer g.workers.Done()
	defer func() {
		g.closeSendErr = g.stream.CloseSend()
	}()

	for {
		select {
		case <-g.done:
			return
		case outbound := <-g.sendQueue:
			if err := g.stream.Send(outbound.request); err != nil {
				g.terminate(fmt.Errorf("send: %w", err))
				return
			}
		}
	}
}

func (g *streamGeneration) recvLoop() {
	defer g.workers.Done()

	for {
		response, err := g.stream.Recv()
		if err != nil {
			g.terminate(fmt.Errorf("receive: %w", err))
			return
		}
		if response == nil {
			g.terminate(errNilStreamResponse)
			return
		}
		g.complete(response.GetRequestId(), streamCallResult{response: response})
	}
}

func (g *streamGeneration) terminate(cause error) {
	g.terminateOnce.Do(func() {
		transportErr := &streamTransportError{cause: cause}

		g.pendingMu.Lock()
		g.terminalErr = transportErr
		pending := g.pending
		g.pending = make(map[uint64]*pendingStreamCall)
		g.pendingMu.Unlock()

		close(g.done)
		g.cancel()

		result := streamCallResult{err: transportErr}
		for _, call := range pending {
			call.result <- result
		}
	})
}

func (g *streamGeneration) err() error {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	return g.terminalErr
}
