package client1of1

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
	errStreamClosed       = errors.New("stream closed")
)

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

type streamResult struct {
	response *redleasev1.ServerResponse
	err      error
}

type streamFuture struct {
	generation *streamGeneration
	requestID  uint64
	result     chan streamResult
}

func (f *streamFuture) await(ctx context.Context) (*redleasev1.ServerResponse, error) {
	select {
	case result := <-f.result:
		return result.response, result.err
	case <-ctx.Done():
		f.generation.complete(f.requestID, streamResult{err: ctx.Err()})
		result := <-f.result
		return result.response, result.err
	}
}

type streamGeneration struct {
	stream leaseClientStream
	cancel context.CancelFunc

	sendToken chan struct{}
	done      chan struct{}

	stateMu            sync.Mutex
	nextRequestID      uint64
	requestIDExhausted bool
	pending            map[uint64]chan streamResult
	terminalErr        error

	terminateOnce sync.Once
	receiver      sync.WaitGroup
	closeOnce     sync.Once
	closeDone     chan struct{}
}

func newStreamGeneration(
	stream leaseClientStream,
	cancel context.CancelFunc,
) *streamGeneration {
	generation := &streamGeneration{
		stream:    stream,
		cancel:    cancel,
		sendToken: make(chan struct{}, 1),
		done:      make(chan struct{}),
		pending:   make(map[uint64]chan streamResult),
		closeDone: make(chan struct{}),
	}
	generation.sendToken <- struct{}{}
	generation.receiver.Add(1)
	go generation.receive()
	return generation
}

// submit returns only after Send has completed. This is the ordering barrier
// used before a cleanup Release is submitted after an ambiguous mutation.
func (g *streamGeneration) submit(
	ctx context.Context,
	request *redleasev1.ClientRequest,
) (*streamFuture, error) {
	if request == nil {
		return nil, errNilStreamRequest
	}

	future, requestID, err := g.register()
	if err != nil {
		if errors.Is(err, errRequestIDExhausted) {
			g.terminate(err)
			return nil, g.err()
		}
		return nil, err
	}
	wireRequest := &redleasev1.ClientRequest{
		RequestId: requestID,
		Operation: request.Operation,
	}

	select {
	case <-g.sendToken:
	case <-ctx.Done():
		g.complete(requestID, streamResult{err: ctx.Err()})
		return nil, ctx.Err()
	case <-g.done:
		return nil, g.err()
	}

	if err := g.err(); err != nil {
		g.sendToken <- struct{}{}
		return nil, err
	}
	stopSendWatch := context.AfterFunc(ctx, func() {
		g.terminate(fmt.Errorf("send deadline: %w", ctx.Err()))
	})
	sendErr := g.stream.Send(wireRequest)
	stopSendWatch()
	g.sendToken <- struct{}{}
	if sendErr != nil {
		g.terminate(fmt.Errorf("send: %w", sendErr))
		return nil, g.err()
	}
	if err := g.err(); err != nil {
		return nil, err
	}
	return future, nil
}

func (g *streamGeneration) register() (*streamFuture, uint64, error) {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	if g.terminalErr != nil {
		return nil, 0, g.terminalErr
	}
	if g.requestIDExhausted {
		return nil, 0, errRequestIDExhausted
	}

	requestID := g.nextRequestID
	if requestID == math.MaxUint64 {
		g.requestIDExhausted = true
	} else {
		g.nextRequestID++
	}
	result := make(chan streamResult, 1)
	g.pending[requestID] = result
	return &streamFuture{
		generation: g,
		requestID:  requestID,
		result:     result,
	}, requestID, nil
}

func (g *streamGeneration) complete(requestID uint64, result streamResult) bool {
	g.stateMu.Lock()
	pending := g.pending[requestID]
	if pending != nil {
		delete(g.pending, requestID)
	}
	g.stateMu.Unlock()
	if pending == nil {
		return false
	}
	pending <- result
	return true
}

func (g *streamGeneration) receive() {
	defer g.receiver.Done()
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
		g.complete(response.GetRequestId(), streamResult{response: response})
	}
}

func (g *streamGeneration) terminate(cause error) {
	g.terminateOnce.Do(func() {
		transportErr := &streamTransportError{cause: cause}

		g.stateMu.Lock()
		g.terminalErr = transportErr
		pending := g.pending
		g.pending = make(map[uint64]chan streamResult)
		g.stateMu.Unlock()

		close(g.done)
		g.cancel()
		result := streamResult{err: transportErr}
		for _, call := range pending {
			call <- result
		}
	})
}

func (g *streamGeneration) err() error {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	return g.terminalErr
}

func (g *streamGeneration) Close() {
	g.closeOnce.Do(func() {
		g.terminate(errStreamClosed)
		<-g.sendToken
		_ = g.stream.CloseSend()
		g.sendToken <- struct{}{}
		g.receiver.Wait()
		close(g.closeDone)
	})
	<-g.closeDone
}
