package client1of1

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

var (
	errNilStreamRequest  = errors.New("nil stream request")
	errNilStreamResponse = errors.New("nil stream response")
	errStreamClosed      = errors.New("stream closed")
)

const sendQueueCapacity = 256

type leaseClientStream interface {
	Send(request *redleasev1.ClientRequest) error
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

type outboundStreamRequest struct {
	request  *redleasev1.ClientRequest
	deadline time.Time
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

	sendQueue chan *outboundStreamRequest
	done      chan struct{}

	stateMu       sync.Mutex
	nextRequestID uint64
	pending       map[uint64]chan streamResult
	terminalErr   error

	terminateOnce sync.Once
	workers       sync.WaitGroup
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
		sendQueue: make(chan *outboundStreamRequest, sendQueueCapacity),
		done:      make(chan struct{}),
		pending:   make(map[uint64]chan streamResult),
		closeDone: make(chan struct{}),
	}
	generation.workers.Add(2)
	go generation.send()
	go generation.receive()
	return generation
}

// submit returns after the request has entered the FIFO send queue. This is
// the ordering barrier used before a cleanup Release is submitted after an
// ambiguous mutation.
func (g *streamGeneration) submit(
	ctx context.Context,
	request *redleasev1.ClientRequest,
) (*streamFuture, error) {
	if request == nil {
		return nil, errNilStreamRequest
	}

	future, requestID, err := g.register()
	if err != nil {
		return nil, err
	}
	request.RequestId = requestID
	deadline, _ := ctx.Deadline()
	outbound := &outboundStreamRequest{request: request, deadline: deadline}

	select {
	case g.sendQueue <- outbound:
	case <-ctx.Done():
		g.complete(requestID, streamResult{err: ctx.Err()})
		return nil, ctx.Err()
	case <-g.done:
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
	requestID := g.nextRequestID
	g.nextRequestID++
	result := make(chan streamResult, 1)
	g.pending[requestID] = result
	return &streamFuture{
		generation: g,
		requestID:  requestID,
		result:     result,
	}, requestID, nil
}

func (g *streamGeneration) complete(requestID uint64, result streamResult) {
	g.stateMu.Lock()
	pending := g.pending[requestID]
	if pending != nil {
		delete(g.pending, requestID)
	}
	g.stateMu.Unlock()
	if pending == nil {
		return
	}
	pending <- result
}

func (g *streamGeneration) send() {
	defer g.workers.Done()
	defer func() {
		_ = g.stream.CloseSend()
	}()

	for {
		select {
		case <-g.done:
			return
		case outbound := <-g.sendQueue:
			if err := g.err(); err != nil {
				return
			}
			if !outbound.deadline.IsZero() && !time.Now().Before(outbound.deadline) {
				g.complete(outbound.request.GetRequestId(), streamResult{err: context.DeadlineExceeded})
				continue
			}
			if err := g.sendRequest(outbound); err != nil {
				g.terminate(fmt.Errorf("send: %w", err))
				return
			}
		}
	}
}

func (g *streamGeneration) sendRequest(outbound *outboundStreamRequest) error {
	if outbound.deadline.IsZero() {
		return g.stream.Send(outbound.request)
	}

	sent := make(chan struct{})
	timer := time.AfterFunc(max(time.Until(outbound.deadline), 0), func() {
		select {
		case <-sent:
			return
		default:
			g.terminate(fmt.Errorf("send deadline: %w", context.DeadlineExceeded))
		}
	})
	err := g.stream.Send(outbound.request)
	close(sent)
	timer.Stop()
	return err
}

func (g *streamGeneration) receive() {
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
		g.workers.Wait()
		close(g.closeDone)
	})
	<-g.closeDone
}
