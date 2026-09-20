package client1of1

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

var (
	errConnectionClosed = errors.New("connection closed")
	errSendQueueFull    = errors.New("connection send queue full")
)

const sendQueueCapacity = 256

type leaseConnection interface {
	SendClientRequest(request *redleasev1.ClientRequest) error
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

type connectionResult struct {
	response protocol.Response
	err      error
}

type connectionFuture struct {
	generation *connectionGeneration
	requestID  uint64
	result     chan connectionResult
}

type outboundConnectionRequest struct {
	request redleasev1.ClientRequest
	builder *flatbuffers.Builder
	pool    *sync.Pool
}

func (f *connectionFuture) await(
	ctx context.Context,
	timeout <-chan time.Time,
) (protocol.Response, error) {
	var result connectionResult
	select {
	case result = <-f.result:
	case <-ctx.Done():
		f.generation.complete(f.requestID, connectionResult{err: ctx.Err()})
		result = <-f.result
	case <-timeout:
		f.generation.complete(f.requestID, connectionResult{err: context.DeadlineExceeded})
		result = <-f.result
	}
	f.generation.releaseFuture(f)
	return result.response, result.err
}

func (c *Client) awaitResponse(
	ctx context.Context,
	future *connectionFuture,
) (protocol.Response, error) {
	timer := c.acquireResponseTimer()
	response, err := future.await(ctx, timer.C)
	c.releaseResponseTimer(timer)
	return response, err
}

func (c *Client) acquireResponseTimer() *time.Timer {
	if pooled := c.responseTimerPool.Get(); pooled != nil {
		if timer, ok := pooled.(*time.Timer); ok {
			timer.Reset(c.responseTimeout)
			return timer
		}
	}
	return time.NewTimer(c.responseTimeout)
}

func (c *Client) releaseResponseTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	c.responseTimerPool.Put(timer)
}

type connectionGeneration struct {
	connection leaseConnection
	cancel     context.CancelFunc

	sendQueue chan *outboundConnectionRequest
	done      chan struct{}

	stateMu     sync.Mutex
	pending     map[uint64]chan connectionResult
	terminalErr error
	futurePool  sync.Pool

	terminateOnce       sync.Once
	workers             sync.WaitGroup
	closeOnce           sync.Once
	connectionCloseOnce sync.Once
	closeDone           chan struct{}
}

func newConnectionGeneration(
	connection leaseConnection,
	cancel context.CancelFunc,
) *connectionGeneration {
	generation := &connectionGeneration{
		connection: connection,
		cancel:     cancel,
		sendQueue:  make(chan *outboundConnectionRequest, sendQueueCapacity),
		done:       make(chan struct{}),
		pending:    make(map[uint64]chan connectionResult),
		closeDone:  make(chan struct{}),
	}
	generation.workers.Add(2)
	go generation.send()
	go generation.receive()
	return generation
}

// submit returns after the request has entered the FIFO send queue. This is
// the ordering barrier used before a cleanup Release is submitted after an
// ambiguous mutation.
func (g *connectionGeneration) submit(
	request *outboundConnectionRequest,
) (*connectionFuture, error) {
	future := g.acquireFuture(request.request.RequestId())
	requestID, err := g.enqueue(request, future.result)
	if err != nil {
		g.releaseFuture(future)
		return nil, err
	}
	future.requestID = requestID
	return future, nil
}

func (g *connectionGeneration) acquireFuture(requestID uint64) *connectionFuture {
	if pooled := g.futurePool.Get(); pooled != nil {
		if future, ok := pooled.(*connectionFuture); ok {
			future.generation = g
			future.requestID = requestID
			return future
		}
	}
	return &connectionFuture{
		generation: g,
		requestID:  requestID,
		result:     make(chan connectionResult, 1),
	}
}

func (g *connectionGeneration) releaseFuture(future *connectionFuture) {
	future.generation = nil
	future.requestID = 0
	g.futurePool.Put(future)
}

func (g *connectionGeneration) submitNoResponse(
	request *outboundConnectionRequest,
) error {
	_, err := g.enqueue(request, nil)
	return err
}

func (g *connectionGeneration) enqueue(
	request *outboundConnectionRequest,
	result chan connectionResult,
) (uint64, error) {
	requestID := request.request.RequestId()

	g.stateMu.Lock()
	if g.terminalErr != nil {
		err := g.terminalErr
		g.stateMu.Unlock()
		request.recycle()
		return 0, err
	}
	if result != nil {
		g.pending[requestID] = result
	}
	select {
	case g.sendQueue <- request:
		g.stateMu.Unlock()
		return requestID, nil
	default:
		delete(g.pending, requestID)
		g.stateMu.Unlock()
		request.recycle()
		return 0, errSendQueueFull
	}
}

func (g *connectionGeneration) complete(requestID uint64, result connectionResult) {
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

func (g *connectionGeneration) send() {
	defer g.workers.Done()

	for {
		select {
		case <-g.done:
			return
		case outbound := <-g.sendQueue:
			if err := g.err(); err != nil {
				outbound.recycle()
				return
			}
			err := g.connection.SendClientRequest(&outbound.request)
			outbound.recycle()
			if err != nil {
				g.terminate(fmt.Errorf("send: %w", err))
				return
			}
		}
	}
}

func (r *outboundConnectionRequest) recycle() {
	r.request = redleasev1.ClientRequest{}
	r.pool.Put(r)
}

func (g *connectionGeneration) receive() {
	defer g.workers.Done()
	for {
		response, err := g.connection.Recv()
		if err != nil {
			g.terminate(fmt.Errorf("receive: %w", err))
			return
		}
		g.complete(response.RequestID, connectionResult{response: response})
	}
}

func (g *connectionGeneration) terminate(cause error) {
	g.terminateOnce.Do(func() {
		transportErr := &connectionTransportError{cause: cause}

		g.stateMu.Lock()
		g.terminalErr = transportErr
		pending := g.pending
		g.pending = make(map[uint64]chan connectionResult)
		g.stateMu.Unlock()

		close(g.done)
		g.cancel()
		g.closeConnection()
		g.discardQueuedRequests()
		result := connectionResult{err: transportErr}
		for _, call := range pending {
			call <- result
		}
	})
}

func (g *connectionGeneration) discardQueuedRequests() {
	for {
		select {
		case request := <-g.sendQueue:
			request.recycle()
		default:
			return
		}
	}
}

func (g *connectionGeneration) closeConnection() {
	g.connectionCloseOnce.Do(func() {
		_ = g.connection.Close()
	})
}

func (g *connectionGeneration) err() error {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	return g.terminalErr
}

func (g *connectionGeneration) Close() {
	g.closeOnce.Do(func() {
		g.terminate(errConnectionClosed)
		g.workers.Wait()
		close(g.closeDone)
	})
	<-g.closeDone
}
