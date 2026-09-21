package client1of1

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

var errSendQueueFull = errors.New("connection send queue full")

const sendQueueCapacity = 4096

type connectionResult struct {
	response protocol.Response
	err      error
}

type connectionFuture struct {
	client    *Client
	requestID uint64
	result    chan connectionResult
}

type outboundConnectionRequest struct {
	request redleasev1.ClientRequest
	builder *flatbuffers.Builder
	pool    *sync.Pool
}

func (f *connectionFuture) await(ctx context.Context, timeout <-chan time.Time) (protocol.Response, error) {
	var result connectionResult
	select {
	case result = <-f.result:
	case <-ctx.Done():
		f.client.complete(f.requestID, connectionResult{err: ctx.Err()})
		result = <-f.result
	case <-timeout:
		f.client.complete(f.requestID, connectionResult{err: context.DeadlineExceeded})
		result = <-f.result
	}
	f.client.releaseFuture(f)
	return result.response, result.err
}

func (c *Client) awaitResponse(ctx context.Context, future *connectionFuture) (protocol.Response, error) {
	timer, ok := c.responseTimerPool.Get().(*time.Timer)
	if ok {
		timer.Reset(c.responseTimeout)
	} else {
		timer = time.NewTimer(c.responseTimeout)
	}

	response, err := future.await(ctx, timer.C)

	timer.Stop()
	c.responseTimerPool.Put(timer)
	return response, err
}

// submit registers the waiter before putting the request into the persistent
// FIFO. The queue insertion is the ordering barrier for a cleanup Release.
func (c *Client) submit(request *outboundConnectionRequest) (*connectionFuture, error) {
	future := c.acquireFuture(request.request.RequestId())
	if err := c.enqueue(request, future.result); err != nil {
		c.releaseFuture(future)
		return nil, err
	}
	return future, nil
}

func (c *Client) acquireFuture(requestID uint64) *connectionFuture {
	if pooled := c.futurePool.Get(); pooled != nil {
		if future, ok := pooled.(*connectionFuture); ok {
			future.client = c
			future.requestID = requestID
			return future
		}
	}
	return &connectionFuture{
		client:    c,
		requestID: requestID,
		result:    make(chan connectionResult, 1),
	}
}

func (c *Client) releaseFuture(future *connectionFuture) {
	future.client = nil
	future.requestID = 0
	c.futurePool.Put(future)
}

func (c *Client) submitNoResponse(request *outboundConnectionRequest) error {
	return c.enqueue(request, nil)
}

func (c *Client) enqueue(request *outboundConnectionRequest, result chan connectionResult) error {
	requestID := request.request.RequestId()
	c.pendingMu.Lock()
	if c.ctx.Err() != nil {
		c.pendingMu.Unlock()
		request.recycle()
		return ErrClientClosed
	}
	if result != nil {
		c.pending[requestID] = result
	}
	select {
	case c.sendQueue <- request:
		c.pendingMu.Unlock()
		return nil
	default:
		delete(c.pending, requestID)
		c.pendingMu.Unlock()
		request.recycle()
		return errSendQueueFull
	}
}

func (c *Client) complete(requestID uint64, result connectionResult) {
	c.pendingMu.Lock()
	pending := c.pending[requestID]
	if pending != nil {
		delete(c.pending, requestID)
	}
	c.pendingMu.Unlock()
	if pending != nil {
		pending <- result
	}
}

// runConnection owns exactly one reader and one writer. Both are joined before
// the manager starts another session, so the persistent queue has one consumer.
func (c *Client) runConnection(connection transport.LeaseConnection) error {
	stop := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		results <- c.send(connection, stop)
	}()
	go func() {
		defer workers.Done()
		results <- c.receive(connection)
	}()

	var cause error
	select {
	case cause = <-results:
	case <-c.ctx.Done():
		cause = ErrClientClosed
	}
	close(stop)
	_ = connection.Close()
	workers.Wait()
	return cause
}

func (c *Client) send(connection transport.LeaseConnection, stop <-chan struct{}) error {
	for {
		var outbound *outboundConnectionRequest
		select {
		case <-stop:
			return nil
		case <-c.ctx.Done():
			return ErrClientClosed
		case outbound = <-c.sendQueue:
		}
		for {
			select {
			case <-stop:
				outbound.recycle()
				return nil
			default:
			}
			err := connection.BufferClientRequest(&outbound.request)
			outbound.recycle()
			if err != nil {
				return fmt.Errorf("send: %w", err)
			}
			select {
			case <-stop:
				return nil
			case <-c.ctx.Done():
				return ErrClientClosed
			case outbound = <-c.sendQueue:
				continue
			default:
			}
			if err := connection.FlushClientRequests(); err != nil {
				return fmt.Errorf("flush send batch: %w", err)
			}
			break
		}
	}
}

func (c *Client) receive(connection transport.LeaseConnection) error {
	for {
		response, err := connection.Recv()
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}
		c.complete(response.RequestID, connectionResult{response: response})
	}
}

func (c *Client) discardQueuedRequests() {
	for {
		select {
		case request := <-c.sendQueue:
			request.recycle()
		default:
			return
		}
	}
}

func (r *outboundConnectionRequest) recycle() {
	r.request = redleasev1.ClientRequest{}
	r.pool.Put(r)
}
