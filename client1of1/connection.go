package client1of1

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

// ErrSendQueueFull means a request could not be queued for transmission.
// Callers may retry the operation after a delay.
var ErrSendQueueFull = errors.New("connection send queue full")

const pendingShardCount = 16

type connectionResult struct {
	response protocol.Response
	err      error
}

type pendingShard struct {
	mu      sync.Mutex
	pending map[uint64]chan connectionResult
}

func newPendingShards() [pendingShardCount]*pendingShard {
	var shards [pendingShardCount]*pendingShard
	for index := range shards {
		shards[index] = &pendingShard{pending: make(map[uint64]chan connectionResult)}
	}
	return shards
}

// Request IDs grow sequentially, so their low bits spread neighboring
// requests evenly across the configured shards without hashing.
func pendingShardIndex(requestID uint64) uint8 {
	return uint8(requestID & (pendingShardCount - 1)) //nolint:gosec // The mask bounds the index.
}

type connectionFuture struct {
	client    *Client
	requestID uint64
	result    chan connectionResult
}

type outboundConnectionRequest struct {
	request redleasev1.ClientRequest
	builder *flatbuffers.Builder
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
	shard := c.pending[pendingShardIndex(requestID)]
	shard.mu.Lock()
	if c.ctx.Err() != nil {
		shard.mu.Unlock()
		c.recycleOutboundRequest(request)
		return ErrClientClosed
	}
	if result != nil {
		shard.pending[requestID] = result
	}
	if c.sendQueue.TryEnqueue(request) {
		shard.mu.Unlock()
		return nil
	}
	delete(shard.pending, requestID)
	shard.mu.Unlock()
	c.recycleOutboundRequest(request)
	return ErrSendQueueFull
}

func (c *Client) complete(requestID uint64, result connectionResult) {
	shard := c.pending[pendingShardIndex(requestID)]
	shard.mu.Lock()
	pending := shard.pending[requestID]
	if pending != nil {
		delete(shard.pending, requestID)
	}
	shard.mu.Unlock()
	if pending != nil {
		pending <- result
	}
}

// runConnection owns exactly one reader and one writer. Both are joined before
// the manager starts another session, so the persistent queue has one consumer.
func (c *Client) runConnection(connection *transport.ClientConnection) error {
	stop := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		results <- c.send(connection.Writer, stop)
	}()
	go func() {
		defer workers.Done()
		results <- c.receive(connection.Reader)
	}()

	var cause error
	select {
	case cause = <-results:
	case <-c.ctx.Done():
		cause = ErrClientClosed
	}
	close(stop)
	_ = connection.Conn.Close()
	workers.Wait()
	return cause
}

func (c *Client) send(writer *transport.FrameWriter, stop <-chan struct{}) error {
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		outbound, ok := c.sendQueue.TryDequeue()
		if !ok {
			select {
			case <-stop:
				return nil
			case <-c.sendQueue.Ready():
			}
			continue
		}
		for {
			select {
			case <-stop:
				c.recycleOutboundRequest(outbound)
				return nil
			default:
			}
			err := writer.BufferFrame(outbound.request.Table().Bytes)
			c.recycleOutboundRequest(outbound)
			if err != nil {
				return fmt.Errorf("send: %w", err)
			}
			outbound, ok = c.sendQueue.TryDequeue()
			if ok {
				continue
			}
			if err := writer.Flush(); err != nil {
				return fmt.Errorf("flush send batch: %w", err)
			}
			break
		}
	}
}

func (c *Client) receive(reader *transport.FrameReader) error {
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}
		response, err := transport.DecodeResponse(frame)
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}
		c.complete(response.RequestID, connectionResult{response: response})
	}
}

func (c *Client) discardQueuedRequests() {
	for {
		request, ok := c.sendQueue.TryDequeue()
		if !ok {
			return
		}
		c.recycleOutboundRequest(request)
	}
}

func (c *Client) recycleOutboundRequest(request *outboundConnectionRequest) {
	request.request = redleasev1.ClientRequest{}
	c.requestPool.Put(request)
}
