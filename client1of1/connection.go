package client1of1

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

// ErrSendQueueFull means a request could not be queued for transmission.
// Callers may retry the operation after a delay.
var ErrSendQueueFull = errors.New("connection send queue full")

const (
	pendingShardCount       = 32
	clientRequestFrameBytes = 72
	// Builder.Prep needs at least nine unused leading bytes for this schema;
	// round the fixed backing buffer up to the next eight-byte boundary.
	clientRequestBufferBytes = clientRequestFrameBytes + 16
)

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
	buffer     [clientRequestBufferBytes]byte
	frameStart uint16
}

func (r *outboundConnectionRequest) frame() []byte {
	return r.buffer[r.frameStart:]
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
func (c *Client) submit(
	operation redleasev1.ClientOperation,
	key uint64,
	sequence uint64,
	ttlMS uint64,
) (*connectionFuture, error) {
	requestID := c.nextRequestID.Add(1)
	future := c.acquireFuture(requestID)
	if err := c.enqueue(requestID, operation, key, sequence, ttlMS, future.result); err != nil {
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

func (c *Client) submitNoResponse(
	key uint64,
	sequence uint64,
) error {
	return c.enqueue(
		c.nextRequestID.Add(1),
		redleasev1.ClientOperationRELEASE,
		key,
		sequence,
		0,
		nil,
	)
}

func (c *Client) enqueue(
	requestID uint64,
	operation redleasev1.ClientOperation,
	key uint64,
	sequence uint64,
	ttlMS uint64,
	result chan connectionResult,
) error {
	shard := c.pending[pendingShardIndex(requestID)]
	shard.mu.Lock()
	if c.ctx.Err() != nil {
		shard.mu.Unlock()
		return ErrClientClosed
	}
	if result != nil {
		shard.pending[requestID] = result
	}
	outbound, ticket := c.reqQueue.TryStartEnqueue()
	if outbound != nil {
		c.buildOutboundRequest(outbound, requestID, operation, key, sequence, ttlMS)
		c.reqQueue.FinishEnqueue(ticket)
		shard.mu.Unlock()
		return nil
	}
	delete(shard.pending, requestID)
	shard.mu.Unlock()
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
		results <- c.sendRequests(connection.Writer, stop)
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

func (c *Client) sendRequests(writer *transport.FrameWriter, stop <-chan struct{}) error {
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		outbound := c.reqQueue.TryStartDequeue()
		if outbound == nil {
			select {
			case <-stop:
				return nil
			case <-c.reqQueue.Ready():
			}
			continue
		}
		for {
			select {
			case <-stop:
				c.reqQueue.FinishDequeue()
				return nil
			default:
			}
			err := writer.BufferFrame(outbound.frame())
			c.reqQueue.FinishDequeue()
			if err != nil {
				return fmt.Errorf("send: %w", err)
			}
			outbound = c.reqQueue.TryStartDequeue()
			if outbound != nil {
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
		request := c.reqQueue.TryStartDequeue()
		if request == nil {
			return
		}
		c.reqQueue.FinishDequeue()
	}
}
