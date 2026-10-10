package client

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

var (
	errConnectionClosed = errors.New("connection generation closed")

	// ErrSendQueueFull means a request could not be queued for transmission.
	// Callers may retry the operation after a delay.
	ErrSendQueueFull = errors.New("connection send queue full")
)

const (
	reqQueueCapacity      = 4096
	pendingShardCount     = 16
	responseQueueCapacity = 512
)

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

type pendingShard struct {
	mu        sync.Mutex
	pending   map[uint64]*connectionFuture
	responses *mpscring.NotifyingRing[protocol.Response]
}

func newPendingShards() [pendingShardCount]*pendingShard {
	var shards [pendingShardCount]*pendingShard
	for index := range shards {
		shards[index] = &pendingShard{
			pending:   make(map[uint64]*connectionFuture),
			responses: mpscring.NewNotifying[protocol.Response](responseQueueCapacity),
		}
	}
	return shards
}

// Request IDs grow sequentially, so their low bits spread neighboring
// requests evenly across the configured shards without hashing.
func pendingShardIndex(requestID uint64) uint8 {
	return uint8(requestID & (pendingShardCount - 1)) //nolint:gosec // The mask bounds the index.
}

type connectionFuture struct {
	replica        *replicaConn
	requestID      uint64
	result         chan connectionCallResult
	operation      chan<- replicaResponse
	replicaIndex   int
	responseExpiry time.Time
}

type replicaResponse struct {
	replica int
	future  *connectionFuture
	result  connectionCallResult
}

func (f *connectionFuture) await(ctx context.Context) (protocol.Response, error) {
	return f.awaitUntil(ctx, nil)
}

func (f *connectionFuture) awaitUntil(
	ctx context.Context,
	timeout <-chan time.Time,
) (protocol.Response, error) {
	var result connectionCallResult
	var completionErr error
	select {
	case result = <-f.result:
	case <-ctx.Done():
		completionErr = ctx.Err()
	case <-timeout:
		completionErr = context.DeadlineExceeded
	}
	if completionErr != nil {
		f.replica.complete(f.requestID, connectionCallResult{err: completionErr})
		result = <-f.result
	}
	f.replica.client.releaseFuture(f)
	return result.response, result.err
}

func (c *Client) awaitResponse(
	ctx context.Context,
	future *connectionFuture,
) (protocol.Response, error) {
	timer, ok := c.responseTimerPool.Get().(*time.Timer)
	if ok {
		timer.Reset(c.responseTimeout)
	} else {
		timer = time.NewTimer(c.responseTimeout)
	}

	response, err := future.awaitUntil(ctx, timer.C)
	timer.Stop()
	c.responseTimerPool.Put(timer)
	return response, err
}

type outboundConnectionRequest struct {
	request redleasev1.ClientRequest
	builder *flatbuffers.Builder
}

func (c *Client) newOutboundRequest() *outboundConnectionRequest {
	if pooled := c.requestPool.Get(); pooled != nil {
		if outbound, ok := pooled.(*outboundConnectionRequest); ok {
			outbound.builder.Reset()
			return outbound
		}
	}
	return &outboundConnectionRequest{
		builder: flatbuffers.NewBuilder(transport.InitialBufferSize),
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

func (c *Client) recycleOutboundRequest(outbound *outboundConnectionRequest) {
	outbound.request = redleasev1.ClientRequest{}
	c.requestPool.Put(outbound)
}

func (c *Client) acquireFuture(replica *replicaConn, requestID uint64) *connectionFuture {
	if pooled := c.futurePool.Get(); pooled != nil {
		if future, ok := pooled.(*connectionFuture); ok {
			future.replica = replica
			future.requestID = requestID
			return future
		}
	}
	return &connectionFuture{
		replica:   replica,
		requestID: requestID,
		result:    make(chan connectionCallResult, 1),
	}
}

func (c *Client) acquireOperationFuture(
	replica *replicaConn,
	requestID uint64,
	replicaIndex int,
	operation chan<- replicaResponse,
) *connectionFuture {
	future := c.acquireFuture(replica, requestID)
	future.operation = operation
	future.replicaIndex = replicaIndex
	future.responseExpiry = time.Now().Add(c.responseTimeout)
	return future
}

func (c *Client) releaseFuture(future *connectionFuture) {
	future.replica = nil
	future.requestID = 0
	future.operation = nil
	future.replicaIndex = 0
	future.responseExpiry = time.Time{}
	c.futurePool.Put(future)
}

func (f *connectionFuture) complete(result connectionCallResult) {
	if f.operation != nil {
		f.operation <- replicaResponse{
			replica: f.replicaIndex,
			future:  f,
			result:  result,
		}
		return
	}
	f.result <- result
}

type connectionGeneration struct {
	replica    *replicaConn
	connection *transport.ClientConnection
	done       chan struct{}

	terminal atomic.Pointer[connectionTransportError]

	terminateOnce      sync.Once
	closeOnce          sync.Once
	workers            sync.WaitGroup
	closeConnectionErr error
}

func newConnectionGeneration(
	replica *replicaConn,
	connection *transport.ClientConnection,
) *connectionGeneration {
	generation := &connectionGeneration{
		replica:    replica,
		connection: connection,
		done:       make(chan struct{}),
	}

	generation.workers.Go(generation.sendRequests)
	generation.workers.Go(generation.receiveResponses)
	return generation
}

func (c *replicaConn) complete(requestID uint64, result connectionCallResult) {
	shard := c.pending[pendingShardIndex(requestID)]
	c.completeInShard(shard, requestID, result)
}

func (*replicaConn) completeInShard(
	shard *pendingShard,
	requestID uint64,
	result connectionCallResult,
) {
	shard.mu.Lock()
	pending := shard.pending[requestID]
	if pending != nil {
		delete(shard.pending, requestID)
	}
	shard.mu.Unlock()
	if pending != nil {
		pending.complete(result)
	}
}

func (c *replicaConn) completeResponses(shard *pendingShard) {
	for {
		response, ok := shard.responses.TryDequeue()
		if ok {
			c.completeInShard(shard, response.RequestID, connectionCallResult{response: response})
			continue
		}
		select {
		case <-c.ctx.Done():
			return
		case <-shard.responses.Ready():
		}
	}
}

func (g *connectionGeneration) sendRequests() {
	for {
		select {
		case <-g.done:
			return
		default:
		}
		outbound, ok := g.replica.reqQueue.TryDequeue()
		if !ok {
			select {
			case <-g.done:
				return
			case <-g.replica.reqQueue.Ready():
			}
			continue
		}
		for {
			select {
			case <-g.done:
				g.replica.client.recycleOutboundRequest(outbound)
				return
			default:
			}
			err := g.connection.Writer.BufferFrame(outbound.request.Table().Bytes)
			g.replica.client.recycleOutboundRequest(outbound)
			if err != nil {
				g.terminate(fmt.Errorf("send: %w", err))
				return
			}
			outbound, ok = g.replica.reqQueue.TryDequeue()
			if ok {
				continue
			}
			if err := g.connection.Writer.Flush(); err != nil {
				g.terminate(fmt.Errorf("flush send batch: %w", err))
				return
			}
			break
		}
	}
}

func (g *connectionGeneration) receiveResponses() {
	for {
		frame, err := g.connection.Reader.ReadFrame()
		if err != nil {
			g.terminate(fmt.Errorf("receive: %w", err))
			return
		}
		response, err := transport.DecodeResponse(frame)
		if err != nil {
			g.terminate(fmt.Errorf("receive: %w", err))
			return
		}
		shard := g.replica.pending[pendingShardIndex(response.RequestID)]
		for !shard.responses.TryEnqueue(response) {
			select {
			case <-g.done:
				return
			default:
				runtime.Gosched()
			}
		}
	}
}

func (g *connectionGeneration) Close() error {
	g.terminate(errConnectionClosed)
	g.workers.Wait()
	return g.closeConnectionErr
}

func (g *connectionGeneration) terminate(cause error) {
	g.terminateOnce.Do(func() {
		transportErr := &connectionTransportError{cause: cause}
		g.terminal.Store(transportErr)
		close(g.done)
		g.closeConnection()
	})
}

func (c *replicaConn) discardQueuedRequests() {
	for {
		outbound, ok := c.reqQueue.TryDequeue()
		if !ok {
			return
		}
		c.client.recycleOutboundRequest(outbound)
	}
}

func (c *replicaConn) failPending(err error) {
	result := connectionCallResult{err: err}
	for _, shard := range c.pending {
		shard.mu.Lock()
		pending := shard.pending
		shard.pending = nil
		shard.mu.Unlock()
		for _, future := range pending {
			future.complete(result)
		}
	}
}

func (g *connectionGeneration) closeConnection() {
	g.closeOnce.Do(func() {
		g.closeConnectionErr = g.connection.Conn.Close()
	})
}

func (g *connectionGeneration) err() error {
	if terminalErr := g.terminal.Load(); terminalErr != nil {
		return terminalErr
	}
	return nil
}
