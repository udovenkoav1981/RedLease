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
	pending   map[uint64]chan connectionCallResult
	responses *mpscring.NotifyingRing[protocol.Response]
}

func newPendingShards() [pendingShardCount]*pendingShard {
	var shards [pendingShardCount]*pendingShard
	for index := range shards {
		shards[index] = &pendingShard{
			pending:   make(map[uint64]chan connectionCallResult),
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
	generation *connectionGeneration
	requestID  uint64
	result     chan connectionCallResult
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
		f.generation.complete(f.requestID, connectionCallResult{err: completionErr})
		result = <-f.result
	}
	f.generation.client.releaseFuture(f)
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

func (c *Client) acquireFuture(generation *connectionGeneration, requestID uint64) *connectionFuture {
	if pooled := c.futurePool.Get(); pooled != nil {
		if future, ok := pooled.(*connectionFuture); ok {
			future.generation = generation
			future.requestID = requestID
			return future
		}
	}
	return &connectionFuture{
		generation: generation,
		requestID:  requestID,
		result:     make(chan connectionCallResult, 1),
	}
}

func (c *Client) releaseFuture(future *connectionFuture) {
	future.generation = nil
	future.requestID = 0
	c.futurePool.Put(future)
}

type connectionGeneration struct {
	client     *Client
	connection *transport.ClientConnection
	cancel     context.CancelFunc

	reqQueue *mpscring.NotifyingRing[*outboundConnectionRequest]
	pending  [pendingShardCount]*pendingShard
	done     chan struct{}

	terminal atomic.Pointer[connectionTransportError]

	terminateOnce      sync.Once
	closeOnce          sync.Once
	workers            sync.WaitGroup
	closeConnectionErr error
}

func newConnectionGeneration(
	client *Client,
	connection *transport.ClientConnection,
	cancel context.CancelFunc,
) *connectionGeneration {
	generation := &connectionGeneration{
		client:     client,
		connection: connection,
		cancel:     cancel,
		reqQueue:   mpscring.NewNotifying[*outboundConnectionRequest](reqQueueCapacity),
		pending:    newPendingShards(),
		done:       make(chan struct{}),
	}

	for _, shard := range generation.pending {
		generation.workers.Go(func() {
			generation.completeResponses(shard)
		})
	}
	generation.workers.Go(generation.sendRequests)
	generation.workers.Go(generation.receiveResponses)
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

// submit registers the waiter before putting the request into the bounded
// connection FIFO. Successful enqueue is the submission ordering barrier.
func (g *connectionGeneration) submit(
	ctx context.Context,
	outbound *outboundConnectionRequest,
) (*connectionFuture, error) {
	if err := ctx.Err(); err != nil {
		g.client.recycleOutboundRequest(outbound)
		return nil, err
	}

	requestID := outbound.request.RequestId()
	future := g.client.acquireFuture(g, requestID)
	shard := g.pending[pendingShardIndex(requestID)]
	shard.mu.Lock()
	if terminalErr := g.terminal.Load(); terminalErr != nil {
		shard.mu.Unlock()
		g.client.releaseFuture(future)
		g.client.recycleOutboundRequest(outbound)
		return nil, terminalErr
	}
	shard.pending[requestID] = future.result
	if g.reqQueue.TryEnqueue(outbound) {
		shard.mu.Unlock()
		return future, nil
	}
	delete(shard.pending, requestID)
	shard.mu.Unlock()
	g.client.releaseFuture(future)
	g.client.recycleOutboundRequest(outbound)
	return nil, ErrSendQueueFull
}

func (g *connectionGeneration) complete(requestID uint64, result connectionCallResult) {
	shard := g.pending[pendingShardIndex(requestID)]
	g.completeInShard(shard, requestID, result)
}

func (*connectionGeneration) completeInShard(
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
		pending <- result
	}
}

func (g *connectionGeneration) completeResponses(shard *pendingShard) {
	for {
		response, ok := shard.responses.TryDequeue()
		if ok {
			g.completeInShard(shard, response.RequestID, connectionCallResult{response: response})
			continue
		}
		select {
		case <-g.done:
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
		outbound, ok := g.reqQueue.TryDequeue()
		if !ok {
			select {
			case <-g.done:
				return
			case <-g.reqQueue.Ready():
			}
			continue
		}
		for {
			select {
			case <-g.done:
				g.client.recycleOutboundRequest(outbound)
				return
			default:
			}
			err := g.connection.Writer.BufferFrame(outbound.request.Table().Bytes)
			g.client.recycleOutboundRequest(outbound)
			if err != nil {
				g.terminate(fmt.Errorf("send: %w", err))
				return
			}
			outbound, ok = g.reqQueue.TryDequeue()
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
		shard := g.pending[pendingShardIndex(response.RequestID)]
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
	g.discardQueuedRequests()
	return g.closeConnectionErr
}

func (g *connectionGeneration) terminate(cause error) {
	g.terminateOnce.Do(func() {
		transportErr := &connectionTransportError{cause: cause}
		g.terminal.Store(transportErr)
		close(g.done)
		g.cancel()
		g.closeConnection()

		result := connectionCallResult{err: transportErr}
		for _, shard := range g.pending {
			shard.mu.Lock()
			pending := shard.pending
			shard.pending = nil
			shard.mu.Unlock()
			for _, completion := range pending {
				completion <- result
			}
		}
	})
}

func (g *connectionGeneration) discardQueuedRequests() {
	for {
		outbound, ok := g.reqQueue.TryDequeue()
		if !ok {
			return
		}
		g.client.recycleOutboundRequest(outbound)
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
