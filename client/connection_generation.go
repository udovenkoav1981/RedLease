package client

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

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
	pending   map[uint64]chan<- replicaResponse
	responses *mpscring.NotifyingRing[protocol.Response]
}

func newPendingShards() [pendingShardCount]*pendingShard {
	var shards [pendingShardCount]*pendingShard
	for index := range shards {
		shards[index] = &pendingShard{
			pending:   make(map[uint64]chan<- replicaResponse),
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

type replicaResponse struct {
	replica   int
	requestID uint64
	result    connectionCallResult
}

// outboundConnectionRequest is an owned, unencoded request. The connection
// writer encodes it into its single reusable builder, so callers never hold
// FlatBuffers backing memory.
type outboundConnectionRequest struct {
	requestID uint64
	key       uint64
	sequence  uint64
	ttlMS     uint64
	clientID  uint32
	bootID    uint32
	operation redleasev1.ClientOperation
}

// encodeRequest serializes one request into the connection writer's reusable
// builder. The returned frame remains valid until the next Reset.
func encodeRequest(builder *flatbuffers.Builder, request *outboundConnectionRequest) []byte {
	builder.Reset()
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddRequestId(builder, request.requestID)
	redleasev1.ClientRequestAddOperation(builder, request.operation)
	switch request.operation {
	case redleasev1.ClientOperationACQUIRE:
		redleasev1.ClientRequestAddAcquire(builder, redleasev1.CreateAcquireRequest(
			builder,
			request.key,
			request.clientID,
			request.bootID,
			request.sequence,
			request.ttlMS,
		))
	case redleasev1.ClientOperationRENEW:
		redleasev1.ClientRequestAddRenew(builder, redleasev1.CreateRenewRequest(
			builder,
			request.key,
			request.clientID,
			request.bootID,
			request.sequence,
			request.ttlMS,
		))
	case redleasev1.ClientOperationRELEASE:
		redleasev1.ClientRequestAddRelease(builder, redleasev1.CreateReleaseRequest(
			builder,
			request.key,
			request.clientID,
			request.bootID,
			request.sequence,
		))
	case redleasev1.ClientOperationNONE, redleasev1.ClientOperationGET_TTL:
	}
	root := redleasev1.ClientRequestEnd(builder)
	redleasev1.FinishSizePrefixedClientRequestBuffer(builder, root)
	return builder.FinishedBytes()
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

func (c *replicaConn) completeInShard(
	shard *pendingShard,
	requestID uint64,
	result connectionCallResult,
) {
	shard.mu.Lock()
	pending, ok := shard.pending[requestID]
	if ok {
		delete(shard.pending, requestID)
	}
	shard.mu.Unlock()
	if ok {
		pending <- replicaResponse{
			replica:   c.index,
			requestID: requestID,
			result:    result,
		}
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
	builder := flatbuffers.NewBuilder(transport.InitialBufferSize)
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
				return
			default:
			}
			if err := g.connection.Writer.BufferFrame(encodeRequest(builder, &outbound)); err != nil {
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
		if _, ok := c.reqQueue.TryDequeue(); !ok {
			return
		}
	}
}

func (c *replicaConn) failPending(err error) {
	result := connectionCallResult{err: err}
	for _, shard := range c.pending {
		shard.mu.Lock()
		pending := shard.pending
		shard.pending = nil
		shard.mu.Unlock()
		for requestID, destination := range pending {
			destination <- replicaResponse{
				replica:   c.index,
				requestID: requestID,
				result:    result,
			}
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
