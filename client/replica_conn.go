package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
	"google.golang.org/grpc"
)

var errReplicaClosed = errors.New("replica connection closed")

type replicaUnavailableError struct {
	cause error
}

func (e *replicaUnavailableError) Error() string {
	if e.cause == nil {
		return "replica unavailable"
	}
	return "replica unavailable: " + e.cause.Error()
}

func (e *replicaUnavailableError) Unwrap() error {
	return e.cause
}

type streamFactory interface {
	open(context.Context) (leaseClientStream, error)
	close() error
}

type grpcStreamFactory struct {
	connection *grpc.ClientConn
	client     redleasev1.RedLeaseClient
}

func newGRPCStreamFactory(connection *grpc.ClientConn) *grpcStreamFactory {
	return &grpcStreamFactory{
		connection: connection,
		client:     redleasev1.NewRedLeaseClient(connection),
	}
}

func (f *grpcStreamFactory) open(ctx context.Context) (leaseClientStream, error) {
	return f.client.LeaseStream(ctx)
}

func (f *grpcStreamFactory) close() error {
	return f.connection.Close()
}

type replicaConnConfig struct {
	factory streamFactory
	backoff exponentialBackoff
	timer   backoffTimer
}

type replicaConn struct {
	factory streamFactory
	backoff exponentialBackoff
	timer   backoffTimer

	ctx    context.Context
	cancel context.CancelFunc

	stateMu    sync.Mutex
	generation *streamGeneration
	lastErr    error
	closed     bool
	changed    chan struct{}

	manager sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

func newReplicaConn(factory streamFactory) *replicaConn {
	return newReplicaConnWithConfig(replicaConnConfig{
		factory: factory,
		backoff: defaultReconnectBackoff(),
		timer:   systemBackoffTimer{},
	})
}

func newReplicaConnWithConfig(config replicaConnConfig) *replicaConn {
	ctx, cancel := context.WithCancel(context.Background())
	connection := &replicaConn{
		factory: config.factory,
		backoff: config.backoff,
		timer:   config.timer,
		ctx:     ctx,
		cancel:  cancel,
		changed: make(chan struct{}),
	}

	connection.manager.Add(1)
	go connection.manage()
	return connection
}

func (c *replicaConn) call(
	ctx context.Context,
	request *redleasev1.ClientRequest,
) (*redleasev1.ServerResponse, error) {
	c.stateMu.Lock()
	generation := c.generation
	cause := c.lastErr
	if c.closed {
		cause = errReplicaClosed
		generation = nil
	}
	c.stateMu.Unlock()

	if generation == nil {
		return nil, &replicaUnavailableError{cause: cause}
	}

	// A failed call is deliberately not retried on a newer generation: the
	// server may already have applied the operation before transport failure.
	return generation.call(ctx, request)
}

// readiness returns a level-triggered snapshot plus a channel closed on the
// next state change. A future Client.WaitReady can safely recheck in a loop.
func (c *replicaConn) readiness() (ready bool, closed bool, changed <-chan struct{}) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.generation != nil && !c.closed, c.closed, c.changed
}

func (c *replicaConn) Close() error {
	c.closeOnce.Do(func() {
		generation := c.markClosed()
		c.cancel()
		if generation != nil {
			_ = generation.Close()
		}
		c.manager.Wait()
		c.closeErr = c.factory.close()
	})
	return c.closeErr
}

func (c *replicaConn) manage() {
	defer c.manager.Done()

	var attempt uint
	for {
		streamContext, cancelStream := context.WithCancel(c.ctx)
		stream, err := c.factory.open(streamContext)
		if err != nil {
			cancelStream()
			c.recordFailure(fmt.Errorf("open stream: %w", err))
			if !c.waitBeforeRetry(attempt) {
				return
			}
			attempt++
			continue
		}

		generation := newStreamGeneration(stream, cancelStream)
		if !c.publish(generation) {
			_ = generation.Close()
			return
		}
		attempt = 0

		select {
		case <-generation.done:
		case <-c.ctx.Done():
		}

		cause := generation.err()
		c.clear(generation, cause)
		_ = generation.Close()
		if c.ctx.Err() != nil {
			return
		}

		if !c.waitBeforeRetry(attempt) {
			return
		}
		attempt++
	}
}

func (c *replicaConn) waitBeforeRetry(attempt uint) bool {
	return c.timer.wait(c.ctx, c.backoff.duration(attempt))
}

func (c *replicaConn) publish(generation *streamGeneration) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.closed {
		return false
	}
	c.generation = generation
	c.lastErr = nil
	c.notifyStateChangeLocked()
	return true
}

func (c *replicaConn) clear(generation *streamGeneration, cause error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.generation != generation {
		return
	}
	c.generation = nil
	c.lastErr = cause
	c.notifyStateChangeLocked()
}

func (c *replicaConn) recordFailure(cause error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if !c.closed {
		c.lastErr = cause
	}
}

func (c *replicaConn) markClosed() *streamGeneration {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.closed = true
	generation := c.generation
	c.generation = nil
	c.lastErr = errReplicaClosed
	c.notifyStateChangeLocked()
	return generation
}

func (c *replicaConn) notifyStateChangeLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}
