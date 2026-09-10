package client1of1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
	"google.golang.org/grpc"
)

// ErrClientClosed is returned when an operation is attempted after Close.
var ErrClientClosed = errors.New("RedLease 1/1 client closed")

type serverUnavailableError struct {
	cause error
}

func (e *serverUnavailableError) Error() string {
	if e.cause == nil {
		return "server unavailable"
	}
	return "server unavailable: " + e.cause.Error()
}

func (e *serverUnavailableError) Unwrap() error {
	return e.cause
}

// Client owns one persistent reconnecting stream to one lock-server.
type Client struct {
	responseTimeout time.Duration
	logger          *slog.Logger
	idGenerator     *leaseIDGenerator

	connection *grpc.ClientConn
	rpc        redleasev1.RedLeaseClient

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

// New creates a client and starts connecting to its lock-server. It does not
// wait for the stream; callers that need a startup barrier can call WaitReady.
func New(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	idGenerator, err := newLeaseIDGenerator(config.ClientID)
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewClient(config.Target, config.DialOptions...)
	if err != nil {
		return nil, fmt.Errorf("create server connection: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		responseTimeout: defaultResponseTimeout,
		logger: config.Logger.With(
			slog.String("component", "redlease-client"),
			slog.Uint64("client_id", uint64(config.ClientID)),
			slog.String("server_target", config.Target),
		),
		idGenerator: idGenerator,
		connection:  connection,
		rpc:         redleasev1.NewRedLeaseClient(connection),
		ctx:         ctx,
		cancel:      cancel,
		changed:     make(chan struct{}),
	}
	if config.ResponseTimeout != 0 {
		client.responseTimeout = time.Duration(config.ResponseTimeout) * time.Millisecond
	}

	client.manager.Add(1)
	go client.manageStream()
	return client, nil
}

// WaitReady waits until the stream to the lock-server is connected. It does
// not wait for the server to leave restart quarantine.
func (c *Client) WaitReady(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.ctx.Err() != nil {
			return ErrClientClosed
		}

		c.stateMu.Lock()
		ready := c.generation != nil && !c.closed
		closed := c.closed
		changed := c.changed
		c.stateMu.Unlock()

		if ready {
			return nil
		}
		if closed {
			return ErrClientClosed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return ErrClientClosed
		case <-changed:
		}
	}
}

// Close stops reconnecting and releases all client-side resources. Server-side
// leases are not implicitly released and remain TTL bounded.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()

		c.stateMu.Lock()
		c.closed = true
		generation := c.generation
		c.generation = nil
		c.lastErr = ErrClientClosed
		c.notifyStateChangeLocked()
		c.stateMu.Unlock()

		if generation != nil {
			generation.Close()
		}
		c.manager.Wait()
		c.closeErr = c.connection.Close()
	})
	return c.closeErr
}

func (c *Client) submit(
	ctx context.Context,
	request *redleasev1.ClientRequest,
) (*streamFuture, error) {
	c.stateMu.Lock()
	generation := c.generation
	cause := c.lastErr
	if c.closed {
		generation = nil
		cause = ErrClientClosed
	}
	c.stateMu.Unlock()
	if generation == nil {
		return nil, &serverUnavailableError{cause: cause}
	}
	return generation.submit(ctx, request)
}

func (c *Client) operationContext(caller context.Context) (context.Context, func()) {
	ctx, cancel := context.WithTimeout(c.ctx, c.responseTimeout)
	stopCallerCancellation := context.AfterFunc(caller, cancel)
	return ctx, func() {
		stopCallerCancellation()
		cancel()
	}
}

func (c *Client) cancellationError(caller context.Context) error {
	if err := caller.Err(); err != nil {
		return err
	}
	if c.ctx.Err() != nil {
		return ErrClientClosed
	}
	return nil
}

func (c *Client) manageStream() {
	defer c.manager.Done()

	var attempt uint
	unavailable := false
	for {
		streamContext, cancelStream := context.WithCancel(c.ctx)
		stream, err := c.rpc.LeaseStream(streamContext)
		if err != nil {
			cancelStream()
			c.recordFailure(fmt.Errorf("open stream: %w", err))
			if !unavailable && c.ctx.Err() == nil {
				c.logger.Warn(
					"server stream unavailable",
					slog.String("reason", "connect_failed"),
					slog.Any("error", err),
				)
				unavailable = true
			}
			if !waitBackoff(c.ctx, reconnectBackoff(attempt)) {
				return
			}
			attempt++
			continue
		}

		generation := newStreamGeneration(stream, cancelStream)
		if !c.publish(generation) {
			generation.Close()
			return
		}
		c.logger.Info(
			"server stream connected",
			slog.Bool("reconnected", unavailable),
			slog.Uint64("attempt", uint64(attempt+1)),
		)
		unavailable = false
		attempt = 0

		select {
		case <-generation.done:
		case <-c.ctx.Done():
		}

		cause := generation.err()
		c.clear(generation, cause)
		generation.Close()
		if c.ctx.Err() != nil {
			return
		}
		c.logger.Warn(
			"server stream unavailable",
			slog.String("reason", "stream_terminated"),
			slog.Any("error", cause),
		)
		unavailable = true
		if !waitBackoff(c.ctx, reconnectBackoff(attempt)) {
			return
		}
		attempt++
	}
}

func (c *Client) publish(generation *streamGeneration) bool {
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

func (c *Client) clear(generation *streamGeneration, cause error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.generation != generation {
		return
	}
	c.generation = nil
	c.lastErr = cause
	c.notifyStateChangeLocked()
}

func (c *Client) recordFailure(cause error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if !c.closed {
		c.lastErr = cause
	}
}

func (c *Client) notifyStateChangeLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}
