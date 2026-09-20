package client1of1

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/backoff"
	"github.com/udovenkoav1981/RedLease/internal/leaseid"
	"github.com/udovenkoav1981/RedLease/internal/transport"
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

// Client owns one persistent reconnecting TCP connection to one lock-server.
type Client struct {
	clientID        uint32
	bootID          uint32
	nextSequence    atomic.Uint64
	nextRequestID   atomic.Uint64
	responseTimeout time.Duration
	logger          *slog.Logger
	target          string

	ctx    context.Context //nolint:containedctx // Client owns this lifecycle context.
	cancel context.CancelFunc

	stateMu    sync.Mutex
	generation *connectionGeneration
	lastErr    error
	closed     bool
	changed    chan struct{}

	manager sync.WaitGroup

	closeOnce sync.Once
	closeErr  error

	requestPool       sync.Pool
	responseTimerPool sync.Pool
}

// New creates a client and starts connecting to its lock-server. It does not
// wait for the connection; callers that need a startup barrier can call
// WaitReady.
func New(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	bootID, err := leaseid.NewBootID()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		clientID:        config.ClientID,
		bootID:          bootID,
		responseTimeout: defaultResponseTimeout,
		target:          config.Target,
		logger: config.Logger.With(
			slog.String("component", "redlease-client"),
			slog.Uint64("client_id", uint64(config.ClientID)),
			slog.String("server_target", config.Target),
		),
		ctx:     ctx,
		cancel:  cancel,
		changed: make(chan struct{}),
	}
	if config.ResponseTimeout != 0 {
		client.responseTimeout = time.Duration(config.ResponseTimeout) * time.Millisecond
	}

	client.manager.Add(1)
	go client.manageConnection()
	return client, nil
}

// WaitReady waits until the TCP connection to the lock-server is established. It does
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
	})
	return c.closeErr
}

func (c *Client) submit(
	request *outboundConnectionRequest,
) (*connectionFuture, error) {
	generation, err := c.currentGeneration()
	if err != nil {
		request.recycle()
		return nil, err
	}
	return generation.submit(request)
}

func (c *Client) submitNoResponse(
	request *outboundConnectionRequest,
) error {
	generation, err := c.currentGeneration()
	if err != nil {
		request.recycle()
		return err
	}
	return generation.submitNoResponse(request)
}

func (c *Client) currentGeneration() (*connectionGeneration, error) {
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
	return generation, nil
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

func (c *Client) manageConnection() {
	defer c.manager.Done()

	var attempt uint
	unavailable := false
	retryBackoff := backoff.Default()
	for {
		connection, err := transport.Dial(c.ctx, c.target)
		if err != nil {
			c.recordFailure(fmt.Errorf("open connection: %w", err))
			if !unavailable && c.ctx.Err() == nil {
				c.logger.Warn(
					"server connection unavailable",
					slog.String("reason", "connect_failed"),
					slog.Any("error", err),
				)
				unavailable = true
			}
			if !backoff.Wait(c.ctx, retryBackoff.Duration(attempt)) {
				return
			}
			attempt++
			continue
		}

		generation := newConnectionGeneration(connection, func() {})
		if !c.publish(generation) {
			generation.Close()
			return
		}
		c.logger.Info(
			"server connection established",
			slog.Bool("reconnected", unavailable),
			slog.Uint64("attempt", uint64(attempt+1)),
		)
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
			"server connection unavailable",
			slog.String("reason", "connection_terminated"),
			slog.Any("error", cause),
		)
		unavailable = true
		if !backoff.Wait(c.ctx, retryBackoff.Duration(attempt)) {
			return
		}
		attempt++
	}
}

func (c *Client) publish(generation *connectionGeneration) bool {
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

func (c *Client) clear(generation *connectionGeneration, cause error) {
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
