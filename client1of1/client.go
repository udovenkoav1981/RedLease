package client1of1

import (
	"context"
	"errors"
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
	connection transport.LeaseConnection
	closed     bool
	changed    chan struct{}

	sendQueue  chan *outboundConnectionRequest
	pendingMu  sync.Mutex
	pending    map[uint64]chan connectionResult
	futurePool sync.Pool

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
		ctx:       ctx,
		cancel:    cancel,
		changed:   make(chan struct{}),
		sendQueue: make(chan *outboundConnectionRequest, sendQueueCapacity),
		pending:   make(map[uint64]chan connectionResult),
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
		ready := c.connection != nil && !c.closed
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
		connection := c.connection
		c.connection = nil
		c.notifyStateChangeLocked()
		c.stateMu.Unlock()

		c.pendingMu.Lock()
		pending := c.pending
		c.pending = make(map[uint64]chan connectionResult)
		c.pendingMu.Unlock()
		for _, result := range pending {
			result <- connectionResult{err: ErrClientClosed}
		}
		if connection != nil {
			_ = connection.Close()
		}
		c.manager.Wait()
		c.discardQueuedRequests()
	})
	return c.closeErr
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

		if !c.publish(connection) {
			_ = connection.Close()
			return
		}
		c.logger.Info(
			"server connection established",
			slog.Bool("reconnected", unavailable),
			slog.Uint64("attempt", uint64(attempt+1)),
		)
		attempt = 0

		cause := c.runConnection(connection)
		c.clear(connection)
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

func (c *Client) publish(connection transport.LeaseConnection) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.closed || c.ctx.Err() != nil {
		return false
	}
	c.connection = connection
	c.notifyStateChangeLocked()
	return true
}

func (c *Client) clear(connection transport.LeaseConnection) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.connection != connection {
		return
	}
	c.connection = nil
	c.notifyStateChangeLocked()
}

func (c *Client) notifyStateChangeLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}
