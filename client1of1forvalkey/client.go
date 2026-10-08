package client1of1forvalkey

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/udovenkoav1981/RedLease/internal/bootid"
)

var (
	// ErrClientClosed is returned when an operation is attempted after Close.
	ErrClientClosed = errors.New("RedLease Valkey 1/1 client closed")
	// ErrSendQueueFull is retained for API compatibility with client1of1.
	// The go-redis full-duplex queue applies backpressure instead of rejecting
	// an operation when its configured window is full.
	ErrSendQueueFull = errors.New("connection send queue full")
)

const (
	fullDuplexWindow    = 4096
	maxRequestBatchSize = 1024
	waitReadyRetryDelay = 50 * time.Millisecond
	pipelineBufferBytes = 64 * 1024
)

// Client owns one ordered go-redis full-duplex command stream to one
// standalone Valkey server.
type Client struct {
	clientID        uint32
	bootID          uint32
	nextSequence    atomic.Uint64
	responseTimeout time.Duration
	logger          *slog.Logger
	redis           *redis.Client
	pipeline        *redis.AutoPipeliner
	scripts         scriptHashes

	ctx    context.Context //nolint:containedctx // Client owns this lifecycle context.
	cancel context.CancelFunc
	closed atomic.Bool

	scriptMu         sync.Mutex
	scriptGeneration atomic.Uint64
	closeOnce        sync.Once
	closeErr         error
}

// New creates a client and starts the go-redis full-duplex command stream. As
// with client1of1.New, a temporarily unavailable server does not make
// construction fail; callers can use WaitReady as the startup barrier.
func New(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	bootID, err := bootid.NewBootID()
	if err != nil {
		return nil, err
	}

	responseTimeout := defaultResponseTimeout
	if config.ResponseTimeout != 0 {
		responseTimeout = time.Duration(config.ResponseTimeout) * time.Millisecond
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:                    config.Target,
		DialTimeout:             responseTimeout,
		ReadTimeout:             responseTimeout,
		WriteTimeout:            responseTimeout,
		ContextTimeoutEnabled:   true,
		MaxRetries:              -1,
		PoolSize:                1,
		PipelinePoolSize:        1,
		PipelineReadBufferSize:  pipelineBufferBytes,
		PipelineWriteBufferSize: pipelineBufferBytes,
	})
	pipeline, err := redisClient.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize:         maxRequestBatchSize,
		MaxConcurrentBatches: 1,
		FullDuplex:           true,
		FullDuplexWindow:     fullDuplexWindow,
		NumShards:            1,
	})
	if err != nil {
		_ = redisClient.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		clientID:        config.ClientID,
		bootID:          bootID,
		responseTimeout: responseTimeout,
		logger: config.Logger.With(
			slog.String("component", "redlease-client1of1forvalkey"),
			slog.Uint64("client_id", uint64(config.ClientID)),
			slog.String("server_target", config.Target),
		),
		redis:    redisClient,
		pipeline: pipeline,
		scripts: scriptHashes{
			acquire: redis.NewScript(acquireLua).Hash(),
			renew:   redis.NewScript(renewLua).Hash(),
			release: redis.NewScript(releaseLua).Hash(),
		},
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// WaitReady waits until Valkey answers PING and has all lease scripts loaded.
func (c *Client) WaitReady(ctx context.Context) error {
	for {
		if err := c.cancellationError(ctx); err != nil {
			return err
		}

		operationCtx, cancel := context.WithTimeout(ctx, c.responseTimeout)
		err := c.redis.Ping(operationCtx).Err()
		if err == nil {
			err = c.reloadScripts(operationCtx, c.scriptGeneration.Load())
		}
		cancel()
		if err == nil {
			return nil
		}
		if err := c.cancellationError(ctx); err != nil {
			return err
		}

		timer := time.NewTimer(waitReadyRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-c.ctx.Done():
			timer.Stop()
			return ErrClientClosed
		case <-timer.C:
		}
	}
}

// Close stops request processing and closes all go-redis connections.
// Server-side leases are not implicitly released and remain TTL bounded.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.cancel()
		c.closeErr = c.redis.Close()
	})
	return c.closeErr
}

func (c *Client) cancellationError(caller context.Context) error {
	if err := caller.Err(); err != nil {
		return err
	}
	if c.closed.Load() {
		return ErrClientClosed
	}
	return nil
}

func (c *Client) operationError(caller context.Context, err error) error {
	if callerErr := caller.Err(); callerErr != nil {
		return callerErr
	}
	if c.closed.Load() || errors.Is(err, redis.ErrClosed) {
		return ErrClientClosed
	}
	return err
}
