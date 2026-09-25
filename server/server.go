// Package server implements the RedLease lock-server library.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

const (
	// ProtocolMaxTTL is the maximum configured TTL allowed by the protocol.
	ProtocolMaxTTL = time.Duration(protocol.MaxTTLMS) * time.Millisecond
	// RestartQuarantineDuration is the minimum delay enforced by the built-in
	// restart quarantine. An embedded owner which skips that quarantine assumes
	// responsibility for enforcing the same delay when prior RAM state may have
	// been lost.
	RestartQuarantineDuration = ProtocolMaxTTL + safetyMargin + time.Millisecond
	// DefaultMaxKeys is the default maximum number of resident lease keys.
	DefaultMaxKeys = 10_000

	safetyMargin = 100 * time.Millisecond

	defaultShardCount           = 256
	shardQueueCapacity          = 16
	expiredLeaseCleanupInterval = time.Minute
)

// ErrServerFailed identifies a fatal internal server error. The affected
// Server has stopped accepting work and cannot be returned to service.
var ErrServerFailed = errors.New("RedLease server failed")

// Config controls one in-memory lock-server instance. MaxTTL is measured in
// milliseconds. Zero values for MaxKeys and ShardCount select implementation
// defaults.
type Config struct {
	MaxTTL  uint64
	MaxKeys uint64

	// Logger receives structured operational events from the Server. It is
	// required. The caller retains ownership of the logger and its handler.
	Logger *slog.Logger

	// SkipRestartQuarantine starts the Server in ACTIVE state without a
	// quarantine timer. The embedding application then owns restart safety and
	// must ensure that RestartQuarantineDuration has elapsed whenever prior
	// in-memory lease state may have been lost.
	SkipRestartQuarantine bool

	ShardCount uint32
}

// Validate checks values explicitly supplied by the caller.
func (c Config) Validate() error {
	switch {
	case c.Logger == nil:
		return errors.New("logger must not be nil")
	case c.MaxTTL == 0:
		return errors.New("max TTL must be positive")
	case c.MaxTTL > uint64(ProtocolMaxTTL/time.Millisecond):
		return fmt.Errorf("max TTL must not exceed %s", ProtocolMaxTTL)
	default:
		return nil
	}
}

func resolveConfig(c Config) (Config, error) {
	if err := c.Validate(); err != nil {
		return Config{}, err
	}

	if c.ShardCount == 0 {
		c.ShardCount = defaultShardCount
	}
	if c.MaxKeys == 0 {
		c.MaxKeys = DefaultMaxKeys
	}
	return c, nil
}

type serverPhase uint32

const (
	phaseQuarantine serverPhase = iota
	phaseActive
	phaseFailed
	phaseClosed
)

// Server is an in-memory RedLease TCP server. New servers initially reject all
// lease mutations while the restart quarantine timer is running.
type Server struct {
	config Config
	logger *slog.Logger

	phase atomic.Uint32
	keys  atomic.Uint64

	activeConnections atomic.Int64
	operationTotals   [operationKindCount]atomic.Uint64
	responsePool      sync.Pool

	ctx    context.Context //nolint:containedctx // Server owns connections and workers lifecycle.
	cancel context.CancelFunc
	timer  *time.Timer
	fatal  chan error

	shards []*leaseShard

	// cleanupMu prevents concurrent capacity-triggered scans of all shards.
	cleanupMu    sync.Mutex
	connectionMu sync.Mutex
	listener     net.Listener
	connections  map[net.Conn]struct{}
	connectionWG sync.WaitGroup
	failOnce     sync.Once
	closeOnce    sync.Once
	wg           sync.WaitGroup
}

// New constructs a lock-server and starts accepting connections from listener.
// The Server owns listener after New succeeds and closes it during Close or a
// fatal failure. By default the Server starts in restart quarantine;
// SkipRestartQuarantine makes it immediately active under owner-managed restart
// safety.
func New(listener net.Listener, c Config) (*Server, error) {
	if listener == nil {
		return nil, errors.New("listener must not be nil")
	}
	config, err := resolveConfig(c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		config:      config,
		logger:      config.Logger.With(slog.String("component", "redlease-server")),
		ctx:         ctx,
		cancel:      cancel,
		fatal:       make(chan error, 1),
		shards:      make([]*leaseShard, config.ShardCount),
		listener:    listener,
		connections: make(map[net.Conn]struct{}),
	}
	if config.SkipRestartQuarantine {
		s.phase.Store(uint32(phaseActive))
	} else {
		s.phase.Store(uint32(phaseQuarantine))
	}

	for i := range s.shards {
		shard := &leaseShard{
			leases:     make(map[uint64]*lease),
			operations: mpscring.NewNotifying[operation](shardQueueCapacity),
			stop:       make(chan struct{}),
		}
		s.shards[i] = shard
		s.wg.Add(1)
		go s.runShard(shard)
	}
	s.wg.Add(1)
	go s.runExpiredLeaseCleanup()

	if !config.SkipRestartQuarantine {
		s.timer = time.NewTimer(RestartQuarantineDuration)
		s.wg.Add(1)
		go s.runQuarantine()
	}

	startAttrs := []slog.Attr{
		slog.Uint64("max_ttl_ms", config.MaxTTL),
		slog.Uint64("max_keys", config.MaxKeys),
		slog.Uint64("shard_count", uint64(config.ShardCount)),
	}
	if config.SkipRestartQuarantine {
		startAttrs = append(startAttrs, slog.String("state", "ACTIVE"))
		s.logger.LogAttrs(
			context.Background(),
			slog.LevelWarn,
			"server started with restart quarantine skipped",
			startAttrs...,
		)
	} else {
		startAttrs = append(
			startAttrs,
			slog.String("state", "QUARANTINE"),
			slog.Uint64("restart_quarantine_ms", uint64(RestartQuarantineDuration/time.Millisecond)),
		)
		s.logger.LogAttrs(context.Background(), slog.LevelInfo, "server started", startAttrs...)
	}
	s.wg.Add(1)
	go s.runListener(listener)

	return s, nil
}

// Fatal returns a channel which receives exactly one non-nil error if the
// server detects an unrecoverable failure. Detection moves the server to FAILED
// and cancels its active connections. The channel is buffered so failure detection
// never waits for the owner, and normal Close does not send to it.
// A failed Server cannot be returned to service and must be closed.
func (s *Server) Fatal() <-chan error {
	return s.fatal
}

func (s *Server) fail(cause error) {
	s.failOnce.Do(func() {
		failure := fmt.Errorf("%w: %w", ErrServerFailed, cause)
		for {
			phase := serverPhase(s.phase.Load())
			if phase == phaseClosed {
				return
			}
			if s.phase.CompareAndSwap(uint32(phase), uint32(phaseFailed)) {
				break
			}
		}
		s.cancel()
		s.closeConnections()
		s.fatal <- failure
		s.logger.Error(
			"server entered failed state",
			slog.String("state", "FAILED"),
			slog.Any("error", failure),
		)
	})
}

// Close stops accepting work, closes active connections and drains work
// already submitted to the shard queues. It is safe to call Close more than
// once.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.phase.Store(uint32(phaseClosed))
		s.cancel()
		s.closeConnections()
		s.connectionWG.Wait()

		// runConnection does not finish until its request reader has stopped, so
		// no dispatch producer remains after connectionWG.Wait.
		for _, shard := range s.shards {
			close(shard.stop)
		}

		s.wg.Wait()
		s.logger.Info("server stopped", slog.String("state", "CLOSED"))
	})
	return nil
}

func (s *Server) runQuarantine() {
	defer s.wg.Done()
	defer s.timer.Stop()

	select {
	case <-s.timer.C:
		if s.phase.CompareAndSwap(uint32(phaseQuarantine), uint32(phaseActive)) {
			s.logger.Info("server entered active state", slog.String("state", "ACTIVE"))
		}
	case <-s.ctx.Done():
	}
}

func (s *Server) runExpiredLeaseCleanup() {
	defer s.wg.Done()
	ticker := time.NewTicker(expiredLeaseCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if s.active() && !s.removeExpiredKeys(time.Now()) {
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) active() bool {
	return serverPhase(s.phase.Load()) == phaseActive
}
