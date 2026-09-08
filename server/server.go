// Package server implements the RedLease lock-server library.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/boottime"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
	"google.golang.org/grpc"
)

const (
	// ProtocolMaxTTL is the maximum configured TTL allowed by the protocol.
	ProtocolMaxTTL = 5 * time.Second
	// RestartQuarantineDuration is the minimum delay enforced by the built-in
	// restart quarantine. An embedded owner which skips that quarantine assumes
	// responsibility for enforcing the same delay when prior RAM state may have
	// been lost.
	RestartQuarantineDuration = ProtocolMaxTTL + safetyMargin + time.Millisecond
	// DefaultMaxKeys is the default maximum number of resident lease keys.
	DefaultMaxKeys = 10_000

	safetyMargin = 100 * time.Millisecond

	defaultShardCount           = 256
	defaultShardQueueDepth      = 256
	defaultMaxInFlightPerStream = 256
	expiredLeaseCleanupInterval = time.Minute
)

// ErrServerFailed identifies a fatal internal server error. The affected
// Server has stopped accepting work and cannot be returned to service.
var ErrServerFailed = errors.New("RedLease server failed")

// Config controls one in-memory lock-server instance. MaxTTL is measured in
// milliseconds. Zero values for MaxKeys and the queue-related fields select
// implementation defaults.
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

	ShardCount           uint32
	ShardQueueDepth      uint32
	MaxInFlightPerStream uint32
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
	if c.ShardQueueDepth == 0 {
		c.ShardQueueDepth = defaultShardQueueDepth
	}
	if c.MaxInFlightPerStream == 0 {
		c.MaxInFlightPerStream = defaultMaxInFlightPerStream
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

// Server is an in-memory RedLease gRPC service. New servers initially reject
// all lease mutations while the restart quarantine timer is running.
type Server struct {
	redleasev1.UnimplementedRedLeaseServer

	config Config
	logger *slog.Logger

	phase atomic.Uint32
	keys  atomic.Uint64

	activeStreams   atomic.Int64
	operationTotals [operationKindCount]atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	timer  *time.Timer
	fatal  chan error

	shards []*leaseShard

	dispatchMu sync.RWMutex
	// cleanupMu prevents concurrent capacity-triggered scans of all shards.
	cleanupMu sync.Mutex
	failOnce  sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup
}

var _ redleasev1.RedLeaseServer = (*Server)(nil)

// New constructs a lock-server. By default it starts in restart quarantine;
// SkipRestartQuarantine makes it immediately active under owner-managed
// restart safety.
func New(c Config) (*Server, error) {
	config, err := resolveConfig(c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		config: config,
		logger: config.Logger.With(slog.String("component", "redlease-server")),
		ctx:    ctx,
		cancel: cancel,
		fatal:  make(chan error, 1),
		shards: make([]*leaseShard, config.ShardCount),
	}
	if config.SkipRestartQuarantine {
		s.phase.Store(uint32(phaseActive))
	} else {
		s.phase.Store(uint32(phaseQuarantine))
	}

	for i := range s.shards {
		shard := &leaseShard{
			leases: make(map[string]*lease),
			jobs:   make(chan shardJob, config.ShardQueueDepth),
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
		slog.Uint64("shard_queue_depth", uint64(config.ShardQueueDepth)),
		slog.Uint64("max_in_flight_per_stream", uint64(config.MaxInFlightPerStream)),
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

	return s, nil
}

// Register registers s with a gRPC service registrar.
func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	redleasev1.RegisterRedLeaseServer(registrar, s)
}

// Fatal returns a channel which receives exactly one non-nil error if the
// server detects an unrecoverable internal failure. Detection moves the server
// to FAILED and cancels its active streams. The channel is buffered so failure
// detection never waits for the owner, and normal Close does not send to it.
// A failed Server cannot be returned to service and must be closed.
func (s *Server) Fatal() <-chan error {
	return s.fatal
}

func (s *Server) fail(cause error) {
	s.failOnce.Do(func() {
		failure := fmt.Errorf("%w: %v", ErrServerFailed, cause)
		for {
			phase := serverPhase(s.phase.Load())
			if phase == phaseClosed || s.phase.CompareAndSwap(uint32(phase), uint32(phaseFailed)) {
				break
			}
		}
		s.cancel()
		s.fatal <- failure
		s.logger.Error(
			"server entered failed state",
			slog.String("state", "FAILED"),
			slog.Any("error", failure),
		)
	})
}

// Close stops accepting work, cancels active streams and drains work already
// submitted to the shard queues. It is safe to call Close more than once.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.phase.Store(uint32(phaseClosed))
		s.cancel()

		// A dispatcher holds a read lock until its send to a shard succeeds or
		// its stream is cancelled. Cancelling above guarantees that Close can
		// eventually acquire this write lock without closing a channel under a
		// sender.
		s.dispatchMu.Lock()
		for _, shard := range s.shards {
			close(shard.jobs)
		}
		s.dispatchMu.Unlock()

		s.wg.Wait()
		s.logger.Info("server stopped", slog.String("state", "CLOSED"))
	})
	return nil
}

func (s *Server) runQuarantine() {
	defer s.wg.Done()
	defer s.timer.Stop()
	defer func() {
		if recovered := recover(); recovered != nil {
			s.failRecoveredPanic("running quarantine", recovered)
		}
	}()

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
	defer func() {
		if recovered := recover(); recovered != nil {
			s.failRecoveredPanic("cleaning expired leases", recovered)
		}
	}()

	for {
		select {
		case <-ticker.C:
			if s.active() && !s.removeExpiredKeys(boottime.Now()) {
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
