// Package server implements the RedLease lock-server library.
package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
	"google.golang.org/grpc"
)

const (
	// ProtocolMaxTTL is the maximum configured TTL allowed by the protocol.
	ProtocolMaxTTL = 5 * time.Second

	safetyMargin          = 100 * time.Millisecond
	restartQuarantineTime = ProtocolMaxTTL + safetyMargin + time.Millisecond

	defaultShardCount           = 256
	defaultShardQueueDepth      = 256
	defaultMaxInFlightPerStream = 256
	leaseCleanupInterval        = time.Second
)

// Config controls one in-memory lock-server instance. Zero values for the
// queue-related fields select implementation defaults.
type Config struct {
	ConfiguredMaxTTL time.Duration

	ShardCount           int
	ShardQueueDepth      int
	MaxInFlightPerStream int
}

// Validate checks values explicitly supplied by the caller. A configured TTL
// must have exact millisecond representation because that is the wire unit.
func (c Config) Validate() error {
	switch {
	case c.ConfiguredMaxTTL <= 0:
		return errors.New("configured max TTL must be positive")
	case c.ConfiguredMaxTTL > ProtocolMaxTTL:
		return fmt.Errorf("configured max TTL must not exceed %s", ProtocolMaxTTL)
	case c.ConfiguredMaxTTL%time.Millisecond != 0:
		return errors.New("configured max TTL must be a whole number of milliseconds")
	case c.ShardCount < 0:
		return errors.New("shard count must not be negative")
	case c.ShardQueueDepth < 0:
		return errors.New("shard queue depth must not be negative")
	case c.MaxInFlightPerStream < 0:
		return errors.New("maximum in-flight requests per stream must not be negative")
	default:
		return nil
	}
}

type resolvedConfig struct {
	configuredMaxTTLMS uint64
	shardCount         int
	shardQueueDepth    int
	maxInFlight        int
}

func resolveConfig(c Config) (resolvedConfig, error) {
	if err := c.Validate(); err != nil {
		return resolvedConfig{}, err
	}

	result := resolvedConfig{
		configuredMaxTTLMS: uint64(c.ConfiguredMaxTTL / time.Millisecond),
		shardCount:         c.ShardCount,
		shardQueueDepth:    c.ShardQueueDepth,
		maxInFlight:        c.MaxInFlightPerStream,
	}
	if result.shardCount == 0 {
		result.shardCount = defaultShardCount
	}
	if result.shardQueueDepth == 0 {
		result.shardQueueDepth = defaultShardQueueDepth
	}
	if result.maxInFlight == 0 {
		result.maxInFlight = defaultMaxInFlightPerStream
	}
	return result, nil
}

type serverPhase uint32

const (
	phaseQuarantine serverPhase = iota
	phaseActive
	phaseClosed
)

// Server is an in-memory RedLease gRPC service. New servers initially reject
// all lease mutations while the restart quarantine timer is running.
type Server struct {
	redleasev1.UnimplementedRedLeaseServer

	config resolvedConfig
	now    func() time.Time
	// beforeApply is a package-private scheduling seam used by concurrency
	// tests. Production servers leave it nil.
	beforeApply func(operation)
	// afterReceive is another package-private scheduling seam. It runs after
	// the stream reader snapshots the phase of a received request.
	afterReceive func(serverPhase)

	phase  atomic.Uint32
	closed atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
	timer  *time.Timer

	shards []*leaseShard

	dispatchMu sync.RWMutex
	closeOnce  sync.Once
	wg         sync.WaitGroup
}

var _ redleasev1.RedLeaseServer = (*Server)(nil)

// New constructs a lock-server and starts its restart quarantine period.
func New(c Config) (*Server, error) {
	config, err := resolveConfig(c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		config: config,
		now:    time.Now,
		ctx:    ctx,
		cancel: cancel,
		shards: make([]*leaseShard, config.shardCount),
	}
	s.phase.Store(uint32(phaseQuarantine))

	for i := range s.shards {
		shard := &leaseShard{
			leases: make(map[string]lease),
			jobs:   make(chan shardJob, config.shardQueueDepth),
		}
		s.shards[i] = shard
		s.wg.Add(1)
		go s.runShard(shard)
	}

	s.timer = time.NewTimer(restartQuarantineTime)
	s.wg.Add(1)
	go s.runQuarantine()

	return s, nil
}

// Register registers s with a gRPC service registrar.
func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	redleasev1.RegisterRedLeaseServer(registrar, s)
}

// Close stops accepting work, cancels active streams and drains work already
// submitted to the shard queues. It is safe to call Close more than once.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
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
	})
	return nil
}

func (s *Server) runQuarantine() {
	defer s.wg.Done()
	defer s.timer.Stop()

	select {
	case <-s.timer.C:
		s.phase.CompareAndSwap(uint32(phaseQuarantine), uint32(phaseActive))
	case <-s.ctx.Done():
	}
}

func (s *Server) active() bool {
	return serverPhase(s.phase.Load()) == phaseActive
}
