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
	// DefaultMaxKeys is the default maximum number of resident lease keys.
	DefaultMaxKeys = 10_000

	safetyMargin          = 100 * time.Millisecond
	restartQuarantineTime = ProtocolMaxTTL + safetyMargin + time.Millisecond

	defaultShardCount           = 256
	defaultShardQueueDepth      = 256
	defaultMaxInFlightPerStream = 256
)

// Config controls one in-memory lock-server instance. MaxTTL is measured in
// milliseconds. Zero values for MaxKeys and the queue-related fields select
// implementation defaults.
type Config struct {
	MaxTTL  uint64
	MaxKeys uint64

	ShardCount           uint32
	ShardQueueDepth      uint32
	MaxInFlightPerStream uint32
}

// Validate checks values explicitly supplied by the caller.
func (c Config) Validate() error {
	switch {
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
	phaseClosed
)

// Server is an in-memory RedLease gRPC service. New servers initially reject
// all lease mutations while the restart quarantine timer is running.
type Server struct {
	redleasev1.UnimplementedRedLeaseServer

	config Config

	phase  atomic.Uint32
	closed atomic.Bool
	keys   atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	timer  *time.Timer

	shards []*leaseShard

	dispatchMu sync.RWMutex
	// cleanupMu prevents concurrent capacity-triggered scans of all shards.
	cleanupMu sync.Mutex
	closeOnce sync.Once
	wg        sync.WaitGroup
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
		ctx:    ctx,
		cancel: cancel,
		shards: make([]*leaseShard, config.ShardCount),
	}
	s.phase.Store(uint32(phaseQuarantine))

	for i := range s.shards {
		shard := &leaseShard{
			leases: make(map[string]*lease),
			jobs:   make(chan shardJob, config.ShardQueueDepth),
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
