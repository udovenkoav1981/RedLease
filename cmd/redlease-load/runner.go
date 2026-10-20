package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/udovenkoav1981/RedLease/client"
	"github.com/udovenkoav1981/RedLease/client1of1"
	"github.com/udovenkoav1981/RedLease/clientmetrics"
	"github.com/udovenkoav1981/RedLease/internal/backoff"
)

type loadLease interface {
	RemainingTTLms() uint64
	Release()
}

type loadClient interface {
	WaitReady(ctx context.Context) error
	Acquire(ctx context.Context, key, ttl uint64) (loadLease, error)
	MetricsSnapshot() clientmetrics.Snapshot
	Close() error
}

type oneClient struct{ client *client1of1.Client }

func (c oneClient) WaitReady(ctx context.Context) error { return c.client.WaitReady(ctx) }
func (c oneClient) Close() error                        { return c.client.Close() }
func (c oneClient) MetricsSnapshot() clientmetrics.Snapshot {
	return c.client.MetricsSnapshot()
}

func (c oneClient) Acquire(ctx context.Context, key, ttl uint64) (loadLease, error) {
	lease, err := c.client.Acquire(ctx, key, ttl)
	if err != nil {
		return nil, err
	}
	return lease, nil
}

type quorumClient struct{ client *client.Client }

func (c quorumClient) WaitReady(ctx context.Context) error { return c.client.WaitReady(ctx) }
func (c quorumClient) Close() error                        { return c.client.Close() }
func (c quorumClient) MetricsSnapshot() clientmetrics.Snapshot {
	return c.client.MetricsSnapshot()
}

func (c quorumClient) Acquire(ctx context.Context, key, ttl uint64) (loadLease, error) {
	lease, err := c.client.Acquire(ctx, key, ttl)
	if err != nil {
		return nil, err
	}
	return lease, nil
}

var errLoadTargetsUnavailable = errors.New("RedLease server targets are unavailable")

type loadClientGeneration struct {
	client loadClient
}

// reconfigurableLoadClient keeps endpoint changes off the load generator's hot
// path: Acquire needs only one atomic load. Retired clients remain alive long
// enough for leases issued by the previous generation to release or expire.
type reconfigurableLoadClient struct {
	selected scenario
	clientID uint32
	config   *options
	logger   *slog.Logger

	current atomic.Pointer[loadClientGeneration]

	mu      sync.Mutex
	closed  bool
	retired []loadClient
	timers  []*time.Timer
}

func newReconfigurableLoadClient(
	selected scenario,
	clientID uint32,
	addresses []string,
	config *options,
	logger *slog.Logger,
) (*reconfigurableLoadClient, error) {
	instance, err := newLoadClient(selected, clientID, addresses, config, logger)
	if err != nil {
		return nil, err
	}
	result := &reconfigurableLoadClient{
		selected: selected,
		clientID: clientID,
		config:   config,
		logger:   logger,
	}
	result.current.Store(&loadClientGeneration{client: instance})
	return result, nil
}

func (c *reconfigurableLoadClient) WaitReady(ctx context.Context) error {
	generation := c.current.Load()
	if generation == nil {
		return errLoadTargetsUnavailable
	}
	return generation.client.WaitReady(ctx)
}

func (c *reconfigurableLoadClient) Acquire(ctx context.Context, key, ttl uint64) (loadLease, error) {
	generation := c.current.Load()
	if generation == nil {
		return nil, errLoadTargetsUnavailable
	}
	return generation.client.Acquire(ctx, key, ttl)
}

func (c *reconfigurableLoadClient) MetricsSnapshot() clientmetrics.Snapshot {
	c.mu.Lock()
	clients := make([]loadClient, 0, len(c.retired)+1)
	clients = append(clients, c.retired...)
	if current := c.current.Load(); current != nil {
		clients = append(clients, current.client)
	}
	c.mu.Unlock()
	return aggregateLoadClientMetrics(clients)
}

func (c *reconfigurableLoadClient) reconfigure(addresses []string) error {
	var replacement *loadClientGeneration
	if len(addresses) != 0 {
		instance, err := newLoadClient(c.selected, c.clientID, addresses, c.config, c.logger)
		if err != nil {
			return errors.Join(err, c.replace(nil))
		}
		replacement = &loadClientGeneration{client: instance}
	}
	return c.replace(replacement)
}

func (c *reconfigurableLoadClient) replace(replacement *loadClientGeneration) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		if replacement != nil {
			return errors.Join(errLoadTargetsUnavailable, replacement.client.Close())
		}
		return errLoadTargetsUnavailable
	}
	previous := c.current.Swap(replacement)
	if previous != nil {
		c.retired = append(c.retired, previous.client)
		c.timers = append(c.timers, time.AfterFunc(c.config.settleDuration(), func() {
			_ = previous.client.Close()
		}))
	}
	c.mu.Unlock()
	return nil
}

func (c *reconfigurableLoadClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	current := c.current.Swap(nil)
	clients := c.retired
	c.retired = nil
	if current != nil {
		clients = append(clients, current.client)
	}
	for _, timer := range c.timers {
		timer.Stop()
	}
	c.timers = nil
	c.mu.Unlock()
	return closeLoadClients(clients)
}

func newLoadClient(
	selected scenario,
	clientID uint32,
	addresses []string,
	config *options,
	logger *slog.Logger,
) (loadClient, error) {
	if selected.name == client1of1Name {
		created, err := client1of1.New(client1of1.Config{
			ClientID:        clientID,
			Target:          addresses[0],
			Logger:          logger,
			ResponseTimeout: config.responseTimeout,
		})
		if err != nil {
			return nil, err
		}
		return oneClient{client: created}, nil
	}

	servers := make([]client.ServerConfig, len(addresses))
	for replica, address := range addresses {
		servers[replica] = client.ServerConfig{Target: address}
	}
	created, err := client.New(client.Config{
		ClientID:        clientID,
		Quorum:          selected.quorum,
		Servers:         servers,
		Logger:          logger,
		ResponseTimeout: config.responseTimeout,
	})
	if err != nil {
		return nil, err
	}
	return quorumClient{client: created}, nil
}

func newLoadClients(
	selected scenario,
	count int,
	addresses []string,
	config *options,
	logger *slog.Logger,
) ([]loadClient, error) {
	clients := make([]loadClient, 0, count)
	for index := range count {
		instance, err := newLoadClient(selected, uint32(index+1), addresses, config, logger)
		if err != nil {
			return clients, fmt.Errorf("create client %d: %w", index, err)
		}
		clients = append(clients, instance)
	}
	return clients, nil
}

func newReconfigurableLoadClients(
	selected scenario,
	count int,
	addresses []string,
	config *options,
	logger *slog.Logger,
) ([]loadClient, []*reconfigurableLoadClient, error) {
	clients := make([]loadClient, 0, count)
	reconfigurable := make([]*reconfigurableLoadClient, 0, count)
	for index := range count {
		instance, err := newReconfigurableLoadClient(
			selected,
			uint32(index+1),
			addresses,
			config,
			logger,
		)
		if err != nil {
			return clients, reconfigurable, fmt.Errorf("create client %d: %w", index, err)
		}
		clients = append(clients, instance)
		reconfigurable = append(reconfigurable, instance)
	}
	return clients, reconfigurable, nil
}

func watchLoadTargets(
	ctx context.Context,
	state *targetState,
	clients []*reconfigurableLoadClient,
	initialVersion uint64,
	logger *slog.Logger,
) {
	currentVersion := initialVersion
	for {
		targets, version, changed := state.snapshot()
		if currentVersion != version {
			for index, instance := range clients {
				if err := instance.reconfigure(targets); err != nil {
					logger.Error(
						"cannot reconfigure RedLease load client",
						slog.Int("client_index", index),
						slog.Any("error", err),
					)
				}
			}
			currentVersion = version
			logger.Info("RedLease load clients reconfigured", slog.Any("targets", targets))
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		}
	}
}

func closeLoadClients(clients []loadClient) error {
	var errs []error
	for index, instance := range clients {
		if err := instance.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close client %d: %w", index, err))
		}
	}
	return errors.Join(errs...)
}

func runCase(
	ctx context.Context,
	selected scenario,
	clientCount, leaseCount int,
	config *options,
	logger *slog.Logger,
	discovery *etcdTargetDiscovery,
	metricsSource *loadMetricsSource,
) (result caseResult, runErr error) {
	var clients []loadClient
	var stopTargetWatch func()
	var err error
	if discovery == nil {
		//nolint:contextcheck // Client connection managers belong to the benchmark cell, not the caller context.
		clients, err = newLoadClients(selected, clientCount, config.targets, config, logger)
	} else {
		var targets []string
		var targetVersion uint64
		targets, targetVersion, err = discovery.targets.waitReady(ctx)
		if err == nil {
			var reconfigurable []*reconfigurableLoadClient
			//nolint:contextcheck // Client connection managers belong to the benchmark cell, not the caller context.
			clients, reconfigurable, err = newReconfigurableLoadClients(
				selected,
				clientCount,
				targets,
				config,
				logger,
			)
			if err == nil {
				watchContext, cancelWatch := context.WithCancel(ctx)
				var watchWorker sync.WaitGroup
				watchWorker.Go(func() {
					watchLoadTargets(
						watchContext,
						discovery.targets,
						reconfigurable,
						targetVersion,
						discovery.logger,
					)
				})
				stopTargetWatch = func() {
					cancelWatch()
					watchWorker.Wait()
				}
			}
		}
	}
	defer func() {
		if stopTargetWatch != nil {
			stopTargetWatch()
		}
		runErr = errors.Join(runErr, closeLoadClients(clients))
	}()
	if err != nil {
		return caseResult{}, err
	}
	metricsSource.setClients(clients)
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return caseResult{}, fmt.Errorf("generate key prefix: %w", err)
	}
	for index, instance := range clients {
		readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		readyErr := instance.WaitReady(readyCtx)
		cancel()
		if readyErr != nil {
			return caseResult{}, fmt.Errorf("wait for client %d: %w", index, readyErr)
		}
	}
	result = measure(ctx, clients, leaseCount, config, binary.LittleEndian.Uint64(nonce[:]))
	settle := time.NewTimer(config.settleDuration())
	defer settle.Stop()
	select {
	case <-settle.C:
	case <-ctx.Done():
	}
	return result, ctx.Err()
}

func measure(ctx context.Context, clients []loadClient, leaseCount int, config *options, keySeed uint64) caseResult {
	stats := make([]workerStats, len(clients)*leaseCount)
	workerCancels := make([]context.CancelFunc, len(stats))
	keyStep := uint64(len(stats))
	startSignal := make(chan struct{})
	var ready, workers sync.WaitGroup
	workerKey := keySeed
	for clientIndex, instance := range clients {
		for workerIndex := range leaseCount {
			position := clientIndex*leaseCount + workerIndex
			initialKey := workerKey
			workerKey++
			workerCtx, cancelWorker := context.WithCancel(ctx)
			workerCancels[position] = cancelWorker
			ready.Add(1)
			workers.Go(func() {
				ready.Done()
				<-startSignal
				work(
					workerCtx,
					instance,
					initialKey,
					keyStep,
					config,
					&stats[position],
				)
			})
		}
	}
	ready.Wait()
	timer := time.NewTimer(config.duration)
	defer timer.Stop()
	started := time.Now()
	close(startSignal)
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
	elapsed := time.Since(started)
	for _, cancelWorker := range workerCancels {
		cancelWorker()
	}
	workers.Wait()
	return summarize(stats, len(clients), leaseCount, elapsed)
}

func work(
	ctx context.Context,
	instance loadClient,
	key, keyStep uint64,
	config *options,
	stats *workerStats,
) {
	var failures uint
	retryBackoff := backoff.Default()
	for ctx.Err() == nil {
		key += keyStep
		started := time.Now()
		lease, err := instance.Acquire(ctx, key, config.ttlMS)
		elapsed := time.Since(started)
		if ctx.Err() != nil {
			if lease != nil {
				lease.Release()
			}
			return
		}
		if err != nil {
			stats.failed++
			if errors.Is(err, client.ErrKeyLimitReached) || errors.Is(err, client1of1.ErrKeyLimitReached) {
				stats.keyLimit++
			}
			if !backoff.Wait(ctx, retryBackoff.Duration(failures)) {
				return
			}
			failures++
			continue
		}
		failures = 0
		stats.succeeded++
		stats.latencies.observe(elapsed)
		stats.totalLatency += elapsed
		held := backoff.Wait(ctx, config.hold)
		if held && lease.RemainingTTLms() == 0 {
			stats.expired++
		}
		lease.Release()
		if !held {
			return
		}
	}
}
