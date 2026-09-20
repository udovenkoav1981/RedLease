package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/udovenkoav1981/RedLease/client"
	"github.com/udovenkoav1981/RedLease/client1of1"
	"github.com/udovenkoav1981/RedLease/internal/backoff"
)

type loadLease interface {
	RemainingTTLms() uint64
	Release()
}

type loadClient interface {
	WaitReady(ctx context.Context) error
	Acquire(ctx context.Context, key, ttl uint64) (loadLease, error)
	Close() error
}

type oneClient struct{ client *client1of1.Client }

func (c oneClient) WaitReady(ctx context.Context) error { return c.client.WaitReady(ctx) }
func (c oneClient) Close() error                        { return c.client.Close() }
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
func (c quorumClient) Acquire(ctx context.Context, key, ttl uint64) (loadLease, error) {
	lease, err := c.client.Acquire(ctx, key, ttl)
	if err != nil {
		return nil, err
	}
	return lease, nil
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
		clientID := uint32(index + 1)
		var instance loadClient
		if selected.name == client1of1Name {
			created, err := client1of1.New(client1of1.Config{
				ClientID:        clientID,
				Target:          addresses[0],
				Logger:          logger,
				ResponseTimeout: config.responseTimeout,
			})
			if err != nil {
				return clients, fmt.Errorf("create client %d: %w", index, err)
			}
			instance = oneClient{client: created}
		} else {
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
				return clients, fmt.Errorf("create client %d: %w", index, err)
			}
			instance = quorumClient{client: created}
		}
		clients = append(clients, instance)
	}
	return clients, nil
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
) (result caseResult, runErr error) {
	//nolint:contextcheck // Client connection managers belong to the benchmark cell, not the caller context.
	clients, err := newLoadClients(selected, clientCount, config.targets, config, logger)
	defer func() { runErr = errors.Join(runErr, closeLoadClients(clients)) }()
	if err != nil {
		return caseResult{}, err
	}
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
	keyStep := uint64(len(stats))
	startSignal := make(chan struct{})
	measurementCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ready, workers sync.WaitGroup
	workerKey := keySeed
	for clientIndex, instance := range clients {
		for workerIndex := range leaseCount {
			position := clientIndex*leaseCount + workerIndex
			initialKey := workerKey
			workerKey++
			ready.Add(1)
			workers.Go(func() {
				ready.Done()
				<-startSignal
				work(
					measurementCtx,
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
	cancel()
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
