package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/udovenkoav1981/RedLease/client"
	"github.com/udovenkoav1981/RedLease/client1of1"
	"github.com/udovenkoav1981/RedLease/internal/backoff"
)

type loadLease interface {
	Valid() bool
	Release()
}

type loadClient interface {
	WaitReady(ctx context.Context) error
	Acquire(ctx context.Context, key []byte, ttl uint64) (loadLease, error)
	Close() error
}

type oneClient struct{ client *client1of1.Client }

func (c oneClient) WaitReady(ctx context.Context) error { return c.client.WaitReady(ctx) }
func (c oneClient) Close() error                        { return c.client.Close() }
func (c oneClient) Acquire(ctx context.Context, key []byte, ttl uint64) (loadLease, error) {
	lease, err := c.client.Acquire(ctx, key, client1of1.Milliseconds(ttl))
	if err != nil {
		return nil, err
	}
	return lease, nil
}

type quorumClient struct{ client *client.Client }

func (c quorumClient) WaitReady(ctx context.Context) error { return c.client.WaitReady(ctx) }
func (c quorumClient) Close() error                        { return c.client.Close() }
func (c quorumClient) Acquire(ctx context.Context, key []byte, ttl uint64) (loadLease, error) {
	lease, err := c.client.Acquire(ctx, key, client.Milliseconds(ttl))
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
				DialOptions:     []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
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
				servers[replica] = client.ServerConfig{
					Target:      address,
					DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
				}
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
	//nolint:contextcheck // Client stream managers belong to the benchmark cell, not the caller context.
	clients, err := newLoadClients(selected, clientCount, config.targets, config, logger)
	defer func() { runErr = errors.Join(runErr, closeLoadClients(clients)) }()
	if err != nil {
		return caseResult{}, err
	}
	var nonce [16]byte
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
	result = measure(ctx, clients, leaseCount, config, hex.EncodeToString(nonce[:]))
	settle := time.NewTimer(config.settleDuration())
	defer settle.Stop()
	select {
	case <-settle.C:
	case <-ctx.Done():
	}
	return result, ctx.Err()
}

func measure(ctx context.Context, clients []loadClient, leaseCount int, config *options, casePrefix string) caseResult {
	stats := make([]workerStats, len(clients)*leaseCount)
	startSignal := make(chan struct{})
	measurementCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ready, workers sync.WaitGroup
	for clientIndex, instance := range clients {
		for workerIndex := range leaseCount {
			position := clientIndex*leaseCount + workerIndex
			prefix := []byte(fmt.Sprintf("load/%s/%d/%d/", casePrefix, clientIndex, workerIndex))
			ready.Add(1)
			workers.Go(func() {
				ready.Done()
				<-startSignal
				work(measurementCtx, instance, prefix, config, &stats[position])
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

func work(ctx context.Context, instance loadClient, prefix []byte, config *options, stats *workerStats) {
	var sequence uint64
	var failures uint
	retryBackoff := backoff.Default()
	for ctx.Err() == nil {
		sequence++
		key := strconv.AppendUint(bytes.Clone(prefix), sequence, decimalBase)
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
		if held && !lease.Valid() {
			stats.expired++
		}
		lease.Release()
		if !held {
			return
		}
	}
}
