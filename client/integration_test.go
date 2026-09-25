package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	redleaseclient "github.com/udovenkoav1981/RedLease/client"
	redleaseserver "github.com/udovenkoav1981/RedLease/server"
)

const integrationServerCount = 5

func TestClientAndServersEndToEnd(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationCluster(t)
	defer cluster.close()

	firstClient := cluster.newClient(t, 1)
	defer closeResource(t, "first client", firstClient)
	secondClient := cluster.newClient(t, 2)
	defer closeResource(t, "second client", secondClient)

	waitReady(t, firstClient)
	waitReady(t, secondClient)

	// Exercise the real restart-quarantine timer instead of exposing a
	// production switch that could bypass this safety invariant.
	waitForServerActivation()

	key := uint64(1)
	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	firstLease, err := firstClient.Acquire(operationContext, key, 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("first client Acquire: %v", err)
	}
	if firstLease.RemainingTTLms() == 0 {
		t.Fatal("first client received an invalid lease")
	}

	operationContext, cancelOperation = context.WithTimeout(context.Background(), 2*time.Second)
	conflictingLease, err := secondClient.Acquire(operationContext, key, 5_000)
	cancelOperation()
	if !errors.Is(err, redleaseclient.ErrNotAcquired) {
		t.Fatalf("conflicting Acquire error = %v, want ErrNotAcquired", err)
	}
	if conflictingLease != nil {
		t.Fatal("conflicting Acquire returned a lease")
	}

	operationContext, cancelOperation = context.WithTimeout(context.Background(), 2*time.Second)
	err = firstLease.Renew(operationContext, 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("first lease Renew: %v", err)
	}
	if firstLease.RemainingTTLms() == 0 {
		t.Fatal("Renew did not leave first lease valid")
	}

	firstLease.Release()
	secondLease := acquireEventually(t, secondClient, key, 5_000, 2*time.Second)
	secondLease.Release()
}

func TestClientAcquiresWithTwoUnavailableServersEndToEnd(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationCluster(t)
	defer cluster.close()

	client := cluster.newClient(t, 1)
	defer closeResource(t, "client", client)
	waitReady(t, client)
	waitForServerActivation()

	cluster.stopReplica(3)
	cluster.stopReplica(4)

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	lease, err := client.Acquire(operationContext, uint64(1), 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("Acquire with two unavailable servers: %v", err)
	}
	if lease.RemainingTTLms() == 0 {
		t.Fatal("Acquire with two unavailable servers returned an invalid lease")
	}
	lease.Release()
}

func TestClientUsesHeterogeneousServerTTLsEndToEnd(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationClusterWithTTLs(t, [integrationServerCount]time.Duration{
		1 * time.Second,
		2 * time.Second,
		3 * time.Second,
		4 * time.Second,
		5 * time.Second,
	})
	defer cluster.close()

	client := cluster.newClient(t, 1)
	defer closeResource(t, "client", client)
	waitReady(t, client)
	waitForServerActivation()

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	lease, err := client.Acquire(operationContext, uint64(1), 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("Acquire with heterogeneous TTLs: %v", err)
	}
	remaining := lease.RemainingTTLms()
	if remaining == 0 {
		t.Fatal("heterogeneous TTL quorum is already invalid")
	}
	// No quorum of three can have a minimum TTL above the third-largest
	// configured value (3s), less the fixed 100ms safety margin.
	if remaining > 2_900 {
		t.Fatalf("heterogeneous TTL validity = %dms, want at most 2900ms", remaining)
	}
	lease.Release()
}

func TestLeaseHealsAfterServerRestartEndToEnd(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationCluster(t)
	defer cluster.close()

	client := cluster.newClient(t, 1)
	defer closeResource(t, "client", client)
	waitReady(t, client)
	waitForServerActivation()

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	lease, err := client.Acquire(operationContext, uint64(1), 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("Acquire before restart: %v", err)
	}

	cluster.restartReplica(t, 3)
	cluster.restartReplica(t, 4)

	// The original three replicas keep the lease alive while the restarted
	// replicas pass through their real quarantine period.
	renewTicker := time.NewTicker(time.Second)
	quarantine := time.NewTimer(redleaseserver.ProtocolMaxTTL + 250*time.Millisecond)
renewDuringQuarantine:
	for {
		select {
		case <-renewTicker.C:
			renewLease(t, lease)
		case <-quarantine.C:
			break renewDuringQuarantine
		}
	}
	renewTicker.Stop()
	quarantine.Stop()

	// Leave only one original replica. Renew can succeed again only after
	// background healing has restored the lease on both restarted servers.
	cluster.stopReplica(1)
	cluster.stopReplica(2)
	renewEventually(t, lease, 4*time.Second)
	lease.Release()
}

func TestFullClusterRestartDoesNotRestoreOldLeaseEndToEnd(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationCluster(t)
	defer cluster.close()

	firstClient := cluster.newClient(t, 1)
	defer closeResource(t, "first client", firstClient)
	secondClient := cluster.newClient(t, 2)
	defer closeResource(t, "second client", secondClient)
	waitReady(t, firstClient)
	waitReady(t, secondClient)
	waitForServerActivation()

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	oldLease, err := firstClient.Acquire(operationContext, uint64(1), 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("Acquire before full restart: %v", err)
	}

	for index := range integrationServerCount {
		cluster.restartReplica(t, index)
	}

	operationContext, cancelOperation = context.WithTimeout(context.Background(), 2*time.Second)
	leaseDuringQuarantine, err := secondClient.Acquire(operationContext, uint64(1), 5_000)
	cancelOperation()
	if !errors.Is(err, redleaseclient.ErrNotAcquired) {
		t.Fatalf("Acquire during full-restart quarantine error = %v, want ErrNotAcquired", err)
	}
	if leaseDuringQuarantine != nil {
		t.Fatal("Acquire during full-restart quarantine returned a lease")
	}

	waitForServerActivation()
	if oldLease.RemainingTTLms() != 0 {
		t.Fatal("old lease remained locally valid after full restart quarantine")
	}

	newLease := acquireEventually(t, secondClient, uint64(1), 5_000, 3*time.Second)
	newLease.Release()
}

func TestServerKeyLimitEndToEnd(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationClusterWithMaxKeys(t, 1)
	defer cluster.close()

	client := cluster.newClient(t, 1)
	defer closeResource(t, "client", client)
	waitReady(t, client)
	waitForServerActivation()

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	firstLease, err := client.Acquire(operationContext, uint64(1), 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	operationContext, cancelOperation = context.WithTimeout(context.Background(), 2*time.Second)
	limitedLease, err := client.Acquire(operationContext, 2, 5_000)
	cancelOperation()
	if !errors.Is(err, redleaseclient.ErrNotAcquired) ||
		!errors.Is(err, redleaseclient.ErrKeyLimitReached) {
		t.Fatalf("over-capacity Acquire error = %v, want ErrNotAcquired and ErrKeyLimitReached", err)
	}
	if limitedLease != nil {
		t.Fatal("over-capacity Acquire returned a lease")
	}

	firstLease.Release()
	replacement := acquireEventually(
		t,
		client,
		2,
		5_000,
		2*time.Second,
	)
	replacement.Release()
}

type integrationCluster struct {
	mu          sync.RWMutex
	addresses   [integrationServerCount]string
	lockServers [integrationServerCount]*redleaseserver.Server
	ttls        [integrationServerCount]time.Duration
	maxKeys     uint64
}

func newIntegrationCluster(t *testing.T) *integrationCluster {
	t.Helper()
	var ttls [integrationServerCount]time.Duration
	for index := range ttls {
		ttls[index] = redleaseserver.ProtocolMaxTTL
	}
	return newIntegrationClusterWithConfig(t, ttls, 0)
}

func newIntegrationClusterWithMaxKeys(t *testing.T, maxKeys uint64) *integrationCluster {
	t.Helper()
	var ttls [integrationServerCount]time.Duration
	for index := range ttls {
		ttls[index] = redleaseserver.ProtocolMaxTTL
	}
	return newIntegrationClusterWithConfig(t, ttls, maxKeys)
}

func newIntegrationClusterWithTTLs(
	t *testing.T,
	ttls [integrationServerCount]time.Duration,
) *integrationCluster {
	t.Helper()
	return newIntegrationClusterWithConfig(t, ttls, 0)
}

func newIntegrationClusterWithConfig(
	t *testing.T,
	ttls [integrationServerCount]time.Duration,
	maxKeys uint64,
) *integrationCluster {
	t.Helper()
	cluster := &integrationCluster{ttls: ttls, maxKeys: maxKeys}

	for index := range integrationServerCount {
		cluster.startReplica(t, index)
	}
	return cluster
}

func (c *integrationCluster) newClient(t *testing.T, clientID uint32) *redleaseclient.Client {
	t.Helper()
	config := redleaseclient.Config{
		ClientID:        clientID,
		Quorum:          redleaseclient.Quorum3Of5,
		Servers:         make([]redleaseclient.ServerConfig, integrationServerCount),
		ResponseTimeout: 500,
		Logger:          slog.New(slog.DiscardHandler),
	}
	c.mu.RLock()
	for index, address := range c.addresses {
		config.Servers[index] = redleaseclient.ServerConfig{Target: address}
	}
	c.mu.RUnlock()

	result, err := redleaseclient.New(config)
	if err != nil {
		t.Fatalf("create client %d: %v", clientID, err)
	}
	return result
}

func (c *integrationCluster) close() {
	for index := range integrationServerCount {
		c.stopReplica(index)
	}
}

func (c *integrationCluster) startReplica(t *testing.T, index int) {
	t.Helper()
	c.mu.RLock()
	address := c.addresses[index]
	c.mu.RUnlock()
	if address == "" {
		address = "127.0.0.1:0"
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", address)
	if err != nil {
		c.close()
		t.Fatalf("listen for lock-server %d: %v", index, err)
	}

	lockServer, err := redleaseserver.New(listener, redleaseserver.Config{
		MaxTTL:     uint64(c.ttls[index] / time.Millisecond), //nolint:gosec // Test fixtures use positive TTLs.
		MaxKeys:    c.maxKeys,
		Logger:     slog.New(slog.DiscardHandler),
		ShardCount: 4,
	})
	if err != nil {
		_ = listener.Close()
		c.close()
		t.Fatalf("create lock-server %d: %v", index, err)
	}

	c.mu.Lock()
	c.addresses[index] = listener.Addr().String()
	c.lockServers[index] = lockServer
	c.mu.Unlock()
}

func (c *integrationCluster) stopReplica(index int) {
	c.mu.Lock()
	lockServer := c.lockServers[index]
	c.lockServers[index] = nil
	c.mu.Unlock()

	if lockServer != nil {
		_ = lockServer.Close()
	}
}

func (c *integrationCluster) restartReplica(t *testing.T, index int) {
	t.Helper()
	c.stopReplica(index)
	c.startReplica(t, index)
}

func waitReady(t *testing.T, client *redleaseclient.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func closeResource(t *testing.T, name string, closer io.Closer) {
	t.Helper()
	if err := closer.Close(); err != nil {
		t.Errorf("close %s: %v", name, err)
	}
}

func waitForServerActivation() {
	// Integration tests deliberately exercise the built-in quarantine.
	time.Sleep(redleaseserver.ProtocolMaxTTL + 250*time.Millisecond)
}

func renewLease(t *testing.T, lease *redleaseclient.Lease) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lease.Renew(ctx, 5_000); err != nil {
		t.Fatalf("Renew: %v", err)
	}
}

func renewEventually(t *testing.T, lease *redleaseclient.Lease, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := lease.Renew(ctx, 5_000)
		cancel()
		if err == nil {
			return
		}
		if !errors.Is(err, redleaseclient.ErrNotRenewed) {
			t.Fatalf("Renew after healing: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Renew did not recover after restart healing: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func acquireEventually(
	t *testing.T,
	client *redleaseclient.Client,
	key uint64,
	ttl uint64,
	timeout time.Duration,
) *redleaseclient.Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		lease, err := client.Acquire(ctx, key, ttl)
		if err == nil {
			return lease
		}
		if !errors.Is(err, redleaseclient.ErrNotAcquired) {
			t.Fatalf("Acquire after Release: %v", err)
		}
		if ctx.Err() != nil {
			t.Fatalf("Acquire after Release did not succeed: %v", ctx.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
