package client_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	redleaseclient "github.com/udovenkoav1981/RedLease/client"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
	redleaseserver "github.com/udovenkoav1981/RedLease/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const integrationServerCount = 5

func TestClientAndServersEndToEnd(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationCluster(t)
	defer cluster.close()

	firstClient := cluster.newClient(t, 1)
	defer firstClient.Close()
	secondClient := cluster.newClient(t, 2)
	defer secondClient.Close()

	waitReady(t, firstClient)
	waitReady(t, secondClient)

	// Exercise the real restart-quarantine timer instead of exposing a
	// production switch that could bypass this safety invariant.
	waitForServerActivation()

	key := []byte("integration-key")
	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	firstLease, err := firstClient.Acquire(operationContext, key, 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("first client Acquire: %v", err)
	}
	if !firstLease.Valid() {
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
	if !firstLease.Valid() || firstLease.RemainingTTL() == 0 {
		t.Fatal("Renew did not leave first lease valid")
	}

	firstLease.Release()
	secondLease := acquireEventually(t, secondClient, key, 5_000, 2*time.Second)
	if firstLease.ID() == secondLease.ID() {
		t.Fatal("different clients acquired the same lease ID")
	}
	secondLease.Release()
}

func TestClientAcquiresWithTwoUnavailableServersEndToEnd(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationCluster(t)
	defer cluster.close()

	client := cluster.newClient(t, 1)
	defer client.Close()
	waitReady(t, client)
	waitForServerActivation()

	cluster.stopReplica(3)
	cluster.stopReplica(4)

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	lease, err := client.Acquire(operationContext, []byte("three-of-five"), 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("Acquire with two unavailable servers: %v", err)
	}
	if !lease.Valid() {
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
	defer client.Close()
	waitReady(t, client)
	waitForServerActivation()

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	lease, err := client.Acquire(operationContext, []byte("heterogeneous-ttl"), 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("Acquire with heterogeneous TTLs: %v", err)
	}
	remaining := lease.RemainingTTL()
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
	defer client.Close()
	waitReady(t, client)
	waitForServerActivation()

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	lease, err := client.Acquire(operationContext, []byte("restart-healing"), 5_000)
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
	defer firstClient.Close()
	secondClient := cluster.newClient(t, 2)
	defer secondClient.Close()
	waitReady(t, firstClient)
	waitReady(t, secondClient)
	waitForServerActivation()

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	oldLease, err := firstClient.Acquire(operationContext, []byte("full-restart"), 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("Acquire before full restart: %v", err)
	}

	for index := range integrationServerCount {
		cluster.restartReplica(t, index)
	}

	operationContext, cancelOperation = context.WithTimeout(context.Background(), 2*time.Second)
	leaseDuringQuarantine, err := secondClient.Acquire(operationContext, []byte("full-restart"), 5_000)
	cancelOperation()
	if !errors.Is(err, redleaseclient.ErrNotAcquired) {
		t.Fatalf("Acquire during full-restart quarantine error = %v, want ErrNotAcquired", err)
	}
	if leaseDuringQuarantine != nil {
		t.Fatal("Acquire during full-restart quarantine returned a lease")
	}

	waitForServerActivation()
	if oldLease.Valid() {
		t.Fatal("old lease remained locally valid after full restart quarantine")
	}

	newLease := acquireEventually(t, secondClient, []byte("full-restart"), 5_000, 3*time.Second)
	if oldLease.ID() == newLease.ID() {
		t.Fatal("lease ID was reused after full cluster restart")
	}
	newLease.Release()
}

func TestServerKeyLimitEndToEnd(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationClusterWithMaxKeys(t, 1)
	defer cluster.close()

	client := cluster.newClient(t, 1)
	defer client.Close()
	waitReady(t, client)
	waitForServerActivation()

	operationContext, cancelOperation := context.WithTimeout(context.Background(), 2*time.Second)
	firstLease, err := client.Acquire(operationContext, []byte("first-capacity-key"), 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	operationContext, cancelOperation = context.WithTimeout(context.Background(), 2*time.Second)
	limitedLease, err := client.Acquire(operationContext, []byte("over-capacity-key"), 5_000)
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
		[]byte("replacement-capacity-key"),
		5_000,
		2*time.Second,
	)
	replacement.Release()
}

func TestServerKeySizeLimitOverGRPC(t *testing.T) {
	t.Parallel()
	cluster := newIntegrationCluster(t)
	defer cluster.close()
	waitForServerActivation()

	connection, err := grpc.NewClient(
		"passthrough:///redlease-key-size",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return cluster.dialReplica(ctx, 0)
		}),
	)
	if err != nil {
		t.Fatalf("create gRPC connection: %v", err)
	}
	defer connection.Close()

	streamContext, cancelStream := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStream()
	stream, err := redleasev1.NewRedLeaseClient(connection).LeaseStream(streamContext)
	if err != nil {
		t.Fatalf("open LeaseStream: %v", err)
	}

	acquire := func(requestID uint64, key []byte) redleasev1.LeaseStatus {
		t.Helper()
		err := stream.Send(&redleasev1.ClientRequest{
			RequestId: requestID,
			Operation: &redleasev1.ClientRequest_Acquire{Acquire: &redleasev1.AcquireRequest{
				Key: key,
				LeaseId: &redleasev1.LeaseID{
					ClientId: 1,
					BootId:   1,
					LeaseSeq: requestID,
				},
				RequestedTtlMs: 5_000,
			}},
		})
		if err != nil {
			t.Fatalf("send Acquire %d: %v", requestID, err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("receive Acquire %d: %v", requestID, err)
		}
		if response.GetRequestId() != requestID || response.GetAcquire() == nil {
			t.Fatalf("Acquire %d response = %+v", requestID, response)
		}
		return response.GetAcquire().GetStatus()
	}

	if status := acquire(1, bytes.Repeat([]byte{'x'}, protocol.MaxKeyBytes+1)); status != redleasev1.LeaseStatus_LEASE_STATUS_KEY_TOO_LARGE {
		t.Fatalf("oversized Acquire = %s, want KEY_TOO_LARGE", status)
	}
	if status := acquire(2, bytes.Repeat([]byte{'x'}, protocol.MaxKeyBytes)); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("boundary-size Acquire = %s, want OK", status)
	}
}

type integrationCluster struct {
	mu          sync.RWMutex
	listeners   [integrationServerCount]*bufconn.Listener
	grpcServers [integrationServerCount]*grpc.Server
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
	}
	for index := range c.listeners {
		index := index
		config.Servers[index] = redleaseclient.ServerConfig{
			Target: fmt.Sprintf("passthrough:///redlease-%d", index),
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return c.dialReplica(ctx, index)
				}),
			},
		}
	}

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
	lockServer, err := redleaseserver.New(redleaseserver.Config{
		MaxTTL:               uint64(c.ttls[index] / time.Millisecond),
		MaxKeys:              c.maxKeys,
		ShardCount:           4,
		ShardQueueDepth:      64,
		MaxInFlightPerStream: 64,
	})
	if err != nil {
		c.close()
		t.Fatalf("create lock-server %d: %v", index, err)
	}

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	lockServer.Register(grpcServer)

	c.mu.Lock()
	c.listeners[index] = listener
	c.grpcServers[index] = grpcServer
	c.lockServers[index] = lockServer
	c.mu.Unlock()

	go func() { _ = grpcServer.Serve(listener) }()
}

func (c *integrationCluster) stopReplica(index int) {
	c.mu.Lock()
	listener := c.listeners[index]
	grpcServer := c.grpcServers[index]
	lockServer := c.lockServers[index]
	c.listeners[index] = nil
	c.grpcServers[index] = nil
	c.lockServers[index] = nil
	c.mu.Unlock()

	if grpcServer != nil {
		grpcServer.Stop()
	}
	if lockServer != nil {
		_ = lockServer.Close()
	}
	if listener != nil {
		_ = listener.Close()
	}
}

func (c *integrationCluster) restartReplica(t *testing.T, index int) {
	t.Helper()
	c.stopReplica(index)
	c.startReplica(t, index)
}

func (c *integrationCluster) dialReplica(
	ctx context.Context,
	index int,
) (net.Conn, error) {
	c.mu.RLock()
	listener := c.listeners[index]
	c.mu.RUnlock()
	if listener == nil {
		return nil, fmt.Errorf("replica %d is unavailable", index)
	}
	return listener.DialContext(ctx)
}

func waitReady(t *testing.T, client *redleaseclient.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func waitForServerActivation() {
	// The server deliberately exposes no way to bypass this safety invariant.
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
	key []byte,
	ttl redleaseclient.Milliseconds,
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
