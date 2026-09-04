package client_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	redleaseclient "github.com/udovenkoav1981/RedLease/client"
	redleaseserver "github.com/udovenkoav1981/RedLease/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientAndServersEndToEnd(t *testing.T) {
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
	time.Sleep(redleaseserver.ProtocolMaxTTL + 250*time.Millisecond)

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

	previousValidUntil := firstLease.ValidUntil()
	operationContext, cancelOperation = context.WithTimeout(context.Background(), 2*time.Second)
	err = firstLease.Renew(operationContext, 5_000)
	cancelOperation()
	if err != nil {
		t.Fatalf("first lease Renew: %v", err)
	}
	if !firstLease.ValidUntil().After(previousValidUntil) {
		t.Fatal("Renew did not extend first lease validity")
	}

	firstLease.Release()
	secondLease := acquireEventually(t, secondClient, key, 5_000, 2*time.Second)
	if firstLease.ID() == secondLease.ID() {
		t.Fatal("different clients acquired the same lease ID")
	}
	secondLease.Release()
}

type integrationCluster struct {
	listeners   [redleaseclient.ServerCount]*bufconn.Listener
	grpcServers [redleaseclient.ServerCount]*grpc.Server
	lockServers [redleaseclient.ServerCount]*redleaseserver.Server
}

func newIntegrationCluster(t *testing.T) *integrationCluster {
	t.Helper()
	cluster := &integrationCluster{}

	for index := range redleaseclient.ServerCount {
		lockServer, err := redleaseserver.New(redleaseserver.Config{
			ConfiguredMaxTTL:     redleaseserver.ProtocolMaxTTL,
			ShardCount:           4,
			ShardQueueDepth:      64,
			MaxInFlightPerStream: 64,
		})
		if err != nil {
			cluster.close()
			t.Fatalf("create lock-server %d: %v", index, err)
		}

		listener := bufconn.Listen(1024 * 1024)
		grpcServer := grpc.NewServer()
		lockServer.Register(grpcServer)
		cluster.listeners[index] = listener
		cluster.grpcServers[index] = grpcServer
		cluster.lockServers[index] = lockServer

		go func() {
			_ = grpcServer.Serve(listener)
		}()
	}
	return cluster
}

func (c *integrationCluster) newClient(t *testing.T, clientID uint32) *redleaseclient.Client {
	t.Helper()
	config := redleaseclient.Config{
		ClientID:        clientID,
		ResponseTimeout: 500,
	}
	for index, listener := range c.listeners {
		listener := listener
		config.Servers[index] = redleaseclient.ServerConfig{
			Target: fmt.Sprintf("passthrough:///redlease-%d", index),
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
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
	for _, lockServer := range c.lockServers {
		if lockServer != nil {
			_ = lockServer.Close()
		}
	}
	for _, grpcServer := range c.grpcServers {
		if grpcServer != nil {
			grpcServer.Stop()
		}
	}
	for _, listener := range c.listeners {
		if listener != nil {
			_ = listener.Close()
		}
	}
}

func waitReady(t *testing.T, client *redleaseclient.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
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
