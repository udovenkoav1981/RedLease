package client1of1_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	redleaseclient "github.com/udovenkoav1981/RedLease/client1of1"
	redleaseserver "github.com/udovenkoav1981/RedLease/server"
)

func TestClientAndServerEndToEnd(t *testing.T) {
	cluster := newIntegrationServer(t)
	firstClient := cluster.newClient(t, 1)
	secondClient := cluster.newClient(t, 2)
	waitReady(t, firstClient)
	waitReady(t, secondClient)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	firstLease, err := firstClient.Acquire(ctx, 1, 1000)
	cancel()
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if firstLease.RemainingTTLms() == 0 || firstLease.RemainingTTLms() > 900 {
		t.Fatalf("first lease remaining TTL = %dms", firstLease.RemainingTTLms())
	}

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	conflictingLease, err := secondClient.Acquire(ctx, 1, 1000)
	cancel()
	if !errors.Is(err, redleaseclient.ErrNotAcquired) {
		t.Fatalf("conflicting Acquire error = %v, want ErrNotAcquired", err)
	}
	if conflictingLease != nil {
		t.Fatal("conflicting Acquire returned a lease")
	}

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	err = firstLease.Renew(ctx, 1000)
	cancel()
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if firstLease.RemainingTTLms() == 0 {
		t.Fatal("renewed lease is invalid")
	}

	firstLease.Release()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = firstLease.Renew(ctx, 1000)
	cancel()
	if !errors.Is(err, redleaseclient.ErrNotRenewed) ||
		!errors.Is(err, redleaseclient.ErrLeaseReleased) {
		t.Fatalf("Renew after Release error = %v", err)
	}
	secondLease := acquireEventually(t, secondClient, 1, 1000)
	secondLease.Release()
}

func TestAcquireRejectsTTLConsumedBySafetyMargin(t *testing.T) {
	cluster := newIntegrationServer(t)
	client := cluster.newClient(t, 4)
	waitReady(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	lease, err := client.Acquire(ctx, 2, 100)
	cancel()
	if !errors.Is(err, redleaseclient.ErrNotAcquired) {
		t.Fatalf("Acquire error = %v, want ErrNotAcquired", err)
	}
	if lease != nil {
		t.Fatal("Acquire without positive local validity returned a lease")
	}
}

func TestConcurrentLeasesShareOneStream(t *testing.T) {
	cluster := newIntegrationServer(t)
	client := cluster.newClient(t, 3)
	waitReady(t, client)

	const leaseCount = 64
	var workers sync.WaitGroup
	errorsSeen := make(chan error, leaseCount)
	key := uint64(1)
	for range leaseCount {
		leaseKey := key
		key++
		workers.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			lease, err := client.Acquire(ctx, leaseKey, 1000)
			if err != nil {
				errorsSeen <- err
				return
			}
			if lease.RemainingTTLms() == 0 {
				errorsSeen <- errors.New("Acquire returned an invalid lease")
				return
			}
			lease.Release()
		})
	}
	workers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent Acquire: %v", err)
	}
}

type integrationServer struct {
	t        *testing.T
	listener *bufconn.Listener
	grpc     *grpc.Server
	lease    *redleaseserver.Server
}

func newIntegrationServer(t *testing.T) *integrationServer {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	leaseServer, err := redleaseserver.New(redleaseserver.Config{
		MaxTTL:                5000,
		MaxKeys:               256,
		Logger:                slog.New(slog.DiscardHandler),
		SkipRestartQuarantine: true,
		ShardCount:            8,
		ShardQueueDepth:       64,
		MaxInFlightPerStream:  256,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	grpcServer := grpc.NewServer()
	leaseServer.Register(grpcServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	server := &integrationServer{
		t:        t,
		listener: listener,
		grpc:     grpcServer,
		lease:    leaseServer,
	}
	t.Cleanup(server.close)
	return server
}

func (s *integrationServer) newClient(t *testing.T, clientID uint32) *redleaseclient.Client {
	t.Helper()
	client, err := redleaseclient.New(redleaseclient.Config{
		ClientID: clientID,
		Target:   "passthrough:///redlease-test",
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return s.listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		Logger:          slog.New(slog.DiscardHandler),
		ResponseTimeout: 500,
	})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func (s *integrationServer) close() {
	_ = s.lease.Close()
	s.grpc.Stop()
	_ = s.listener.Close()
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
	key uint64,
	ttl uint64,
) *redleaseclient.Lease {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		lease, err := client.Acquire(ctx, key, ttl)
		cancel()
		if err == nil {
			return lease
		}
		if !errors.Is(err, redleaseclient.ErrNotAcquired) || time.Now().After(deadline) {
			t.Fatalf("Acquire after Release: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
