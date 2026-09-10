package client1of1_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	redleaseclient "github.com/udovenkoav1981/RedLease/client1of1"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
	redleaseserver "github.com/udovenkoav1981/RedLease/server"
)

func TestClientAndServerEndToEnd(t *testing.T) {
	cluster := newIntegrationServer(t)
	firstClient := cluster.newClient(t, 1)
	secondClient := cluster.newClient(t, 2)
	waitReady(t, firstClient)
	waitReady(t, secondClient)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	firstLease, err := firstClient.Acquire(ctx, []byte("shared-key"), 1000)
	cancel()
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if !firstLease.Valid() || firstLease.RemainingTTL() > 900 {
		t.Fatalf("first lease remaining TTL = %dms", firstLease.RemainingTTL())
	}

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	conflictingLease, err := secondClient.Acquire(ctx, []byte("shared-key"), 1000)
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
	if !firstLease.Valid() {
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
	secondLease := acquireEventually(t, secondClient, []byte("shared-key"), 1000)
	secondLease.Release()
}

func TestAcquireRejectsTTLConsumedBySafetyMargin(t *testing.T) {
	cluster := newIntegrationServer(t)
	client := cluster.newClient(t, 4)
	waitReady(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	lease, err := client.Acquire(ctx, []byte("short"), 100)
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
	for index := range leaseCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			lease, err := client.Acquire(ctx, []byte(fmt.Sprintf("key-%d", index)), 1000)
			if err != nil {
				errorsSeen <- err
				return
			}
			if !lease.Valid() {
				errorsSeen <- errors.New("Acquire returned an invalid lease")
				return
			}
			lease.Release()
		}()
	}
	workers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent Acquire: %v", err)
	}
}

func TestAmbiguousAcquireIsReleasedAfterReconnect(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	service := &disconnectAfterAcquireServer{
		acquired: make(chan *redleasev1.ClientRequest, 1),
		released: make(chan *redleasev1.ClientRequest, 1),
	}
	grpcServer := grpc.NewServer()
	redleasev1.RegisterRedLeaseServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	client, err := redleaseclient.New(redleaseclient.Config{
		ClientID: 4,
		Target:   "passthrough:///ambiguous-acquire-test",
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
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
	waitReady(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	lease, err := client.Acquire(ctx, []byte("ambiguous"), 1000)
	cancel()
	if !errors.Is(err, redleaseclient.ErrNotAcquired) {
		t.Fatalf("Acquire error = %v, want ErrNotAcquired", err)
	}
	if lease != nil {
		t.Fatal("ambiguous Acquire returned a lease")
	}

	var acquireRequest, releaseRequest *redleasev1.ClientRequest
	select {
	case acquireRequest = <-service.acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive Acquire")
	}
	select {
	case releaseRequest = <-service.released:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive cleanup Release after reconnect")
	}
	acquireID := acquireRequest.GetAcquire().GetLeaseId()
	releaseID := releaseRequest.GetRelease().GetLeaseId()
	if acquireID.GetClientId() != releaseID.GetClientId() ||
		acquireID.GetBootId() != releaseID.GetBootId() ||
		acquireID.GetLeaseSeq() != releaseID.GetLeaseSeq() {
		t.Fatal("cleanup after reconnect used a different lease ID")
	}
}

type disconnectAfterAcquireServer struct {
	redleasev1.UnimplementedRedLeaseServer

	streamCount atomic.Uint32
	acquired    chan *redleasev1.ClientRequest
	released    chan *redleasev1.ClientRequest
}

func (s *disconnectAfterAcquireServer) LeaseStream(
	stream grpc.BidiStreamingServer[redleasev1.ClientRequest, redleasev1.ServerResponse],
) error {
	if s.streamCount.Add(1) == 1 {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		s.acquired <- request
		return status.Error(codes.Unavailable, "response deliberately lost")
	}

	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		if request.GetRelease() == nil {
			continue
		}
		s.released <- request
		if err := stream.Send(&redleasev1.ServerResponse{
			RequestId: request.GetRequestId(),
			Result: &redleasev1.ServerResponse_Release{Release: &redleasev1.ReleaseResponse{
				Status: redleasev1.LeaseStatus_LEASE_STATUS_OK,
			}},
		}); err != nil {
			return err
		}
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
	key []byte,
	ttl redleaseclient.Milliseconds,
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
