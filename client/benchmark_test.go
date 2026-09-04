package client

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func BenchmarkClientAcquireRelease(b *testing.B) {
	client := newBenchmarkClient(b)
	b.ReportAllocs()
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		key := strconv.AppendInt(nil, int64(iteration), 10)
		lease, err := client.Acquire(context.Background(), key, 5_000)
		if err != nil {
			b.Fatalf("Acquire: %v", err)
		}
		lease.Release()
		<-lease.releaseDone
	}
}

func BenchmarkLeaseRenew(b *testing.B) {
	client := newBenchmarkClient(b)
	lease, err := client.Acquire(context.Background(), []byte("renew-benchmark"), 5_000)
	if err != nil {
		b.Fatalf("Acquire: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := lease.Renew(context.Background(), 5_000); err != nil {
			b.Fatalf("Renew: %v", err)
		}
	}
	b.StopTimer()
	lease.Release()
	<-lease.releaseDone
}

func newBenchmarkClient(b *testing.B) *Client {
	b.Helper()
	client, factories := newClientWithScriptedReplicasWithoutCleanup()
	generator, err := newLeaseIDGeneratorFromReader(
		1,
		bytes.NewReader([]byte{1, 2, 3, 4}),
	)
	if err != nil {
		b.Fatalf("new lease ID generator: %v", err)
	}
	client.idGenerator = generator
	client.responseTimeout = time.Second

	done := make(chan struct{})
	var responders sync.WaitGroup
	responders.Add(ServerCount)
	for _, factory := range factories {
		stream := newReplicaFakeStream()
		factory.results <- streamFactoryResult{stream: stream}
		go func() {
			defer responders.Done()
			serveBenchmarkReplica(done, stream)
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := client.WaitReady(ctx); err != nil {
		cancel()
		b.Fatalf("WaitReady: %v", err)
	}
	cancel()

	b.Cleanup(func() {
		_ = client.Close()
		close(done)
		responders.Wait()
	})
	return client
}

func serveBenchmarkReplica(done <-chan struct{}, stream *fakeLeaseClientStream) {
	for {
		select {
		case request := <-stream.sent:
			response := benchmarkResponse(request)
			select {
			case stream.receive <- fakeReceive{response: response}:
			case <-done:
				return
			}
		case <-done:
			return
		}
	}
}

func benchmarkResponse(request *redleasev1.ClientRequest) *redleasev1.ServerResponse {
	response := &redleasev1.ServerResponse{RequestId: request.GetRequestId()}
	switch operation := request.Operation.(type) {
	case *redleasev1.ClientRequest_Acquire:
		response.Result = &redleasev1.ServerResponse_Acquire{
			Acquire: &redleasev1.AcquireResponse{
				Status: redleasev1.LeaseStatus_LEASE_STATUS_OK,
				TtlMs:  min(operation.Acquire.GetRequestedTtlMs(), uint64(5_000)),
			},
		}
	case *redleasev1.ClientRequest_Renew:
		response.Result = &redleasev1.ServerResponse_Renew{
			Renew: &redleasev1.RenewResponse{
				Status: redleasev1.LeaseStatus_LEASE_STATUS_OK,
				TtlMs:  min(operation.Renew.GetRequestedTtlMs(), uint64(5_000)),
			},
		}
	case *redleasev1.ClientRequest_Release:
		response.Result = &redleasev1.ServerResponse_Release{
			Release: &redleasev1.ReleaseResponse{Status: redleasev1.LeaseStatus_LEASE_STATUS_OK},
		}
	default:
		panic("unexpected benchmark request")
	}
	return response
}
