package client

import (
	"context"
	"sync"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

func BenchmarkClientAcquireRelease(b *testing.B) {
	client := newBenchmarkClient(b)
	b.ReportAllocs()
	b.ResetTimer()

	for iteration := range b.N {
		key := uint64(iteration)
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
	lease, err := client.Acquire(context.Background(), uint64(1), 5_000)
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
	client.clientID = 1
	client.bootID = 1
	client.responseTimeout = time.Second

	done := make(chan struct{})
	var responders sync.WaitGroup
	responders.Add(testServerCount)
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

func benchmarkResponse(request observedRequest) protocol.Response {
	response := protocol.Response{
		RequestID: request.RequestID,
		Operation: request.Operation,
		Status:    redleasev1.LeaseStatusOK,
	}
	switch request.Operation {
	case redleasev1.ClientOperationACQUIRE, redleasev1.ClientOperationRENEW:
		response.TTLMS = min(request.RequestedTTLMS, uint64(5_000))
	case redleasev1.ClientOperationRELEASE:
	default:
		panic("unexpected benchmark request")
	}
	return response
}
