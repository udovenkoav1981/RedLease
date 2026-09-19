package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

// BenchmarkServerLeaseStorage isolates the sharded map, deadline heap and
// shard mutexes from the request queue, protocol and response allocation.
func BenchmarkServerLeaseStorage(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			server := &Server{shards: make([]*leaseShard, defaultShardCount)}
			for index := range server.shards {
				shard := &leaseShard{leases: make(map[uint64]*lease)}
				server.shards[index] = shard
			}

			runFixedWorkerBenchmark(b, workers, "lease-cycles/s", func(worker, first, operationCount int) {
				id := leaseID{clientID: uint32(worker + 1), bootID: 1}
				for operationIndex := first; operationIndex < first+operationCount; operationIndex++ {
					key := uint64(operationIndex + 1)
					id.leaseSeq = key
					shard := server.shards[server.shardIndex(key)]

					shard.mu.Lock()
					shard.addLease(key, id, 1)
					shard.mu.Unlock()

					shard = server.shards[server.shardIndex(key)]
					shard.mu.Lock()
					current := shard.leases[key]
					shard.removeLease(current)
					shard.mu.Unlock()
				}
			})
		})
	}
}

// BenchmarkServerApplyAcquireRelease measures the complete in-memory state
// machine while excluding the shard queue, stream and gRPC transport.
func BenchmarkServerApplyAcquireRelease(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			server := &Server{
				config: Config{
					MaxTTL:  uint64(ProtocolMaxTTL / time.Millisecond),
					MaxKeys: DefaultMaxKeys,
				},
				shards: make([]*leaseShard, defaultShardCount),
			}
			for index := range server.shards {
				server.shards[index] = &leaseShard{leases: make(map[uint64]*lease)}
			}
			server.phase.Store(uint32(phaseActive))

			runFixedWorkerBenchmark(b, workers, "acquire-release-pairs/s", func(worker, first, operationCount int) {
				id := leaseID{clientID: uint32(worker + 1), bootID: 1}
				for operationIndex := first; operationIndex < first+operationCount; operationIndex++ {
					key := uint64(operationIndex + 1)
					id.leaseSeq = key

					acquire := server.apply(server.shards[server.shardIndex(key)], operation{
						requestID:      key,
						kind:           operationAcquire,
						key:            key,
						leaseID:        id,
						requestedTTLMS: server.config.MaxTTL,
					})
					if status := acquire.GetAcquire().GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
						b.Errorf("Acquire status = %s", status)
						return
					}

					release := server.apply(server.shards[server.shardIndex(key)], operation{
						requestID: key,
						kind:      operationRelease,
						key:       key,
						leaseID:   id,
					})
					if status := release.GetRelease().GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
						b.Errorf("Release status = %s", status)
						return
					}
				}
			})
			if keys := server.keys.Load(); keys != 0 {
				b.Fatalf("resident keys = %d, want 0", keys)
			}
		})
	}
}

func runFixedWorkerBenchmark(
	b *testing.B,
	workers int,
	throughputMetric string,
	work func(worker, first, operationCount int),
) {
	b.Helper()

	var ready sync.WaitGroup
	var done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(workers)
	done.Add(workers)

	firstOperation := 0
	for worker := range workers {
		operationCount := b.N / workers
		if worker < b.N%workers {
			operationCount++
		}
		first := firstOperation
		firstOperation += operationCount

		go func(worker, first, operationCount int) {
			defer done.Done()
			ready.Done()
			<-start
			work(worker, first, operationCount)
		}(worker, first, operationCount)
	}

	ready.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), throughputMetric)
}

func BenchmarkServerAcquireReleaseQueue(b *testing.B) {
	s, err := New(Config{
		MaxTTL: uint64(ProtocolMaxTTL / time.Millisecond),
		Logger: testLogger,
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	s.phase.Store(uint32(phaseActive))
	b.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	responses := make(chan *redleasev1.ServerResponse, 1)
	complete := func(response *redleasev1.ServerResponse) { responses <- response }
	b.ReportAllocs()
	b.ResetTimer()

	for iteration := range b.N {
		key := uint64(iteration)
		id := leaseID{clientID: 1, bootID: 1, leaseSeq: uint64(iteration + 1)}
		if !s.dispatch(ctx.Done(), shardJob{
			operation: operation{
				kind:           operationAcquire,
				key:            key,
				leaseID:        id,
				requestedTTLMS: uint64(ProtocolMaxTTL / time.Millisecond),
			},
			complete: complete,
		}) {
			b.Fatal("dispatch Acquire")
		}
		if status := (<-responses).GetAcquire().GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
			b.Fatalf("Acquire status = %s", status)
		}

		if !s.dispatch(ctx.Done(), shardJob{
			operation: operation{kind: operationRelease, key: key, leaseID: id},
			complete:  complete,
		}) {
			b.Fatal("dispatch Release")
		}
		if status := (<-responses).GetRelease().GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
			b.Fatalf("Release status = %s", status)
		}
	}
}
