package server

import (
	"fmt"
	"hash/maphash"
	"sync"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

var benchmarkShardIndex int

//go:noinline
func benchmarkShardIndexModulo(key, shardCount uint64) int {
	return int(maphash.Comparable(hashSeed, key) % shardCount)
}

//go:noinline
func benchmarkShardIndexMask(key, shardMask uint64) int {
	return int(maphash.Comparable(hashSeed, key) & shardMask)
}

func BenchmarkShardIndex(b *testing.B) {
	b.Run("maphash_modulo", func(b *testing.B) {
		key := uint64(1)
		b.ReportAllocs()
		for range b.N {
			key = uint64(benchmarkShardIndexModulo(key, defaultShardCount)) + 1
		}
		benchmarkShardIndex = int(key)
	})
	b.Run("maphash_mask", func(b *testing.B) {
		key := uint64(1)
		b.ReportAllocs()
		for range b.N {
			key = uint64(benchmarkShardIndexMask(key, defaultShardCount-1)) + 1
		}
		benchmarkShardIndex = int(key)
	})
}

// BenchmarkServerLeaseStorage isolates the sharded map, deadline heap and
// shard mutexes from the request queue, protocol and response allocation.
func BenchmarkServerLeaseStorage(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			server := &Server{
				shards:    make([]*leaseShard, defaultShardCount),
				shardMask: defaultShardCount - 1,
			}
			deadline := time.Now().Add(time.Minute)
			for index := range server.shards {
				shard := &leaseShard{leases: make(map[uint64]*lease)}
				server.shards[index] = shard
			}

			runFixedWorkerBenchmark(b, workers, "lease-cycles/s", func(worker, first, operationCount int) {
				id := leaseID{clientID: uint32(worker + 1), bootID: 1} //nolint:gosec // Benchmark sizes are bounded.
				for operationIndex := first; operationIndex < first+operationCount; operationIndex++ {
					key := uint64(operationIndex + 1) //nolint:gosec // Benchmark sizes are bounded.
					id.leaseSeq = key
					shard := server.shards[server.shardIndex(key)]

					shard.mu.Lock()
					shard.addLease(key, id, deadline)
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
// machine while excluding the shard queue and TCP transport.
func BenchmarkServerApplyAcquireRelease(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			server := &Server{
				config: Config{
					MaxTTL:  uint64(ProtocolMaxTTL / time.Millisecond),
					MaxKeys: DefaultMaxKeys,
				},
				shards:    make([]*leaseShard, defaultShardCount),
				shardMask: defaultShardCount - 1,
			}
			for index := range server.shards {
				server.shards[index] = &leaseShard{leases: make(map[uint64]*lease)}
			}
			server.phase.Store(uint32(phaseActive))

			runFixedWorkerBenchmark(b, workers, "acquire-release-pairs/s", func(worker, first, operationCount int) {
				id := leaseID{clientID: uint32(worker + 1), bootID: 1} //nolint:gosec // Benchmark sizes are bounded.
				for operationIndex := first; operationIndex < first+operationCount; operationIndex++ {
					key := uint64(operationIndex + 1) //nolint:gosec // Benchmark sizes are bounded.
					id.leaseSeq = key

					acquire := server.apply(server.shards[server.shardIndex(key)], operation{
						requestID:      key,
						kind:           operationAcquire,
						key:            key,
						leaseID:        id,
						requestedTTLMS: server.config.MaxTTL,
					})
					if status := acquire.Status; status != redleasev1.LeaseStatusOK {
						b.Errorf("Acquire status = %s", status)
						return
					}

					release := server.apply(server.shards[server.shardIndex(key)], operation{
						requestID: key,
						kind:      operationRelease,
						key:       key,
						leaseID:   id,
					})
					if status := release.Status; status != redleasev1.LeaseStatusOK {
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
	s := newTestServerWithConfig(b, Config{
		MaxTTL: uint64(ProtocolMaxTTL / time.Millisecond),
		Logger: testLogger,
	})
	s.phase.Store(uint32(phaseActive))

	session := newOperationTestSession(s, 2)
	b.ReportAllocs()
	b.ResetTimer()

	for iteration := range b.N {
		key := uint64(iteration)
		id := leaseID{clientID: 1, bootID: 1, leaseSeq: uint64(iteration + 1)}
		dispatchTestOperation(b, s, session, operation{
			kind:           operationAcquire,
			key:            key,
			leaseID:        id,
			requestedTTLMS: uint64(ProtocolMaxTTL / time.Millisecond),
		})
		if status := receiveBenchmarkOperationResponse(b, s, session).Status; status != redleasev1.LeaseStatusOK {
			b.Fatalf("Acquire status = %s", status)
		}

		dispatchTestOperation(b, s, session, operation{kind: operationRelease, key: key, leaseID: id})
		if status := receiveBenchmarkOperationResponse(b, s, session).Status; status != redleasev1.LeaseStatusOK {
			b.Fatalf("Release status = %s", status)
		}
	}
}

func receiveBenchmarkOperationResponse(
	b *testing.B,
	s *Server,
	session *connectionSession,
) protocol.Response {
	b.Helper()
	for {
		outbound, ok := session.respQueue.TryDequeue()
		if !ok {
			<-session.respQueue.Ready()
			continue
		}
		response, err := transport.DecodeResponse(outbound.message.Table().Bytes)
		s.recycleOutboundResponse(outbound)
		if err != nil {
			b.Fatalf("decode queued operation response: %v", err)
		}
		return response
	}
}
