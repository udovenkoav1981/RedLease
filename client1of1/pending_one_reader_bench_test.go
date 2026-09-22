package client1of1

import (
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkPendingOneReader compares a single map with 256 sharded maps when
// many callers register requests but one reader completes every response.
// A channel hands request IDs to that reader in lieu of network responses.
func BenchmarkPendingOneReader(b *testing.B) {
	for _, shardsCount := range []int{1, pendingBenchmarkShardCount} {
		for _, workers := range []int{1, 2, 4, 8, 16, 32, 64, 256, 4096} {
			b.Run(fmt.Sprintf("shards=%d/workers=%d", shardsCount, workers), func(b *testing.B) {
				benchmarkPendingOneReader(b, shardsCount, workers)
			})
		}
	}
}

func benchmarkPendingOneReader(b *testing.B, shardsCount, workers int) {
	b.Helper()
	const liveEntries = 4096
	seed := maphash.MakeSeed()
	shards := make([]*pendingBenchmarkShard, shardsCount)
	for index := range shards {
		shards[index] = &pendingBenchmarkShard{
			pending: make(map[uint64]chan connectionResult, (liveEntries+workers)/shardsCount+1),
		}
	}
	shardIndex := func(requestID uint64) int {
		if shardsCount == 1 {
			return 0
		}
		return int(maphash.Comparable(seed, requestID) % uint64(shardsCount)) //nolint:gosec // Shard count is 256.
	}
	liveResult := make(chan connectionResult, 1)
	for index := range liveEntries {
		requestID := ^uint64(0) - uint64(index) //nolint:gosec // Benchmark counts are non-negative.
		shards[shardIndex(requestID)].pending[requestID] = liveResult
	}

	responses := make(chan uint64, 4096)
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	var mismatch atomic.Bool
	activeWorkers := min(workers, b.N)
	ready.Add(activeWorkers + 1)
	done.Go(func() {
		ready.Done()
		<-start
		for range b.N {
			requestID := <-responses
			shard := shards[shardIndex(requestID)]
			shard.mu.Lock()
			result := shard.pending[requestID]
			delete(shard.pending, requestID)
			shard.mu.Unlock()
			if result == nil {
				mismatch.Store(true)
			}
		}
	})
	for worker := range activeWorkers {
		operations := b.N / activeWorkers
		if worker < b.N%activeWorkers {
			operations++
		}
		base := uint64(worker) * (uint64(b.N) + 1) //nolint:gosec // Benchmark counts are non-negative.
		result := make(chan connectionResult, 1)
		done.Go(func() {
			ready.Done()
			<-start
			for index := range operations {
				requestID := base + uint64(index) + 1 //nolint:gosec // Benchmark counts are non-negative.
				shard := shards[shardIndex(requestID)]
				shard.mu.Lock()
				shard.pending[requestID] = result
				shard.mu.Unlock()
				responses <- requestID
			}
		})
	}

	ready.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	remaining := 0
	for _, shard := range shards {
		remaining += len(shard.pending)
	}
	if mismatch.Load() || remaining != liveEntries {
		b.Fatal("pending response was correlated with the wrong request")
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "pairs/s")
}
