package client1of1

import (
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"testing"
)

const pendingBenchmarkShardCount = 256

type pendingBenchmarkShard struct {
	mu      sync.Mutex
	pending map[uint64]chan connectionResult
}

// BenchmarkPendingShardedMap uses the server's 256-shard layout and hash
// function for the client's requestID-to-future map. Like the single-map
// benchmark, one pair registers, finds, and deletes one pending request.
func BenchmarkPendingShardedMap(b *testing.B) {
	for _, liveEntries := range []int{0, 4096} {
		for _, workers := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096} {
			b.Run(fmt.Sprintf("live=%d/workers=%d", liveEntries, workers), func(b *testing.B) {
				benchmarkPendingShardedMap(b, liveEntries, workers)
			})
		}
	}
}

func benchmarkPendingShardedMap(b *testing.B, liveEntries, workers int) {
	b.Helper()
	seed := maphash.MakeSeed()
	shards := make([]*pendingBenchmarkShard, pendingBenchmarkShardCount)
	for index := range shards {
		shards[index] = &pendingBenchmarkShard{
			pending: make(map[uint64]chan connectionResult, (liveEntries+workers)/len(shards)+1),
		}
	}
	liveResult := make(chan connectionResult, 1)
	for index := range liveEntries {
		requestID := ^uint64(0) - uint64(index) //nolint:gosec // Benchmark counts are non-negative.
		shard := shards[maphash.Comparable(seed, requestID)%uint64(len(shards))]
		shard.pending[requestID] = liveResult
	}
	var ready sync.WaitGroup
	var done sync.WaitGroup
	var mismatch atomic.Bool
	start := make(chan struct{})

	activeWorkers := min(workers, b.N)
	ready.Add(activeWorkers)
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
				shard := shards[maphash.Comparable(seed, requestID)%uint64(len(shards))]
				shard.mu.Lock()
				shard.pending[requestID] = result
				shard.mu.Unlock()

				shard = shards[maphash.Comparable(seed, requestID)%uint64(len(shards))]
				shard.mu.Lock()
				got := shard.pending[requestID]
				delete(shard.pending, requestID)
				shard.mu.Unlock()
				if got != result {
					mismatch.Store(true)
				}
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
