package client1of1

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkPendingMutexMap isolates the two critical sections used for one
// request/response pair: register a pending request, then find and delete it.
// It intentionally excludes FlatBuffers, the send queue, TCP, and response
// channels, so the result is an upper bound for this part of the client path.
func BenchmarkPendingMutexMap(b *testing.B) {
	for _, liveEntries := range []int{0, 4096} {
		for _, workers := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096} {
			b.Run(fmt.Sprintf("live=%d/workers=%d", liveEntries, workers), func(b *testing.B) {
				benchmarkPendingMutexMap(b, liveEntries, workers)
			})
		}
	}
}

func benchmarkPendingMutexMap(b *testing.B, liveEntries, workers int) {
	b.Helper()
	var mu sync.Mutex
	pending := make(map[uint64]chan connectionResult, liveEntries+workers)
	liveResult := make(chan connectionResult, 1)
	for index := range liveEntries {
		pending[^uint64(0)-uint64(index)] = liveResult //nolint:gosec // Benchmark counts are non-negative.
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
				mu.Lock()
				pending[requestID] = result
				mu.Unlock()

				mu.Lock()
				got := pending[requestID]
				delete(pending, requestID)
				mu.Unlock()
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
	if mismatch.Load() || len(pending) != liveEntries {
		b.Fatal("pending response was correlated with the wrong request")
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "pairs/s")
}
