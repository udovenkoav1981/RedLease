package client1of1_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redleaseclient "github.com/udovenkoav1981/RedLease/client1of1"
)

const (
	benchmarkTargetEnvironment = "REDLEASE_BENCH_TARGET"
	defaultBenchmarkTarget     = "127.0.0.1:50051"
	benchmarkTTLMS             = uint64(1000)
	benchmarkResponseTimeoutMS = uint32(5000)
	benchmarkReadyTimeout      = 10 * time.Second
	latencySubdivisions        = 4
	latencyBuckets             = 128
)

type benchmarkLatencyHistogram [latencyBuckets]uint64

type benchmarkWorkerStats struct {
	count     uint64
	total     time.Duration
	latencies benchmarkLatencyHistogram
}

// BenchmarkClient1Of1AcquireRelease uses one real TCP connection to an
// externally started RedLease server.
func BenchmarkClient1Of1AcquireRelease(b *testing.B) {
	target := os.Getenv(benchmarkTargetEnvironment)
	if target == "" {
		target = defaultBenchmarkTarget
	}

	for _, workers := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			client := newExternalBenchmarkClient(b, target)
			firstKey := randomBenchmarkKey(b)
			keys := make([]uint64, workers)
			for worker := range workers {
				keys[worker] = firstKey + uint64(worker) //nolint:gosec // worker is non-negative.
			}

			waitForBenchmarkServer(b, client, firstKey-1)
			runClientWorkers(b, client, keys)
			drainBenchmarkReleases(b, client, keys)
		})
	}
}

func newExternalBenchmarkClient(b *testing.B, target string) *redleaseclient.Client {
	b.Helper()
	client, err := redleaseclient.New(redleaseclient.Config{
		ClientID:        1,
		Target:          target,
		Logger:          slog.New(slog.DiscardHandler),
		ResponseTimeout: benchmarkResponseTimeoutMS,
	})
	if err != nil {
		b.Fatalf("create client1of1: %v", err)
	}
	b.Cleanup(func() {
		if err := client.Close(); err != nil {
			b.Errorf("close client1of1: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), benchmarkReadyTimeout)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		b.Fatalf("wait for client1of1 connection to %s: %v", target, err)
	}
	return client
}

func waitForBenchmarkServer(b *testing.B, client *redleaseclient.Client, key uint64) {
	b.Helper()
	deadline := time.Now().Add(benchmarkReadyTimeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		lease, err := client.Acquire(ctx, key, benchmarkTTLMS)
		cancel()
		if err == nil {
			lease.Release()
			drainBenchmarkReleases(b, client, []uint64{key})
			return
		}
		if !errors.Is(err, redleaseclient.ErrNotAcquired) || time.Now().After(deadline) {
			b.Fatalf("wait for active benchmark server: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func runClientWorkers(b *testing.B, client *redleaseclient.Client, keys []uint64) {
	b.Helper()
	workers := len(keys)
	workerStats := make([]benchmarkWorkerStats, workers)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(workers)
	done.Add(workers)

	var failed atomic.Bool
	failure := make(chan error, 1)
	for worker, key := range keys {
		stats := &workerStats[worker]
		operationCount := b.N / workers
		if worker < b.N%workers {
			operationCount++
		}

		go func() {
			defer done.Done()
			ready.Done()
			<-start
			for range operationCount {
				if failed.Load() {
					return
				}
				started := time.Now()
				lease, err := client.Acquire(context.Background(), key, benchmarkTTLMS)
				if err != nil {
					if failed.CompareAndSwap(false, true) {
						failure <- fmt.Errorf("worker %d Acquire: %w", worker, err)
					}
					return
				}
				lease.Release()
				stats.observe(time.Since(started))
			}
		}()
	}

	ready.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	if failed.Load() {
		b.Fatal(<-failure)
	}
	var combined benchmarkWorkerStats
	for index := range workerStats {
		combined.merge(&workerStats[index])
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "acquire-release-pairs/s")
	b.ReportMetric(float64(combined.total)/float64(combined.count)/float64(time.Millisecond), "avg-latency-ms")
	b.ReportMetric(float64(combined.percentile(50))/float64(time.Millisecond), "p50-latency-ms")
	b.ReportMetric(float64(combined.percentile(95))/float64(time.Millisecond), "p95-latency-ms")
	b.ReportMetric(float64(combined.percentile(99))/float64(time.Millisecond), "p99-latency-ms")
}

func (s *benchmarkWorkerStats) observe(elapsed time.Duration) {
	microseconds := uint64(max(1, elapsed.Microseconds()))
	exponent := uint64(bits.Len64(microseconds)) - 1 //nolint:gosec // Positive input makes bits.Len64 return 1..64.
	base := uint64(1) << exponent
	subdivision := (microseconds - base) * latencySubdivisions / base
	index := min(exponent*latencySubdivisions+subdivision, latencyBuckets-1)
	s.count++
	s.total += elapsed
	s.latencies[index]++
}

func (s *benchmarkWorkerStats) merge(other *benchmarkWorkerStats) {
	s.count += other.count
	s.total += other.total
	for index, count := range other.latencies {
		s.latencies[index] += count
	}
}

func (s *benchmarkWorkerStats) percentile(percentage uint64) time.Duration {
	target := (s.count*percentage + 99) / 100
	var seen uint64
	for index, count := range s.latencies {
		seen += count
		if seen >= target {
			exponent := index / latencySubdivisions
			subdivision := index % latencySubdivisions
			base := time.Microsecond << exponent
			return base + base*time.Duration(subdivision+1)/time.Duration(latencySubdivisions)
		}
	}
	return 0
}

func drainBenchmarkReleases(b *testing.B, client *redleaseclient.Client, keys []uint64) {
	b.Helper()
	// Release has no client-visible acknowledgement. A subsequent zero-TTL
	// Acquire for the same key is ordered behind it on the same server shard;
	// its response proves the Release was processed without leaving a new lease.
	var drains sync.WaitGroup
	errorsSeen := make(chan error, len(keys))
	for _, key := range keys {
		drains.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), benchmarkReadyTimeout)
			defer cancel()
			lease, err := client.Acquire(ctx, key, 0)
			if lease != nil || !errors.Is(err, redleaseclient.ErrNotAcquired) || ctx.Err() != nil {
				errorsSeen <- fmt.Errorf("zero-TTL barrier for key %d returned lease=%v, error=%w", key, lease != nil, err)
			}
		})
	}
	drains.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		b.Error(err)
	}
}

func randomBenchmarkKey(b *testing.B) uint64 {
	b.Helper()
	var encoded [8]byte
	if _, err := rand.Read(encoded[:]); err != nil {
		b.Fatalf("generate benchmark key: %v", err)
	}
	return binary.LittleEndian.Uint64(encoded[:])
}
