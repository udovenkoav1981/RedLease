package clientmetrics

import (
	"sync"
	"testing"
	"time"

	publicmetrics "github.com/udovenkoav1981/RedLease/clientmetrics"
)

func TestAcquireLatencyBucketPowerOfTwoBoundaries(t *testing.T) {
	const firstBucketExponent = 17

	bounds := publicmetrics.AcquireLatencyBucketUpperBounds()
	for index, bound := range bounds {
		if want := time.Nanosecond << (firstBucketExponent + index); bound != want {
			t.Fatalf("bound %d = %s, want %s", index, bound, want)
		}
		if got := acquireLatencyBucket(bound); got != index {
			t.Fatalf("bucket(%s) = %d, want %d", bound, got, index)
		}
		if got := acquireLatencyBucket(bound + 1); got != index+1 {
			t.Fatalf("bucket(%s) = %d, want %d", bound+1, got, index+1)
		}
	}
}

func TestRecorderSeparatesAcquireResultsAndBuildsCumulativeBuckets(t *testing.T) {
	var recorder Recorder
	recorder.ObserveAcquire(1, 50*time.Microsecond, true)
	recorder.ObserveAcquire(2, 150*time.Microsecond, true)
	recorder.ObserveAcquire(3, 20*time.Second, false)

	snapshot := recorder.Snapshot()
	acquired := snapshot.AcquireLatency.Acquired
	if acquired.Count != 2 || acquired.SumMicroseconds != 200 {
		t.Fatalf("acquired histogram = %+v, want count 2 and sum 200us", acquired)
	}
	if acquired.Buckets[0].Count != 1 || acquired.Buckets[1].Count != 2 {
		t.Fatalf("acquired leading buckets = %+v, want counts 1 and 2", acquired.Buckets[:2])
	}
	for index := 2; index < len(acquired.Buckets); index++ {
		if acquired.Buckets[index].Count != 2 {
			t.Fatalf("acquired bucket %d count = %d, want 2", index, acquired.Buckets[index].Count)
		}
	}

	notAcquired := snapshot.AcquireLatency.NotAcquired
	if notAcquired.Count != 1 || notAcquired.SumMicroseconds != 20_000_000 {
		t.Fatalf("not-acquired histogram = %+v, want count 1 and sum 20s", notAcquired)
	}
	if notAcquired.Buckets[len(notAcquired.Buckets)-1].Count != 0 {
		t.Fatal("duration above the largest finite bound was included in a finite bucket")
	}
}

func TestRecorderCountsConcurrentAcquireCalls(t *testing.T) {
	const (
		workers      = 16
		observations = 10_000
	)
	var recorder Recorder
	var wait sync.WaitGroup
	for worker := range workers {
		wait.Go(func() {
			for observation := range observations {
				recorder.ObserveAcquire(
					uint64(worker*observations+observation), //nolint:gosec // Test values are nonnegative.
					time.Millisecond,
					observation%2 == 0,
				)
			}
		})
	}
	wait.Wait()

	snapshot := recorder.Snapshot()
	wantEach := uint64(workers * observations / 2) //nolint:gosec // Test constants are positive.
	if snapshot.AcquireLatency.Acquired.Count != wantEach ||
		snapshot.AcquireLatency.NotAcquired.Count != wantEach {
		t.Fatalf("result counts = (%d, %d), want (%d, %d)",
			snapshot.AcquireLatency.Acquired.Count,
			snapshot.AcquireLatency.NotAcquired.Count,
			wantEach,
			wantEach,
		)
	}
}
