package clientmetrics

import (
	"sync/atomic"
	"testing"
	"time"
)

var benchmarkBucket int

func BenchmarkAcquireLatencyBucket(b *testing.B) {
	durations := [...]time.Duration{
		50 * time.Microsecond,
		150 * time.Microsecond,
		350 * time.Microsecond,
		750 * time.Microsecond,
		1500 * time.Microsecond,
		3500 * time.Microsecond,
		7500 * time.Microsecond,
		15 * time.Millisecond,
		35 * time.Millisecond,
		75 * time.Millisecond,
		150 * time.Millisecond,
		350 * time.Millisecond,
		750 * time.Millisecond,
		1500 * time.Millisecond,
		3500 * time.Millisecond,
		7500 * time.Millisecond,
		20 * time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	bucket := 0
	durationIndex := 0
	for range b.N {
		bucket += acquireLatencyBucket(durations[durationIndex])
		durationIndex++
		if durationIndex == len(durations) {
			durationIndex = 0
		}
	}
	benchmarkBucket = bucket
}

func BenchmarkRecorderObserveAcquire(b *testing.B) {
	var recorder Recorder
	var nextKey atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(worker *testing.PB) {
		key := nextKey.Add(1)
		for worker.Next() {
			recorder.ObserveAcquire(key, time.Millisecond, true)
			key++
		}
	})
}
