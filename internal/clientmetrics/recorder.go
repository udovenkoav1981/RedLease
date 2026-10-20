package clientmetrics

import (
	"math/bits"
	"sync/atomic"
	"time"

	publicmetrics "github.com/udovenkoav1981/RedLease/clientmetrics"
)

const (
	recorderShardCount = 64
	overflowBucket     = publicmetrics.AcquireLatencyBucketCount
)

type latencyHistogram struct {
	buckets         [publicmetrics.AcquireLatencyBucketCount + 1]atomic.Uint64
	sumMicroseconds atomic.Uint64
}

type recorderShard struct {
	acquired    latencyHistogram
	notAcquired latencyHistogram
	_           [16]byte // Pad 304 bytes of counters to a 320-byte cache-line-aligned stride.
}

// Recorder collects latency without locks or allocations on the operation
// path. A stable per-operation key distributes writers across independent
// cache lines.
type Recorder struct {
	shards [recorderShardCount]recorderShard
}

// ObserveAcquire records one completed Acquire call.
func (r *Recorder) ObserveAcquire(
	shardKey uint64,
	elapsed time.Duration,
	acquired bool,
) {
	if elapsed < 0 {
		elapsed = 0
	}
	microseconds := uint64(elapsed.Microseconds()) //nolint:gosec // elapsed was clamped to a nonnegative duration.
	shard := &r.shards[shardKey&(recorderShardCount-1)]
	histogram := &shard.notAcquired
	if acquired {
		histogram = &shard.acquired
	}
	histogram.buckets[acquireLatencyBucket(elapsed)].Add(1)
	histogram.sumMicroseconds.Add(microseconds)
}

// Snapshot returns the current cumulative metrics.
func (r *Recorder) Snapshot() publicmetrics.Snapshot {
	return publicmetrics.Snapshot{
		AcquireLatency: publicmetrics.AcquireLatency{
			Acquired:    r.snapshotHistogram(true),
			NotAcquired: r.snapshotHistogram(false),
		},
	}
}

func (r *Recorder) snapshotHistogram(acquired bool) publicmetrics.LatencyHistogram {
	bounds := publicmetrics.AcquireLatencyBucketUpperBounds()
	var snapshot publicmetrics.LatencyHistogram
	var cumulative uint64
	for bucket := range publicmetrics.AcquireLatencyBucketCount + 1 {
		var count uint64
		for shard := range &r.shards {
			histogram := &r.shards[shard].notAcquired
			if acquired {
				histogram = &r.shards[shard].acquired
			}
			count += histogram.buckets[bucket].Load()
			if bucket == 0 {
				snapshot.SumMicroseconds += histogram.sumMicroseconds.Load()
			}
		}
		cumulative += count
		if bucket < publicmetrics.AcquireLatencyBucketCount {
			snapshot.Buckets[bucket] = publicmetrics.LatencyBucket{
				UpperBound: bounds[bucket],
				Count:      cumulative,
			}
		}
	}
	snapshot.Count = cumulative
	return snapshot
}

func acquireLatencyBucket(elapsed time.Duration) int {
	const firstBucketExponent = 17

	if elapsed <= time.Nanosecond<<firstBucketExponent {
		return 0
	}
	nanoseconds := uint64(elapsed) //nolint:gosec // A positive time.Duration always fits in uint64.
	return min(bits.Len64(nanoseconds-1)-firstBucketExponent, overflowBucket)
}
