package clientmetrics

import "time"

// AcquireLatencyBucketCount is the number of finite latency buckets recorded
// for each Acquire result. Durations above the final bound are included in
// Count but not in a finite bucket.
const (
	AcquireLatencyBucketCount         = 17
	firstAcquireLatencyBucketExponent = 17
)

// LatencyBucket is one cumulative latency-histogram bucket.
type LatencyBucket struct {
	UpperBound time.Duration
	Count      uint64
}

// LatencyHistogram is a cumulative latency histogram. SumMicroseconds is the
// sum of observed durations truncated to microseconds.
type LatencyHistogram struct {
	Count           uint64
	SumMicroseconds uint64
	Buckets         [AcquireLatencyBucketCount]LatencyBucket
}

// AcquireLatency contains separate distributions for successful and
// unsuccessful Acquire calls.
type AcquireLatency struct {
	Acquired    LatencyHistogram
	NotAcquired LatencyHistogram
}

// Snapshot is a lock-free, point-in-time view of client metrics. Individual
// fields may have been observed at slightly different instants.
type Snapshot struct {
	AcquireLatency AcquireLatency
}

// AcquireLatencyBucketUpperBounds returns power-of-two nanosecond boundaries
// from 2^17 ns through 2^33 ns.
func AcquireLatencyBucketUpperBounds() [AcquireLatencyBucketCount]time.Duration {
	var bounds [AcquireLatencyBucketCount]time.Duration
	for index := range bounds {
		bounds[index] = time.Nanosecond << (firstAcquireLatencyBucketExponent + index)
	}
	return bounds
}
