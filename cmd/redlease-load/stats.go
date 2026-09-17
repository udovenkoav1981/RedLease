package main

import (
	"math/bits"
	"strconv"
	"time"
)

const (
	latencySubdivisions = 4
	latencyBuckets      = 128
	decimalBase         = 10
	percentScale        = 100
	percentRoundup      = percentScale - 1
	floatBitSize        = 64
)

// The logarithmic histogram bounds memory independently of benchmark length.
// Each power-of-two range is split into four buckets (about 25% precision).
type latencyHistogram [latencyBuckets]uint64

func (h *latencyHistogram) observe(elapsed time.Duration) {
	microseconds := uint64(max(1, elapsed.Microseconds()))
	exponent := uint64(bits.Len64(microseconds)) - 1 //nolint:gosec // Positive input makes bits.Len64 return 1..64.
	base := uint64(1) << exponent
	subdivision := (microseconds - base) * latencySubdivisions / base
	index := min(exponent*latencySubdivisions+subdivision, latencyBuckets-1)
	h[index]++
}

func (h *latencyHistogram) merge(other *latencyHistogram) {
	for index, count := range other {
		h[index] += count
	}
}

func (h *latencyHistogram) percentile(total, percentage uint64) time.Duration {
	if total == 0 {
		return 0
	}
	target := (total*percentage + percentRoundup) / percentScale
	var seen uint64
	for index, count := range h {
		seen += count
		if seen >= target {
			exponent := index / latencySubdivisions
			subdivision := index % latencySubdivisions
			base := time.Microsecond << exponent
			return base + base*time.Duration(subdivision+1)/latencySubdivisions
		}
	}
	return 0
}

type workerStats struct {
	succeeded    uint64
	failed       uint64
	keyLimit     uint64
	expired      uint64
	totalLatency time.Duration
	latencies    latencyHistogram
}

type caseResult struct {
	elapsed      time.Duration
	succeeded    uint64
	failed       uint64
	keyLimit     uint64
	expired      uint64
	totalLatency time.Duration
	latencies    latencyHistogram
	perClient    []uint64
}

func summarize(stats []workerStats, clientCount, leaseCount int, elapsed time.Duration) caseResult {
	result := caseResult{elapsed: elapsed, perClient: make([]uint64, clientCount)}
	for index := range stats {
		worker := &stats[index]
		result.succeeded += worker.succeeded
		result.failed += worker.failed
		result.keyLimit += worker.keyLimit
		result.expired += worker.expired
		result.totalLatency += worker.totalLatency
		result.latencies.merge(&worker.latencies)
		result.perClient[index/leaseCount] += worker.succeeded
	}
	return result
}

func (r *caseResult) csvRecord(selected scenario, quorum string, clients, leases int) []string {
	seconds := r.elapsed.Seconds()
	var minClient, maxClient uint64
	for index, count := range r.perClient {
		if index == 0 {
			minClient = count
		}
		minClient = min(minClient, count)
		maxClient = max(maxClient, count)
	}
	var fairness, average float64
	if maxClient > 0 {
		fairness = float64(minClient) / float64(maxClient)
	}
	if r.succeeded > 0 {
		average = float64(r.totalLatency) / float64(r.succeeded) / float64(time.Millisecond)
	}
	return []string{
		selected.name,
		quorum,
		strconv.Itoa(clients),
		strconv.Itoa(leases),
		formatFloat(seconds),
		strconv.FormatUint(r.succeeded, decimalBase),
		strconv.FormatUint(r.failed, decimalBase),
		strconv.FormatUint(r.keyLimit, decimalBase),
		strconv.FormatUint(r.expired, decimalBase),
		formatFloat(float64(r.succeeded) / seconds),
		formatFloat(float64(minClient) / seconds),
		formatFloat(float64(maxClient) / seconds),
		formatFloat(fairness),
		formatFloat(average),
		formatFloat(float64(r.latencies.percentile(r.succeeded, 50)) / float64(time.Millisecond)),
		formatFloat(float64(r.latencies.percentile(r.succeeded, 95)) / float64(time.Millisecond)),
		formatFloat(float64(r.latencies.percentile(r.succeeded, 99)) / float64(time.Millisecond)),
	}
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 3, floatBitSize)
}
