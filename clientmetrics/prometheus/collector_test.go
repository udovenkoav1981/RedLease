package prometheus

import (
	"testing"

	clientprometheus "github.com/prometheus/client_golang/prometheus"

	redleaseclient "github.com/udovenkoav1981/RedLease/client"
	redleaseclient1of1 "github.com/udovenkoav1981/RedLease/client1of1"
	"github.com/udovenkoav1981/RedLease/clientmetrics"
)

var (
	_ Source = (*redleaseclient.Client)(nil)
	_ Source = (*redleaseclient1of1.Client)(nil)
)

type fixedSource struct {
	snapshot clientmetrics.Snapshot
}

func (s *fixedSource) MetricsSnapshot() clientmetrics.Snapshot {
	return s.snapshot
}

func TestNewCollectorRejectsNilSource(t *testing.T) {
	var source *fixedSource
	if _, err := NewCollector(source); err == nil {
		t.Fatal("NewCollector(nil) succeeded")
	}
}

func TestCollectorExportsAcquireLatencyByResult(t *testing.T) {
	bounds := clientmetrics.AcquireLatencyBucketUpperBounds()
	snapshot := clientmetrics.Snapshot{
		AcquireLatency: clientmetrics.AcquireLatency{
			Acquired: clientmetrics.LatencyHistogram{
				Count:           3,
				SumMicroseconds: 4_500,
			},
			NotAcquired: clientmetrics.LatencyHistogram{
				Count:           2,
				SumMicroseconds: 7_000,
			},
		},
	}
	for index, bound := range bounds {
		snapshot.AcquireLatency.Acquired.Buckets[index] = clientmetrics.LatencyBucket{
			UpperBound: bound,
			Count:      min(uint64(index+1), 3), //nolint:gosec // The index is nonnegative.
		}
		snapshot.AcquireLatency.NotAcquired.Buckets[index] = clientmetrics.LatencyBucket{
			UpperBound: bound,
			Count:      min(uint64(index), 2), //nolint:gosec // The index is nonnegative.
		}
	}

	collector, err := NewCollector(&fixedSource{snapshot: snapshot})
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	registry := clientprometheus.NewRegistry()
	if err := registry.Register(collector); err != nil {
		t.Fatalf("Register: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(families) != 1 || families[0].GetName() != "redlease_client_acquire_duration_seconds" {
		t.Fatalf("metric families = %+v", families)
	}

	want := map[string]struct {
		count uint64
		sum   float64
	}{
		resultAcquired:    {count: 3, sum: float64(4_500) / 1_000_000},
		resultNotAcquired: {count: 2, sum: float64(7_000) / 1_000_000},
	}
	for _, metric := range families[0].GetMetric() {
		result := metric.GetLabel()[0].GetValue()
		expected, ok := want[result]
		if !ok {
			t.Fatalf("unexpected result label %q", result)
		}
		histogram := metric.GetHistogram()
		if histogram.GetSampleCount() != expected.count || histogram.GetSampleSum() != expected.sum {
			t.Errorf("result %q = (count %d, sum %v), want (%d, %v)",
				result,
				histogram.GetSampleCount(),
				histogram.GetSampleSum(),
				expected.count,
				expected.sum,
			)
		}
		delete(want, result)
	}
	if len(want) != 0 {
		t.Fatalf("missing result histograms: %v", want)
	}
}
