package main

import (
	"context"
	"errors"
	"testing"

	"github.com/udovenkoav1981/RedLease/clientmetrics"
)

type fixedMetricsLoadClient struct {
	snapshot clientmetrics.Snapshot
}

var errFixedMetricsAcquire = errors.New("fixed metrics client does not acquire leases")

func (c *fixedMetricsLoadClient) WaitReady(context.Context) error { return nil }
func (c *fixedMetricsLoadClient) Close() error                    { return nil }
func (c *fixedMetricsLoadClient) MetricsSnapshot() clientmetrics.Snapshot {
	return c.snapshot
}

func (c *fixedMetricsLoadClient) Acquire(context.Context, uint64, uint64) (loadLease, error) {
	return nil, errFixedMetricsAcquire
}

func TestLoadMetricsSourceAggregatesCurrentClients(t *testing.T) {
	first := clientmetrics.Snapshot{}
	first.AcquireLatency.Acquired.Count = 2
	first.AcquireLatency.Acquired.SumMicroseconds = 300
	first.AcquireLatency.Acquired.Buckets[0].Count = 1
	first.AcquireLatency.NotAcquired.Count = 1
	first.AcquireLatency.NotAcquired.SumMicroseconds = 500
	first.AcquireLatency.NotAcquired.Buckets[0].Count = 1

	second := clientmetrics.Snapshot{}
	second.AcquireLatency.Acquired.Count = 3
	second.AcquireLatency.Acquired.SumMicroseconds = 700
	second.AcquireLatency.Acquired.Buckets[0].Count = 2

	source := &loadMetricsSource{}
	source.setClients([]loadClient{
		&fixedMetricsLoadClient{snapshot: first},
		&fixedMetricsLoadClient{snapshot: second},
	})
	snapshot := source.MetricsSnapshot()
	if snapshot.AcquireLatency.Acquired.Count != 5 ||
		snapshot.AcquireLatency.Acquired.SumMicroseconds != 1000 ||
		snapshot.AcquireLatency.Acquired.Buckets[0].Count != 3 {
		t.Fatalf("acquired histogram = %+v", snapshot.AcquireLatency.Acquired)
	}
	if snapshot.AcquireLatency.NotAcquired.Count != 1 ||
		snapshot.AcquireLatency.NotAcquired.SumMicroseconds != 500 ||
		snapshot.AcquireLatency.NotAcquired.Buckets[0].Count != 1 {
		t.Fatalf("not-acquired histogram = %+v", snapshot.AcquireLatency.NotAcquired)
	}
	wantBounds := clientmetrics.AcquireLatencyBucketUpperBounds()
	for index, bound := range wantBounds {
		if got := snapshot.AcquireLatency.Acquired.Buckets[index].UpperBound; got != bound {
			t.Fatalf("acquired bound %d = %s, want %s", index, got, bound)
		}
		if got := snapshot.AcquireLatency.NotAcquired.Buckets[index].UpperBound; got != bound {
			t.Fatalf("not-acquired bound %d = %s, want %s", index, got, bound)
		}
	}
}

func TestLoadMetricsRegistryExportsClientHistogram(t *testing.T) {
	source := &loadMetricsSource{}
	registry, err := newLoadMetricsRegistry(source)
	if err != nil {
		t.Fatalf("newLoadMetricsRegistry: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "redlease_client_acquire_duration_seconds" {
			return
		}
	}
	t.Fatal("client Acquire latency histogram was not exported")
}
