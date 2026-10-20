package prometheus

import (
	"errors"
	"reflect"

	clientprometheus "github.com/prometheus/client_golang/prometheus"

	"github.com/udovenkoav1981/RedLease/clientmetrics"
)

const (
	resultAcquired    = "acquired"
	resultNotAcquired = "not_acquired"
)

// Source is implemented by both client.Client and client1of1.Client.
type Source interface {
	MetricsSnapshot() clientmetrics.Snapshot
}

// Collector exports metrics for one RedLease client. The owner registers it
// with the Prometheus registry used by the embedding application.
type Collector struct {
	source          Source
	acquireDuration *clientprometheus.Desc
}

var _ clientprometheus.Collector = (*Collector)(nil)

// NewCollector constructs a collector for either RedLease client type. It
// does not register the collector or expose an HTTP endpoint.
func NewCollector(source Source) (*Collector, error) {
	if nilSource(source) {
		return nil, errors.New("client metrics source must not be nil")
	}
	return &Collector{
		source: source,
		acquireDuration: clientprometheus.NewDesc(
			"redlease_client_acquire_duration_seconds",
			"Latency of completed RedLease client Acquire calls.",
			[]string{"result"},
			nil,
		),
	}, nil
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(metrics chan<- *clientprometheus.Desc) {
	metrics <- c.acquireDuration
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(metrics chan<- clientprometheus.Metric) {
	snapshot := c.source.MetricsSnapshot()
	c.emitHistogram(metrics, &snapshot.AcquireLatency.Acquired, resultAcquired)
	c.emitHistogram(metrics, &snapshot.AcquireLatency.NotAcquired, resultNotAcquired)
}

func (c *Collector) emitHistogram(
	metrics chan<- clientprometheus.Metric,
	histogram *clientmetrics.LatencyHistogram,
	result string,
) {
	buckets := make(map[float64]uint64, len(histogram.Buckets))
	for _, bucket := range histogram.Buckets {
		buckets[bucket.UpperBound.Seconds()] = bucket.Count
	}
	metric, err := clientprometheus.NewConstHistogram(
		c.acquireDuration,
		histogram.Count,
		float64(histogram.SumMicroseconds)/1_000_000,
		buckets,
		result,
	)
	if err != nil {
		metrics <- clientprometheus.NewInvalidMetric(c.acquireDuration, err)
		return
	}
	metrics <- metric
}

func nilSource(source Source) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
