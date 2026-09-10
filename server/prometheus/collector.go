// Package prometheus exposes RedLease server metrics to Prometheus without
// starting an HTTP server or modifying the default registry.
package prometheus

import (
	"errors"

	clientprometheus "github.com/prometheus/client_golang/prometheus"

	redleaseserver "github.com/udovenkoav1981/RedLease/server"
)

var serverStates = [...]string{"quarantine", "active", "failed", "closed"}

type metricsSource interface {
	MetricsSnapshot() redleaseserver.MetricsSnapshot
}

// Collector exports metrics for one RedLease server. The owner registers it
// with the Prometheus registry used by the embedding application.
type Collector struct {
	source metricsSource

	state                    *clientprometheus.Desc
	residentKeys             *clientprometheus.Desc
	queuedOperations         *clientprometheus.Desc
	activeStreams            *clientprometheus.Desc
	restartQuarantineSkipped *clientprometheus.Desc
	acquiresTotal            *clientprometheus.Desc
	renewsTotal              *clientprometheus.Desc
	releasesTotal            *clientprometheus.Desc
}

var _ clientprometheus.Collector = (*Collector)(nil)

// NewCollector constructs a collector for server. It does not register the
// collector or expose an HTTP endpoint.
func NewCollector(server *redleaseserver.Server) (*Collector, error) {
	if server == nil {
		return nil, errors.New("server must not be nil")
	}
	return newCollector(server), nil
}

func newCollector(source metricsSource) *Collector {
	return &Collector{
		source: source,
		state: clientprometheus.NewDesc(
			"redlease_server_state",
			"Whether the RedLease server is in the indicated state (1) or not (0).",
			[]string{"state"},
			nil,
		),
		residentKeys: clientprometheus.NewDesc(
			"redlease_server_resident_keys",
			"Number of lease records currently resident in memory, including expired records awaiting cleanup.",
			nil,
			nil,
		),
		queuedOperations: clientprometheus.NewDesc(
			"redlease_server_queued_operations",
			"Number of lease operations currently waiting in shard queues.",
			nil,
			nil,
		),
		activeStreams: clientprometheus.NewDesc(
			"redlease_server_active_streams",
			"Number of active RedLease gRPC streams.",
			nil,
			nil,
		),
		restartQuarantineSkipped: clientprometheus.NewDesc(
			"redlease_server_restart_quarantine_skipped",
			"Whether restart quarantine was explicitly delegated to the embedding application.",
			nil,
			nil,
		),
		acquiresTotal: clientprometheus.NewDesc(
			"redlease_server_acquires_total",
			"Total number of Acquire operations processed while the server was active.",
			nil,
			nil,
		),
		renewsTotal: clientprometheus.NewDesc(
			"redlease_server_renews_total",
			"Total number of Renew operations processed while the server was active.",
			nil,
			nil,
		),
		releasesTotal: clientprometheus.NewDesc(
			"redlease_server_releases_total",
			"Total number of Release operations processed while the server was active.",
			nil,
			nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(metrics chan<- *clientprometheus.Desc) {
	metrics <- c.state
	metrics <- c.residentKeys
	metrics <- c.queuedOperations
	metrics <- c.activeStreams
	metrics <- c.restartQuarantineSkipped
	metrics <- c.acquiresTotal
	metrics <- c.renewsTotal
	metrics <- c.releasesTotal
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(metrics chan<- clientprometheus.Metric) {
	snapshot := c.source.MetricsSnapshot()
	for _, state := range serverStates {
		value := 0.0
		if snapshot.State == state {
			value = 1
		}
		emit(metrics, c.state, clientprometheus.GaugeValue, value, state)
	}

	emit(metrics, c.residentKeys, clientprometheus.GaugeValue, float64(snapshot.ResidentKeys))
	emit(metrics, c.queuedOperations, clientprometheus.GaugeValue, float64(snapshot.QueuedOperations))
	emit(metrics, c.activeStreams, clientprometheus.GaugeValue, float64(snapshot.ActiveStreams))
	quarantineSkipped := 0.0
	if snapshot.RestartQuarantineSkipped {
		quarantineSkipped = 1
	}
	emit(metrics, c.restartQuarantineSkipped, clientprometheus.GaugeValue, quarantineSkipped)
	emit(metrics, c.acquiresTotal, clientprometheus.CounterValue, float64(snapshot.AcquiresTotal))
	emit(metrics, c.renewsTotal, clientprometheus.CounterValue, float64(snapshot.RenewsTotal))
	emit(metrics, c.releasesTotal, clientprometheus.CounterValue, float64(snapshot.ReleasesTotal))
}

func emit(
	metrics chan<- clientprometheus.Metric,
	description *clientprometheus.Desc,
	valueType clientprometheus.ValueType,
	value float64,
	labels ...string,
) {
	metric, err := clientprometheus.NewConstMetric(description, valueType, value, labels...)
	if err != nil {
		metrics <- clientprometheus.NewInvalidMetric(description, err)
		return
	}
	metrics <- metric
}
