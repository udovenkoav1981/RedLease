package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	clientprometheus "github.com/prometheus/client_golang/prometheus"
	clientcollectors "github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/udovenkoav1981/RedLease/clientmetrics"
	clientmetricsprometheus "github.com/udovenkoav1981/RedLease/clientmetrics/prometheus"
)

type loadMetricsClientSet struct {
	clients []loadClient
}

// loadMetricsSource aggregates every client participating in the current
// matrix cell into one low-cardinality histogram.
type loadMetricsSource struct {
	current atomic.Pointer[loadMetricsClientSet]
}

func (s *loadMetricsSource) setClients(clients []loadClient) {
	clientSet := &loadMetricsClientSet{clients: append([]loadClient(nil), clients...)}
	s.current.Store(clientSet)
}

func (s *loadMetricsSource) MetricsSnapshot() clientmetrics.Snapshot {
	clientSet := s.current.Load()
	if clientSet == nil {
		return aggregateLoadClientMetrics(nil)
	}
	return aggregateLoadClientMetrics(clientSet.clients)
}

func aggregateLoadClientMetrics(clients []loadClient) clientmetrics.Snapshot {
	bounds := clientmetrics.AcquireLatencyBucketUpperBounds()
	var result clientmetrics.Snapshot
	for index, bound := range bounds {
		result.AcquireLatency.Acquired.Buckets[index].UpperBound = bound
		result.AcquireLatency.NotAcquired.Buckets[index].UpperBound = bound
	}
	for _, instance := range clients {
		snapshot := instance.MetricsSnapshot()
		addLoadLatencyHistogram(
			&result.AcquireLatency.Acquired,
			&snapshot.AcquireLatency.Acquired,
		)
		addLoadLatencyHistogram(
			&result.AcquireLatency.NotAcquired,
			&snapshot.AcquireLatency.NotAcquired,
		)
	}
	return result
}

func addLoadLatencyHistogram(
	target *clientmetrics.LatencyHistogram,
	source *clientmetrics.LatencyHistogram,
) {
	target.Count += source.Count
	target.SumMicroseconds += source.SumMicroseconds
	for index := range target.Buckets {
		target.Buckets[index].Count += source.Buckets[index].Count
	}
}

type loadMetricsEndpoint struct {
	server   *http.Server
	listener net.Listener
}

func startLoadMetricsEndpoint(
	address string,
	source *loadMetricsSource,
	logger *slog.Logger,
) (*loadMetricsEndpoint, error) {
	registry, err := newLoadMetricsRegistry(source)
	if err != nil {
		return nil, err
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for Prometheus metrics on %q: %w", address, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	endpoint := &loadMetricsEndpoint{
		server: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		listener: listener,
	}
	go func() {
		if serveErr := endpoint.server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("Prometheus metrics endpoint failed", slog.Any("error", serveErr))
		}
	}()
	logger.Info(
		"Prometheus metrics listening",
		slog.String("address", listener.Addr().String()),
		slog.String("path", "/metrics"),
	)
	return endpoint, nil
}

func newLoadMetricsRegistry(source *loadMetricsSource) (*clientprometheus.Registry, error) {
	clientCollector, err := clientmetricsprometheus.NewCollector(source)
	if err != nil {
		return nil, fmt.Errorf("create RedLease client Prometheus collector: %w", err)
	}
	registry := clientprometheus.NewRegistry()
	for _, collector := range []clientprometheus.Collector{
		clientcollectors.NewGoCollector(),
		clientcollectors.NewProcessCollector(clientcollectors.ProcessCollectorOpts{}),
		clientCollector,
	} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register Prometheus collector: %w", err)
		}
	}
	return registry, nil
}

func (e *loadMetricsEndpoint) Close() error {
	serverErr := e.server.Close()
	listenerErr := e.listener.Close()
	if errors.Is(listenerErr, net.ErrClosed) {
		listenerErr = nil
	}
	return errors.Join(serverErr, listenerErr)
}
