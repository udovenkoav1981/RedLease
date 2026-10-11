// Command redlease-server runs a plaintext RedLease server for local testing.
// It intentionally provides no TLS, authentication, or production hardening.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	clientprometheus "github.com/prometheus/client_golang/prometheus"
	clientcollectors "github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/udovenkoav1981/RedLease/server"
	redleaseprometheus "github.com/udovenkoav1981/RedLease/server/prometheus"
)

const (
	defaultListenAddress    = "127.0.0.1:17379"
	defaultConfiguredMaxTTL = uint64(5000)
)

type launcherConfig struct {
	listenAddress        string
	metricsListenAddress string
	configuredMaxTTLMS   uint64
	maxKeys              uint64
	shardCount           uint32
	etcdEndpoint         string
	etcdPrefix           string
	etcdAdvertiseAddress string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(os.Args[1:], os.Stderr, logger); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		logger.Error("redlease-server exited", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(args []string, flagOutput io.Writer, logger *slog.Logger) (runErr error) {
	config, err := parseFlags(args, flagOutput)
	if err != nil {
		return err
	}
	serverConfig, err := config.serverConfig(logger)
	if err != nil {
		return err
	}
	etcdConfig, err := config.etcdConfig()
	if err != nil {
		return err
	}

	runContext, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	var metrics *metricsEndpoint
	if config.metricsListenAddress != "" {
		metrics, err = startMetricsEndpoint(config.metricsListenAddress, logger)
		if err != nil {
			return err
		}
	}

	lifecycle := &launcherLifecycle{
		ctx:           runContext,
		logger:        logger,
		listenAddress: config.listenAddress,
		serverConfig:  serverConfig,
		metrics:       metrics,
		etcdConfig:    etcdConfig,
	}
	if etcdConfig.enabled() {
		lifecycle.acquisition = startEtcdAcquisition(runContext, etcdConfig, logger)
	} else if err := lifecycle.startServer(); err != nil {
		return errors.Join(err, lifecycle.Close())
	}
	defer func() {
		runErr = errors.Join(runErr, lifecycle.Close())
	}()
	lifecycleErr := lifecycle.run()
	if errors.Is(lifecycleErr, context.Canceled) && runContext.Err() != nil {
		return nil
	}
	return lifecycleErr
}

type launcherLifecycle struct {
	ctx           context.Context //nolint:containedctx // Launcher owns the process lifecycle.
	logger        *slog.Logger
	listenAddress string
	serverConfig  server.Config
	server        *server.Server
	metrics       *metricsEndpoint
	etcdConfig    launcherEtcdConfig
	registration  *etcdRegistration
	acquisition   *etcdAcquisition
}

func (l *launcherLifecycle) run() error {
	for {
		var metricsServeErr <-chan error
		if l.metrics != nil {
			metricsServeErr = l.metrics.serveErr
		}
		var registrationErr <-chan error
		if l.registration != nil {
			registrationErr = l.registration.keepAliveErr
		}
		var acquisitionDone <-chan etcdRegistrationResult
		if l.acquisition != nil {
			acquisitionDone = l.acquisition.done
		}
		var fatalErr <-chan error
		if l.server != nil {
			fatalErr = l.server.Fatal()
		}

		select {
		case err := <-metricsServeErr:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("serve Prometheus metrics: %w", err)
			}
			return nil

		case <-l.ctx.Done():
			l.logger.Info("shutdown requested")
			return nil

		case result := <-acquisitionDone:
			if err := l.finishAcquisition(result); err != nil {
				return err
			}

		case keepAliveErr := <-registrationErr:
			if err := l.recoverRegistration(keepAliveErr); err != nil {
				return err
			}

		case serverErr := <-fatalErr:
			l.logger.Error("server failure received; shutting down", slog.Any("error", serverErr))
			return serverErr
		}
	}
}

func (l *launcherLifecycle) finishAcquisition(result etcdRegistrationResult) error {
	if result.registration != nil {
		result.registration.parentCancel = l.acquisition.cancel
	} else {
		l.acquisition.cancel()
	}
	l.acquisition = nil
	if result.err != nil {
		return fmt.Errorf("acquire etcd server slot: %w", result.err)
	}
	l.registration = result.registration
	if err := l.startServer(); err != nil {
		return err
	}
	return nil
}

func (l *launcherLifecycle) recoverRegistration(keepAliveErr error) error {
	l.logger.Warn(
		"etcd registration lost; stopping RedLease TCP server",
		slog.Any("error", keepAliveErr),
	)
	if err := l.stopServer(); err != nil {
		return errors.Join(keepAliveErr, fmt.Errorf("stop RedLease server: %w", err))
	}
	if closeErr := l.registration.Close(); closeErr != nil {
		l.logger.Warn("cannot close lost etcd registration", slog.Any("error", closeErr))
	}
	l.registration = nil
	l.acquisition = startEtcdAcquisition(l.ctx, l.etcdConfig, l.logger)
	return nil
}

func (l *launcherLifecycle) startServer() error {
	listener, err := (&net.ListenConfig{}).Listen(l.ctx, "tcp", l.listenAddress)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", l.listenAddress, err)
	}
	leaseServer, err := server.New(listener, l.serverConfig)
	if err != nil {
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, fmt.Errorf("close TCP listener: %w", closeErr))
		}
		return fmt.Errorf("create RedLease server: %w", err)
	}
	if l.metrics != nil {
		if err := l.metrics.setServer(leaseServer); err != nil {
			return errors.Join(err, leaseServer.Close())
		}
	}
	l.server = leaseServer
	l.logger.Info(
		"listening with plaintext TCP (local testing only)",
		slog.String("address", listener.Addr().String()),
	)
	return nil
}

func (l *launcherLifecycle) stopServer() error {
	if l.server == nil {
		return nil
	}
	var metricsErr error
	if l.metrics != nil {
		metricsErr = l.metrics.setServer(nil)
	}
	closeErr := l.server.Close()
	l.server = nil
	return errors.Join(metricsErr, closeErr)
}

func (l *launcherLifecycle) Close() error {
	var closeErr error
	closeErr = errors.Join(closeErr, l.stopServer())
	if l.acquisition != nil {
		closeErr = errors.Join(closeErr, l.acquisition.Close())
	}
	if l.registration != nil {
		closeErr = errors.Join(closeErr, l.registration.Close())
	}
	if l.metrics != nil {
		closeErr = errors.Join(closeErr, l.metrics.Close())
	}
	return closeErr
}

func parseFlags(args []string, output io.Writer) (launcherConfig, error) {
	config := launcherConfig{}
	flags := flag.NewFlagSet("redlease-server", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(
		&config.listenAddress,
		"listen",
		defaultListenAddress,
		"TCP listen address",
	)
	flags.StringVar(
		&config.metricsListenAddress,
		"metrics-listen",
		"",
		"Prometheus HTTP listen address; empty disables the exporter",
	)
	flags.Uint64Var(
		&config.configuredMaxTTLMS,
		"configured-max-ttl-ms",
		defaultConfiguredMaxTTL,
		"maximum lease TTL in whole milliseconds (1..5000)",
	)
	flags.Uint64Var(
		&config.maxKeys,
		"max-keys",
		server.DefaultMaxKeys,
		"maximum resident lease keys (0 uses the library default)",
	)
	uint32Flag(
		flags,
		&config.shardCount,
		"shard-count",
		"server shard count; must be a power of two (0 uses the library default)",
	)
	flags.StringVar(
		&config.etcdEndpoint,
		"etcd-endpoint",
		"",
		"etcd endpoint; empty disables etcd slot registration",
	)
	flags.StringVar(
		&config.etcdPrefix,
		"etcd-prefix",
		"",
		"prefix for the five etcd server-slot keys",
	)
	flags.StringVar(
		&config.etcdAdvertiseAddress,
		"etcd-advertise",
		"",
		"RedLease IP:port stored in etcd; defaults to -listen",
	)
	flags.Usage = func() {
		_, _ = fmt.Fprintf(output, "Usage: %s [flags]\n\n", flags.Name())
		_, _ = fmt.Fprintln(output, "Local test launcher using plaintext TCP; no TLS or authentication.")
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return launcherConfig{}, err
	}
	if flags.NArg() != 0 {
		return launcherConfig{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	return config, nil
}

type metricsEndpoint struct {
	server    *http.Server
	listener  net.Listener
	serveErr  chan error
	registry  *clientprometheus.Registry
	collector *redleaseprometheus.Collector
}

func startMetricsEndpoint(
	address string,
	logger *slog.Logger,
) (*metricsEndpoint, error) {
	registry, err := newMetricsRegistry()
	if err != nil {
		return nil, err
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for Prometheus metrics on %q: %w", address, err)
	}
	endpoint := &metricsEndpoint{
		server: &http.Server{
			Handler:           metricsHandler(registry),
			ReadHeaderTimeout: 5 * time.Second,
		},
		listener: listener,
		serveErr: make(chan error, 1),
		registry: registry,
	}
	go func() {
		endpoint.serveErr <- endpoint.server.Serve(listener)
	}()

	logger.Info(
		"Prometheus metrics listening",
		slog.String("address", listener.Addr().String()),
		slog.String("path", "/metrics"),
	)
	return endpoint, nil
}

func newMetricsHandler(leaseServer *server.Server) (http.Handler, error) {
	registry, err := newMetricsRegistry()
	if err != nil {
		return nil, err
	}
	redLeaseCollector, err := redleaseprometheus.NewCollector(leaseServer)
	if err != nil {
		return nil, fmt.Errorf("create Prometheus collector: %w", err)
	}
	if err := registry.Register(redLeaseCollector); err != nil {
		return nil, fmt.Errorf("register Prometheus collector: %w", err)
	}
	return metricsHandler(registry), nil
}

func newMetricsRegistry() (*clientprometheus.Registry, error) {
	registry := clientprometheus.NewRegistry()
	for _, collector := range []clientprometheus.Collector{
		clientcollectors.NewGoCollector(),
		clientcollectors.NewProcessCollector(clientcollectors.ProcessCollectorOpts{}),
	} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register standard Prometheus collector: %w", err)
		}
	}
	return registry, nil
}

func metricsHandler(registry *clientprometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	return mux
}

func (e *metricsEndpoint) setServer(leaseServer *server.Server) error {
	if e.collector != nil {
		if !e.registry.Unregister(e.collector) {
			return errors.New("unregister RedLease Prometheus collector")
		}
		e.collector = nil
	}
	if leaseServer == nil {
		return nil
	}
	collector, err := redleaseprometheus.NewCollector(leaseServer)
	if err != nil {
		return fmt.Errorf("create Prometheus collector: %w", err)
	}
	if err := e.registry.Register(collector); err != nil {
		return fmt.Errorf("register Prometheus collector: %w", err)
	}
	e.collector = collector
	return nil
}

func (e *metricsEndpoint) Close() error {
	serverErr := e.server.Close()
	listenerErr := e.listener.Close()
	if errors.Is(listenerErr, net.ErrClosed) {
		listenerErr = nil
	}
	return errors.Join(serverErr, listenerErr)
}

func uint32Flag(flags *flag.FlagSet, target *uint32, name, usage string) {
	flags.Func(name, usage, func(raw string) error {
		value, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return err
		}
		*target = uint32(value)
		return nil
	})
}

func (c *launcherConfig) serverConfig(logger *slog.Logger) (server.Config, error) {
	maxKeys := c.maxKeys
	if maxKeys == 0 {
		maxKeys = server.DefaultMaxKeys
	}
	result := server.Config{
		MaxTTL:     c.configuredMaxTTLMS,
		MaxKeys:    maxKeys,
		Logger:     logger,
		ShardCount: c.shardCount,
	}
	if err := result.Validate(); err != nil {
		return server.Config{}, err
	}
	return result, nil
}
