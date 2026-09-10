// Command redlease-server runs a plaintext RedLease server for local testing.
// It intentionally provides no TLS, authentication, or production hardening.
package main

import (
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/udovenkoav1981/RedLease/server"
	redleaseprometheus "github.com/udovenkoav1981/RedLease/server/prometheus"
)

const (
	defaultListenAddress    = "127.0.0.1:50051"
	defaultConfiguredMaxTTL = uint64(5000)
)

type launcherConfig struct {
	listenAddress        string
	metricsListenAddress string
	configuredMaxTTLMS   uint64
	maxKeys              uint64
	shardCount           uint32
	shardQueueDepth      uint32
	maxInFlightPerStream uint32
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

	listener, err := net.Listen("tcp", config.listenAddress)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", config.listenAddress, err)
	}
	defer func() {
		closeErr := listener.Close()
		if closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			runErr = errors.Join(runErr, fmt.Errorf("close gRPC listener: %w", closeErr))
		}
	}()

	leaseServer, err := server.New(serverConfig)
	if err != nil {
		return fmt.Errorf("create RedLease server: %w", err)
	}
	defer func() {
		if closeErr := leaseServer.Close(); closeErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close RedLease server: %w", closeErr))
		}
	}()

	grpcServer := grpc.NewServer()
	leaseServer.Register(grpcServer)
	defer grpcServer.Stop()

	var metrics *metricsEndpoint
	if config.metricsListenAddress != "" {
		metrics, err = startMetricsEndpoint(config.metricsListenAddress, leaseServer, logger)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := metrics.Close(); closeErr != nil {
				runErr = errors.Join(runErr, fmt.Errorf("close Prometheus metrics endpoint: %w", closeErr))
			}
		}()
	}

	logger.Info(
		"listening with plaintext gRPC (local testing only)",
		slog.String("address", listener.Addr().String()),
	)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()
	var metricsServeErr <-chan error
	if metrics != nil {
		metricsServeErr = metrics.serveErr
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve gRPC: %w", err)
		}
		return nil

	case err := <-metricsServeErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve Prometheus metrics: %w", err)
		}
		return nil

	case received := <-signals:
		logger.Info("shutdown requested", slog.String("signal", received.String()))
		if err := leaseServer.Close(); err != nil {
			grpcServer.Stop()
			return fmt.Errorf("close RedLease server: %w", err)
		}
		grpcServer.GracefulStop()
		if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve gRPC during shutdown: %w", err)
		}
		logger.Info("standalone server stopped")
		return nil

	case fatalErr := <-leaseServer.Fatal():
		logger.Error("server failure received; shutting down", slog.Any("error", fatalErr))
		grpcServer.Stop()
		closeErr := leaseServer.Close()
		serveResult := <-serveErr
		if serveResult != nil && !errors.Is(serveResult, grpc.ErrServerStopped) {
			serveResult = fmt.Errorf("serve gRPC during failed shutdown: %w", serveResult)
		} else {
			serveResult = nil
		}
		return errors.Join(fatalErr, closeErr, serveResult)
	}
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
		"server shard count (0 uses the library default)",
	)
	uint32Flag(
		flags,
		&config.shardQueueDepth,
		"shard-queue-depth",
		"jobs buffered per shard (0 uses the library default)",
	)
	uint32Flag(
		flags,
		&config.maxInFlightPerStream,
		"max-in-flight-per-stream",
		"maximum requests in flight per stream (0 uses the library default)",
	)
	flags.Usage = func() {
		_, _ = fmt.Fprintf(output, "Usage: %s [flags]\n\n", flags.Name())
		_, _ = fmt.Fprintln(output, "Local test launcher using plaintext gRPC; no TLS or authentication.")
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
	server   *http.Server
	listener net.Listener
	serveErr chan error
}

func startMetricsEndpoint(
	address string,
	leaseServer *server.Server,
	logger *slog.Logger,
) (*metricsEndpoint, error) {
	handler, err := newMetricsHandler(leaseServer)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for Prometheus metrics on %q: %w", address, err)
	}
	endpoint := &metricsEndpoint{
		server: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
		listener: listener,
		serveErr: make(chan error, 1),
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
	collector, err := redleaseprometheus.NewCollector(leaseServer)
	if err != nil {
		return nil, fmt.Errorf("create Prometheus collector: %w", err)
	}
	registry := clientprometheus.NewRegistry()
	if err := registry.Register(collector); err != nil {
		return nil, fmt.Errorf("register Prometheus collector: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	return mux, nil
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

func (c launcherConfig) serverConfig(logger *slog.Logger) (server.Config, error) {
	maxKeys := c.maxKeys
	if maxKeys == 0 {
		maxKeys = server.DefaultMaxKeys
	}
	result := server.Config{
		MaxTTL:               c.configuredMaxTTLMS,
		MaxKeys:              maxKeys,
		Logger:               logger,
		ShardCount:           c.shardCount,
		ShardQueueDepth:      c.shardQueueDepth,
		MaxInFlightPerStream: c.maxInFlightPerStream,
	}
	if err := result.Validate(); err != nil {
		return server.Config{}, err
	}
	return result, nil
}
