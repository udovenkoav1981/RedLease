// Command redlease-server runs a plaintext RedLease server for local testing.
// It intentionally provides no TLS, authentication, or production hardening.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/udovenkoav1981/RedLease/server"
	"google.golang.org/grpc"
)

const (
	defaultListenAddress    = "127.0.0.1:50051"
	defaultConfiguredMaxTTL = uint64(5000)
)

type launcherConfig struct {
	listenAddress        string
	configuredMaxTTLMS   uint64
	maxKeys              uint64
	shardCount           int
	shardQueueDepth      int
	maxInFlightPerStream int
}

func main() {
	logger := log.New(os.Stderr, "redlease-server: ", log.LstdFlags|log.Lmicroseconds)
	if err := run(os.Args[1:], os.Stderr, logger); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		logger.Printf("error: %v", err)
		os.Exit(1)
	}
}

func run(args []string, flagOutput io.Writer, logger *log.Logger) error {
	config, err := parseFlags(args, flagOutput)
	if err != nil {
		return err
	}
	serverConfig, err := config.serverConfig()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", config.listenAddress)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", config.listenAddress, err)
	}
	defer listener.Close()

	leaseServer, err := server.New(serverConfig)
	if err != nil {
		return fmt.Errorf("create RedLease server: %w", err)
	}
	defer leaseServer.Close()

	grpcServer := grpc.NewServer()
	leaseServer.Register(grpcServer)

	logger.Printf(
		"state=QUARANTINE configured_max_ttl_ms=%d max_keys=%d; activation is managed by the server library",
		config.configuredMaxTTLMS,
		serverConfig.MaxKeys,
	)
	logger.Printf("listening on %s with plaintext gRPC (local testing only)", listener.Addr())

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve gRPC: %w", err)
		}
		return nil

	case received := <-signals:
		logger.Printf("received %s; shutting down", received)
		if err := leaseServer.Close(); err != nil {
			grpcServer.Stop()
			return fmt.Errorf("close RedLease server: %w", err)
		}
		grpcServer.GracefulStop()
		if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve gRPC during shutdown: %w", err)
		}
		logger.Print("stopped")
		return nil
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
	flags.IntVar(
		&config.shardCount,
		"shard-count",
		0,
		"server shard count (0 uses the library default)",
	)
	flags.IntVar(
		&config.shardQueueDepth,
		"shard-queue-depth",
		0,
		"jobs buffered per shard (0 uses the library default)",
	)
	flags.IntVar(
		&config.maxInFlightPerStream,
		"max-in-flight-per-stream",
		0,
		"maximum requests in flight per stream (0 uses the library default)",
	)
	flags.Usage = func() {
		fmt.Fprintf(output, "Usage: %s [flags]\n\n", flags.Name())
		fmt.Fprintln(output, "Local test launcher using plaintext gRPC; no TLS or authentication.")
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

func (c launcherConfig) serverConfig() (server.Config, error) {
	protocolMaxTTLMS := uint64(server.ProtocolMaxTTL / time.Millisecond)
	if c.configuredMaxTTLMS == 0 {
		return server.Config{}, errors.New("configured max TTL must be positive")
	}
	// Compare in the wire type before converting to time.Duration. This keeps
	// even math.MaxUint64 from overflowing the signed duration representation.
	if c.configuredMaxTTLMS > protocolMaxTTLMS {
		return server.Config{}, fmt.Errorf(
			"configured max TTL %dms exceeds protocol maximum %dms",
			c.configuredMaxTTLMS,
			protocolMaxTTLMS,
		)
	}

	maxKeys := c.maxKeys
	if maxKeys == 0 {
		maxKeys = server.DefaultMaxKeys
	}
	result := server.Config{
		ConfiguredMaxTTL:     time.Duration(c.configuredMaxTTLMS) * time.Millisecond,
		MaxKeys:              maxKeys,
		ShardCount:           c.shardCount,
		ShardQueueDepth:      c.shardQueueDepth,
		MaxInFlightPerStream: c.maxInFlightPerStream,
	}
	if err := result.Validate(); err != nil {
		return server.Config{}, err
	}
	return result, nil
}
