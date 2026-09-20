// Command redlease-tcp-load measures one raw RedLease TCP connection without
// using either public client library.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

const (
	defaultTarget       = "127.0.0.1:50051"
	defaultKeyCount     = uint64(1024)
	defaultTTLMS        = uint64(1000)
	defaultDuration     = 10 * time.Second
	defaultWarmup       = time.Second
	defaultReadyTimeout = 10 * time.Second
	maxTTLMS            = uint64(5000)
)

type options struct {
	target       string
	keyCount     uint64
	ttlMS        uint64
	duration     time.Duration
	warmup       time.Duration
	readyTimeout time.Duration
}

type responseCounters struct {
	acquires atomic.Uint64
	releases atomic.Uint64
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, "redlease-tcp-load:", err)
		os.Exit(1)
	}
}

func run(args []string, output, flagOutput io.Writer) error {
	config, err := parseOptions(args, flagOutput)
	if err != nil {
		return err
	}
	bootID, err := newBootID()
	if err != nil {
		return err
	}
	if err := waitForActive(context.Background(), &config, bootID); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	connection, err := transport.Dial(ctx, config.target)
	if err != nil {
		cancel()
		return fmt.Errorf("connect to measurement server: %w", err)
	}

	var counters responseCounters
	workerErrors := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Go(func() { workerErrors <- sendRequests(ctx, connection, &config, bootID) })
	workers.Go(func() { workerErrors <- receiveResponses(connection, &counters) })

	if err := waitInterval(ctx, workerErrors, config.warmup); err != nil {
		cancel()
		_ = connection.Close()
		workers.Wait()
		return fmt.Errorf("warmup: %w", err)
	}
	initialAcquires := counters.acquires.Load()
	initialReleases := counters.releases.Load()
	started := time.Now()
	if err := waitInterval(ctx, workerErrors, config.duration); err != nil {
		cancel()
		_ = connection.Close()
		workers.Wait()
		return fmt.Errorf("measurement: %w", err)
	}
	elapsed := time.Since(started)
	acquires := counters.acquires.Load() - initialAcquires
	releases := counters.releases.Load() - initialReleases

	cancel()
	_ = connection.Close()
	workers.Wait()

	_, err = fmt.Fprintf(
		output,
		"target=%s elapsed=%s keys=%d ttl_ms=%d\n"+
			"acquire_ok=%d release_ok=%d\n"+
			"acquire_release_pairs_per_second=%.0f\n"+
			"response_messages_per_second=%.0f\n",
		config.target,
		elapsed.Round(time.Millisecond),
		config.keyCount,
		config.ttlMS,
		acquires,
		releases,
		float64(releases)/elapsed.Seconds(),
		float64(acquires+releases)/elapsed.Seconds(),
	)
	if err != nil {
		return fmt.Errorf("write result: %w", err)
	}
	return nil
}

func parseOptions(args []string, output io.Writer) (options, error) {
	config := options{}
	flags := flag.NewFlagSet("redlease-tcp-load", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&config.target, "target", defaultTarget, "address of an already running TCP lock-server")
	flags.Uint64Var(&config.keyCount, "keys", defaultKeyCount, "number of resource keys used in round-robin order")
	flags.Uint64Var(&config.ttlMS, "ttl-ms", defaultTTLMS, "requested lease TTL in milliseconds")
	flags.DurationVar(&config.duration, "duration", defaultDuration, "measurement duration")
	flags.DurationVar(&config.warmup, "warmup", defaultWarmup, "warmup before measurement")
	flags.DurationVar(&config.readyTimeout, "ready-timeout", defaultReadyTimeout, "maximum wait for ACTIVE server state")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	switch {
	case config.target == "":
		return options{}, errors.New("target must not be empty")
	case config.keyCount == 0:
		return options{}, errors.New("keys must be positive")
	case config.ttlMS == 0 || config.ttlMS > maxTTLMS:
		return options{}, errors.New("ttl-ms must be between 1 and 5000")
	case config.duration <= 0:
		return options{}, errors.New("duration must be positive")
	case config.warmup < 0:
		return options{}, errors.New("warmup must not be negative")
	case config.readyTimeout <= 0:
		return options{}, errors.New("ready-timeout must be positive")
	default:
		return config, nil
	}
}

func newBootID() (uint32, error) {
	var encoded [4]byte
	if _, err := rand.Read(encoded[:]); err != nil {
		return 0, fmt.Errorf("generate boot ID: %w", err)
	}
	bootID := binary.LittleEndian.Uint32(encoded[:])
	if bootID == 0 {
		bootID = 1
	}
	return bootID, nil
}

func waitForActive(parent context.Context, config *options, bootID uint32) error {
	ctx, cancel := context.WithTimeout(parent, config.readyTimeout)
	defer cancel()
	connection, err := transport.Dial(ctx, config.target)
	if err != nil {
		return fmt.Errorf("connect for readiness check: %w", err)
	}
	defer func() { _ = connection.Close() }()
	go func() {
		<-ctx.Done()
		_ = connection.Close()
	}()

	for sequence := uint64(1); ; sequence++ {
		requestID := sequence*2 - 1
		if err := connection.Send(protocol.Request{
			RequestID: requestID, Operation: protocol.OperationAcquire, Key: 0,
			ClientID: 1, BootID: bootID, LeaseSequence: sequence, RequestedTTLMS: config.ttlMS,
		}); err != nil {
			return fmt.Errorf("send readiness Acquire: %w", err)
		}
		response, err := connection.Recv()
		if err != nil {
			return fmt.Errorf("receive readiness Acquire: %w", err)
		}
		if response.Operation != protocol.OperationAcquire {
			return fmt.Errorf("readiness request %d: unexpected response operation %d", response.RequestID, response.Operation)
		}
		switch response.Status {
		case protocol.StatusOK:
			if err := connection.Send(protocol.Request{
				RequestID: requestID + 1, Operation: protocol.OperationRelease, Key: 0,
				ClientID: 1, BootID: bootID, LeaseSequence: sequence,
			}); err != nil {
				return fmt.Errorf("send readiness Release: %w", err)
			}
			release, err := connection.Recv()
			if err != nil {
				return fmt.Errorf("receive readiness Release: %w", err)
			}
			if release.Operation != protocol.OperationRelease || release.Status != protocol.StatusOK {
				return fmt.Errorf("readiness Release response: operation=%d status=%d", release.Operation, release.Status)
			}
			return nil
		case protocol.StatusNotReady, protocol.StatusBusy:
			timer := time.NewTimer(25 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("wait for ACTIVE server: %w", ctx.Err())
			}
		default:
			return fmt.Errorf("readiness Acquire status: %d", response.Status)
		}
	}
}

func sendRequests(ctx context.Context, connection *transport.Connection, config *options, bootID uint32) error {
	for sequence := uint64(1); ; sequence++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		key := (sequence-1)%config.keyCount + 1
		requestID := sequence*2 - 1
		if err := connection.Send(protocol.Request{
			RequestID: requestID, Operation: protocol.OperationAcquire, Key: key,
			ClientID: 1, BootID: bootID, LeaseSequence: sequence, RequestedTTLMS: config.ttlMS,
		}); err != nil {
			return fmt.Errorf("send Acquire: %w", err)
		}
		if err := connection.Send(protocol.Request{
			RequestID: requestID + 1, Operation: protocol.OperationRelease, Key: key,
			ClientID: 1, BootID: bootID, LeaseSequence: sequence,
		}); err != nil {
			return fmt.Errorf("send Release: %w", err)
		}
	}
}

func receiveResponses(connection *transport.Connection, counters *responseCounters) error {
	for {
		response, err := connection.Recv()
		if err != nil {
			return fmt.Errorf("receive response: %w", err)
		}
		if response.Status != protocol.StatusOK {
			return fmt.Errorf("request %d: status %d", response.RequestID, response.Status)
		}
		switch response.Operation {
		case protocol.OperationAcquire:
			counters.acquires.Add(1)
		case protocol.OperationRelease:
			counters.releases.Add(1)
		default:
			return fmt.Errorf("request %d: unexpected response operation %d", response.RequestID, response.Operation)
		}
	}
}

func waitInterval(ctx context.Context, workerErrors <-chan error, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case err := <-workerErrors:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
