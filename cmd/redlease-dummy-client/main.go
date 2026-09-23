// Command redlease-dummy-client measures raw RedLease TCP throughput without
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
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

const (
	defaultTarget       = "127.0.0.1:50051"
	defaultKeyCount     = uint64(1024)
	defaultTTLMS        = uint64(1000)
	defaultDuration     = 10 * time.Second
	defaultWarmup       = time.Second
	defaultReadyTimeout = 10 * time.Second
	defaultConnections  = 1
	maxConnections      = 1024
	maxTTLMS            = uint64(5000)
	// Each queued pair contains two wire requests. This bounds the queue to
	// the same 4096-request capacity as the regular client writer.
	sendPairQueueCapacity = 2048
)

type options struct {
	target       string
	keyCount     uint64
	ttlMS        uint64
	duration     time.Duration
	warmup       time.Duration
	readyTimeout time.Duration
	connections  int
}

type responseCounters struct {
	acquires atomic.Uint64
	releases atomic.Uint64
}

type requestPair struct {
	key      uint64
	sequence uint64
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, "redlease-dummy-client:", err)
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
	connections := make([]*transport.ClientConnection, 0, config.connections)
	for range config.connections {
		connection, dialErr := transport.Dial(ctx, config.target)
		if dialErr != nil {
			cancel()
			closeConnections(connections)
			return fmt.Errorf("connect to measurement server: %w", dialErr)
		}
		connections = append(connections, connection)
	}

	var counters responseCounters
	workerErrors := make(chan error, 3*config.connections)
	var workers sync.WaitGroup
	for index, connection := range connections {
		sendQueue := make(chan requestPair, sendPairQueueCapacity)
		keyOffset := uint64(index) * config.keyCount
		workers.Go(func() { workerErrors <- produceRequests(ctx, sendQueue, &config, keyOffset) })
		workers.Go(func() { workerErrors <- sendRequests(ctx, connection, sendQueue, bootID, config.ttlMS) })
		workers.Go(func() { workerErrors <- receiveResponses(connection, &counters) })
	}

	if err := waitInterval(ctx, workerErrors, config.warmup); err != nil {
		cancel()
		closeConnections(connections)
		workers.Wait()
		return fmt.Errorf("warmup: %w", err)
	}
	initialAcquires := counters.acquires.Load()
	initialReleases := counters.releases.Load()
	started := time.Now()
	if err := waitInterval(ctx, workerErrors, config.duration); err != nil {
		cancel()
		closeConnections(connections)
		workers.Wait()
		return fmt.Errorf("measurement: %w", err)
	}
	elapsed := time.Since(started)
	acquires := counters.acquires.Load() - initialAcquires
	releases := counters.releases.Load() - initialReleases

	cancel()
	closeConnections(connections)
	workers.Wait()

	_, err = fmt.Fprintf(
		output,
		"target=%s elapsed=%s connections=%d keys_per_connection=%d ttl_ms=%d\n"+
			"acquire_ok=%d release_ok=%d\n"+
			"acquire_release_pairs_per_second=%.0f\n"+
			"response_messages_per_second=%.0f\n",
		config.target,
		elapsed.Round(time.Millisecond),
		config.connections,
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

func closeConnections(connections []*transport.ClientConnection) {
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func parseOptions(args []string, output io.Writer) (options, error) {
	config := options{}
	flags := flag.NewFlagSet("redlease-dummy-client", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&config.target, "target", defaultTarget, "address of an already running TCP lock-server")
	flags.Uint64Var(&config.keyCount, "keys", defaultKeyCount, "number of resource keys used in round-robin order")
	flags.Uint64Var(&config.ttlMS, "ttl-ms", defaultTTLMS, "requested lease TTL in milliseconds")
	flags.DurationVar(&config.duration, "duration", defaultDuration, "measurement duration")
	flags.DurationVar(&config.warmup, "warmup", defaultWarmup, "warmup before measurement")
	flags.DurationVar(&config.readyTimeout, "ready-timeout", defaultReadyTimeout, "maximum wait for ACTIVE server state")
	flags.IntVar(&config.connections, "connections", defaultConnections, "number of independent TCP connections")
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
	case config.connections < 1 || config.connections > maxConnections:
		return options{}, fmt.Errorf("connections must be between 1 and %d", maxConnections)
	case config.keyCount > math.MaxUint64/uint64(config.connections):
		return options{}, errors.New("keys times connections exceeds uint64 key space")
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
	builder := flatbuffers.NewBuilder(transport.InitialBufferSize)

	for sequence := uint64(1); ; sequence++ {
		requestID := sequence*2 - 1
		if err := bufferAcquire(connection, builder, requestID, 0, bootID, sequence, config.ttlMS); err != nil {
			return fmt.Errorf("send readiness Acquire: %w", err)
		}
		if err := connection.FlushClientRequests(); err != nil {
			return fmt.Errorf("flush readiness Acquire: %w", err)
		}
		response, err := connection.Recv()
		if err != nil {
			return fmt.Errorf("receive readiness Acquire: %w", err)
		}
		if response.Operation != redleasev1.ClientOperationACQUIRE {
			return fmt.Errorf("readiness request %d: unexpected response operation %d", response.RequestID, response.Operation)
		}
		switch response.Status {
		case redleasev1.LeaseStatusOK:
			if err := bufferRelease(connection, builder, requestID+1, 0, bootID, sequence); err != nil {
				return fmt.Errorf("send readiness Release: %w", err)
			}
			if err := connection.FlushClientRequests(); err != nil {
				return fmt.Errorf("flush readiness Release: %w", err)
			}
			release, err := connection.Recv()
			if err != nil {
				return fmt.Errorf("receive readiness Release: %w", err)
			}
			if release.Operation != redleasev1.ClientOperationRELEASE || release.Status != redleasev1.LeaseStatusOK {
				return fmt.Errorf("readiness Release response: operation=%d status=%d", release.Operation, release.Status)
			}
			return nil
		case redleasev1.LeaseStatusNOT_READY, redleasev1.LeaseStatusBUSY:
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

func produceRequests(ctx context.Context, sendQueue chan<- requestPair, config *options, keyOffset uint64) error {
	for sequence := uint64(1); ; sequence++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		pair := requestPair{key: keyOffset + (sequence-1)%config.keyCount + 1, sequence: sequence}
		select {
		case sendQueue <- pair:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func sendRequests(
	ctx context.Context,
	connection transport.LeaseConnection,
	sendQueue <-chan requestPair,
	bootID uint32,
	ttlMS uint64,
) error {
	builder := flatbuffers.NewBuilder(transport.InitialBufferSize)
	for {
		var pair requestPair
		select {
		case <-ctx.Done():
			return ctx.Err()
		case pair = <-sendQueue:
		}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			requestID := pair.sequence*2 - 1
			if err := bufferAcquire(connection, builder, requestID, pair.key, bootID, pair.sequence, ttlMS); err != nil {
				return fmt.Errorf("buffer Acquire: %w", err)
			}
			if err := bufferRelease(connection, builder, requestID+1, pair.key, bootID, pair.sequence); err != nil {
				return fmt.Errorf("buffer Release: %w", err)
			}
			select {
			case pair = <-sendQueue:
				continue
			default:
			}
			if err := connection.FlushClientRequests(); err != nil {
				return fmt.Errorf("flush send batch: %w", err)
			}
			break
		}
	}
}

func bufferAcquire(
	connection transport.LeaseConnection,
	builder *flatbuffers.Builder,
	requestID uint64,
	key uint64,
	bootID uint32,
	sequence uint64,
	ttlMS uint64,
) error {
	builder.Reset()
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddRequestId(builder, requestID)
	redleasev1.ClientRequestAddOperation(builder, redleasev1.ClientOperationACQUIRE)
	redleasev1.ClientRequestAddAcquire(builder, redleasev1.CreateAcquireRequest(
		builder,
		key,
		1,
		bootID,
		sequence,
		ttlMS,
	))
	return bufferBuiltRequest(connection, builder)
}

func bufferRelease(
	connection transport.LeaseConnection,
	builder *flatbuffers.Builder,
	requestID uint64,
	key uint64,
	bootID uint32,
	sequence uint64,
) error {
	builder.Reset()
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddRequestId(builder, requestID)
	redleasev1.ClientRequestAddOperation(builder, redleasev1.ClientOperationRELEASE)
	redleasev1.ClientRequestAddRelease(builder, redleasev1.CreateReleaseRequest(
		builder,
		key,
		1,
		bootID,
		sequence,
	))
	return bufferBuiltRequest(connection, builder)
}

func bufferBuiltRequest(
	connection transport.LeaseConnection,
	builder *flatbuffers.Builder,
) error {
	root := redleasev1.ClientRequestEnd(builder)
	redleasev1.FinishSizePrefixedClientRequestBuffer(builder, root)
	request := redleasev1.GetSizePrefixedRootAsClientRequest(builder.FinishedBytes(), 0)
	return connection.BufferClientRequest(request)
}

func receiveResponses(connection *transport.ClientConnection, counters *responseCounters) error {
	for {
		response, err := connection.Recv()
		if err != nil {
			return fmt.Errorf("receive response: %w", err)
		}
		if response.Status != redleasev1.LeaseStatusOK {
			return fmt.Errorf("request %d: status %d", response.RequestID, response.Status)
		}
		switch response.Operation {
		case redleasev1.ClientOperationACQUIRE:
			counters.acquires.Add(1)
		case redleasev1.ClientOperationRELEASE:
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
