// Command redlease-grpc-load measures one raw RedLease gRPC stream without
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
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
		_, _ = fmt.Fprintln(os.Stderr, "redlease-grpc-load:", err)
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

	connection, err := grpc.NewClient(
		config.target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("create gRPC connection: %w", err)
	}
	defer func() { _ = connection.Close() }()

	rpc := redleasev1.NewRedLeaseClient(connection)
	if err := waitForActive(context.Background(), rpc, &config, bootID); err != nil {
		return err
	}

	streamContext, cancelStream := context.WithCancel(context.Background())
	stream, err := rpc.LeaseStream(streamContext, grpc.WaitForReady(true))
	if err != nil {
		cancelStream()
		return fmt.Errorf("open measurement stream: %w", err)
	}

	var counters responseCounters
	workerErrors := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Go(func() {
		workerErrors <- sendRequests(streamContext, stream, &config, bootID)
	})
	workers.Go(func() {
		workerErrors <- receiveResponses(stream, &counters)
	})

	if err := waitInterval(streamContext, workerErrors, config.warmup); err != nil {
		cancelStream()
		workers.Wait()
		return fmt.Errorf("warmup: %w", err)
	}
	initialAcquires := counters.acquires.Load()
	initialReleases := counters.releases.Load()
	started := time.Now()
	if err := waitInterval(streamContext, workerErrors, config.duration); err != nil {
		cancelStream()
		workers.Wait()
		return fmt.Errorf("measurement: %w", err)
	}
	elapsed := time.Since(started)
	acquires := counters.acquires.Load() - initialAcquires
	releases := counters.releases.Load() - initialReleases

	cancelStream()
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
	flags := flag.NewFlagSet("redlease-grpc-load", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&config.target, "target", defaultTarget, "address of an already running gRPC lock-server")
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

func waitForActive(
	parent context.Context,
	rpc redleasev1.RedLeaseClient,
	config *options,
	bootID uint32,
) error {
	ctx, cancel := context.WithTimeout(parent, config.readyTimeout)
	defer cancel()
	stream, err := rpc.LeaseStream(ctx, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("open readiness stream: %w", err)
	}
	defer func() { _ = stream.CloseSend() }()

	for sequence := uint64(1); ; sequence++ {
		leaseID := &redleasev1.LeaseID{ClientId: 1, BootId: bootID, LeaseSeq: sequence}
		requestID := sequence*2 - 1
		if err := stream.Send(&redleasev1.ClientRequest{
			RequestId: requestID,
			Operation: &redleasev1.ClientRequest_Acquire{Acquire: &redleasev1.AcquireRequest{
				Key:            0,
				LeaseId:        leaseID,
				RequestedTtlMs: config.ttlMS,
			}},
		}); err != nil {
			return fmt.Errorf("send readiness Acquire: %w", err)
		}
		response, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receive readiness Acquire: %w", err)
		}
		acquire := response.GetAcquire()
		if acquire == nil {
			return fmt.Errorf("readiness request %d: unexpected response type", response.GetRequestId())
		}
		switch acquire.GetStatus() {
		case redleasev1.LeaseStatus_LEASE_STATUS_OK:
			if err := stream.Send(&redleasev1.ClientRequest{
				RequestId: requestID + 1,
				Operation: &redleasev1.ClientRequest_Release{Release: &redleasev1.ReleaseRequest{
					Key:     0,
					LeaseId: leaseID,
				}},
			}); err != nil {
				return fmt.Errorf("send readiness Release: %w", err)
			}
			releaseResponse, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("receive readiness Release: %w", err)
			}
			release := releaseResponse.GetRelease()
			if release == nil {
				return fmt.Errorf("readiness request %d: unexpected response type", releaseResponse.GetRequestId())
			}
			if status := release.GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
				return fmt.Errorf("readiness Release status: %s", status)
			}
			return nil

		case redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY,
			redleasev1.LeaseStatus_LEASE_STATUS_BUSY:
			timer := time.NewTimer(25 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("wait for ACTIVE server: %w", ctx.Err())
			}

		default:
			return fmt.Errorf("readiness Acquire status: %s", acquire.GetStatus())
		}
	}
}

func sendRequests(
	ctx context.Context,
	stream grpc.BidiStreamingClient[redleasev1.ClientRequest, redleasev1.ServerResponse],
	config *options,
	bootID uint32,
) error {
	defer func() { _ = stream.CloseSend() }()
	for sequence := uint64(1); ; sequence++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		key := (sequence-1)%config.keyCount + 1
		leaseID := &redleasev1.LeaseID{ClientId: 1, BootId: bootID, LeaseSeq: sequence}
		requestID := sequence*2 - 1
		if err := stream.Send(&redleasev1.ClientRequest{
			RequestId: requestID,
			Operation: &redleasev1.ClientRequest_Acquire{Acquire: &redleasev1.AcquireRequest{
				Key:            key,
				LeaseId:        leaseID,
				RequestedTtlMs: config.ttlMS,
			}},
		}); err != nil {
			return fmt.Errorf("send Acquire: %w", err)
		}
		if err := stream.Send(&redleasev1.ClientRequest{
			RequestId: requestID + 1,
			Operation: &redleasev1.ClientRequest_Release{Release: &redleasev1.ReleaseRequest{
				Key:     key,
				LeaseId: leaseID,
			}},
		}); err != nil {
			return fmt.Errorf("send Release: %w", err)
		}
	}
}

func receiveResponses(
	stream grpc.BidiStreamingClient[redleasev1.ClientRequest, redleasev1.ServerResponse],
	counters *responseCounters,
) error {
	for {
		response, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receive response: %w", err)
		}
		switch result := response.GetResult().(type) {
		case *redleasev1.ServerResponse_Acquire:
			if status := result.Acquire.GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
				return fmt.Errorf("Acquire request %d: %s", response.GetRequestId(), status)
			}
			counters.acquires.Add(1)

		case *redleasev1.ServerResponse_Release:
			if status := result.Release.GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
				return fmt.Errorf("Release request %d: %s", response.GetRequestId(), status)
			}
			counters.releases.Add(1)

		default:
			return fmt.Errorf("request %d: unexpected response type", response.GetRequestId())
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
