package client1of1

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/leaseid"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

type fakeLeaseStream struct {
	ctx       context.Context //nolint:containedctx // Test stream owns this context.
	requests  chan *redleasev1.ClientRequest
	responses chan *redleasev1.ServerResponse
}

func (s *fakeLeaseStream) Send(request *redleasev1.ClientRequest) error {
	select {
	case s.requests <- request:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *fakeLeaseStream) Recv() (*redleasev1.ServerResponse, error) {
	select {
	case response := <-s.responses:
		return response, nil
	case <-s.ctx.Done():
		return nil, io.EOF
	}
}

func (s *fakeLeaseStream) CloseSend() error {
	return nil
}

func TestStreamGenerationMultiplexesOutOfOrderResponses(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	stream := &fakeLeaseStream{
		ctx:       streamContext,
		requests:  make(chan *redleasev1.ClientRequest, 2),
		responses: make(chan *redleasev1.ServerResponse, 2),
	}
	generation := newStreamGeneration(stream, cancelStream)
	defer generation.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := generation.submit(ctx, newReleaseRequest([]byte("first"), leaseid.LeaseID{}))
	if err != nil {
		t.Fatalf("submit first: %v", err)
	}
	second, err := generation.submit(ctx, newReleaseRequest([]byte("second"), leaseid.LeaseID{}))
	if err != nil {
		t.Fatalf("submit second: %v", err)
	}
	firstRequest := <-stream.requests
	secondRequest := <-stream.requests
	if firstRequest.GetRequestId() == secondRequest.GetRequestId() {
		t.Fatal("concurrent stream requests have the same request ID")
	}

	stream.responses <- releaseServerResponse(secondRequest.GetRequestId())
	stream.responses <- releaseServerResponse(firstRequest.GetRequestId())
	firstResponse, err := first.await(ctx)
	if err != nil {
		t.Fatalf("await first: %v", err)
	}
	secondResponse, err := second.await(ctx)
	if err != nil {
		t.Fatalf("await second: %v", err)
	}
	if firstResponse.GetRequestId() != firstRequest.GetRequestId() ||
		secondResponse.GetRequestId() != secondRequest.GetRequestId() {
		t.Fatalf("responses were correlated incorrectly: first=%d second=%d",
			firstResponse.GetRequestId(), secondResponse.GetRequestId())
	}
}

func TestStreamGenerationCancellationUnblocksAwait(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	stream := &fakeLeaseStream{
		ctx:       streamContext,
		requests:  make(chan *redleasev1.ClientRequest, 1),
		responses: make(chan *redleasev1.ServerResponse),
	}
	generation := newStreamGeneration(stream, cancelStream)
	defer generation.Close()

	ctx, cancel := context.WithCancel(context.Background())
	future, err := generation.submit(ctx, newReleaseRequest([]byte("key"), leaseid.LeaseID{}))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	cancel()
	if _, err := future.await(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
}

func TestStreamGenerationDeadlineUnblocksSend(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	stream := &fakeLeaseStream{
		ctx:       streamContext,
		requests:  make(chan *redleasev1.ClientRequest),
		responses: make(chan *redleasev1.ServerResponse),
	}
	generation := newStreamGeneration(stream, cancelStream)
	defer generation.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := generation.submit(ctx, newReleaseRequest([]byte("key"), leaseid.LeaseID{})); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("submit error = %v, want context.DeadlineExceeded", err)
	}
}

func TestFailedAcquireSubmitsCleanupReleaseBeforeReturning(t *testing.T) {
	streamContext, cancelStream := context.WithCancel(context.Background())
	stream := &fakeLeaseStream{
		ctx:       streamContext,
		requests:  make(chan *redleasev1.ClientRequest, 2),
		responses: make(chan *redleasev1.ServerResponse, 2),
	}
	generation := newStreamGeneration(stream, cancelStream)
	defer generation.Close()

	clientContext, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	idGenerator, err := leaseid.NewGenerator(7)
	if err != nil {
		t.Fatalf("new lease ID generator: %v", err)
	}
	client := &Client{
		responseTimeout: 100 * time.Millisecond,
		logger:          slog.New(slog.DiscardHandler),
		idGenerator:     idGenerator,
		ctx:             clientContext,
		cancel:          cancelClient,
		generation:      generation,
		changed:         make(chan struct{}),
	}

	acquireSent := make(chan *redleasev1.ClientRequest, 1)
	go func() {
		request := <-stream.requests
		acquireSent <- request
		stream.responses <- &redleasev1.ServerResponse{
			RequestId: request.GetRequestId(),
			Result: &redleasev1.ServerResponse_Acquire{Acquire: &redleasev1.AcquireResponse{
				Status: redleasev1.LeaseStatus_LEASE_STATUS_BUSY,
			}},
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	lease, err := client.Acquire(ctx, []byte("busy"), 1000)
	cancel()
	if !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("Acquire error = %v, want ErrNotAcquired", err)
	}
	if lease != nil {
		t.Fatal("failed Acquire returned a lease")
	}

	acquireRequest := <-acquireSent
	select {
	case releaseRequest := <-stream.requests:
		acquireID := acquireRequest.GetAcquire().GetLeaseId()
		if releaseRequest.GetRelease() == nil {
			t.Fatal("cleanup request is not Release")
		}
		releaseID := releaseRequest.GetRelease().GetLeaseId()
		if acquireID.GetClientId() != releaseID.GetClientId() ||
			acquireID.GetBootId() != releaseID.GetBootId() ||
			acquireID.GetLeaseSeq() != releaseID.GetLeaseSeq() {
			t.Fatal("cleanup Release used a different lease ID")
		}
		stream.responses <- releaseServerResponse(releaseRequest.GetRequestId())
	default:
		t.Fatal("failed Acquire returned before submitting cleanup Release")
	}
}

func releaseServerResponse(requestID uint64) *redleasev1.ServerResponse {
	return &redleasev1.ServerResponse{
		RequestId: requestID,
		Result: &redleasev1.ServerResponse_Release{Release: &redleasev1.ReleaseResponse{
			Status: redleasev1.LeaseStatus_LEASE_STATUS_OK,
		}},
	}
}
