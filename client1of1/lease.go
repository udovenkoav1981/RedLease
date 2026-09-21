package client1of1

import (
	"context"
	"errors"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

const safetyMarginMS uint64 = 100

var (
	// ErrNotAcquired identifies an Acquire which did not establish a currently
	// valid lease on the server.
	ErrNotAcquired = errors.New("RedLease 1/1 lease not acquired")
	// ErrKeyLimitReached means the server cannot store another lease key.
	ErrKeyLimitReached = errors.New("RedLease server key limit reached")
	// ErrNotRenewed identifies a Renew which did not establish new validity.
	// Previously confirmed validity is not revoked.
	ErrNotRenewed = errors.New("RedLease 1/1 lease not renewed")
	// ErrLeaseReleased is returned when Renew follows Release.
	ErrLeaseReleased = errors.New("RedLease lease released")
)

type operationError struct {
	kind  error
	cause error
}

func (e *operationError) Error() string {
	if e.cause == nil {
		return e.kind.Error()
	}
	return e.kind.Error() + ": " + e.cause.Error()
}

func (e *operationError) Unwrap() error {
	return e.cause
}

func (e *operationError) Is(target error) bool {
	return target == e.kind
}

type leaseLifecycle uint8

const (
	leaseActive leaseLifecycle = iota
	leaseReleased
)

// Lease is a locally confirmed lease on the configured lock-server. Its public
// methods must not be called concurrently on the same Lease.
type Lease struct {
	client   *Client
	sequence uint64
	key      uint64
	now      time.Time

	lifecycle  leaseLifecycle
	validUntil time.Time
}

// Acquire makes one attempt to establish a currently valid lease for the
// application-defined uint64 key. The caller owns retry policy; every new call
// uses a new lease ID.
func (c *Client) Acquire(
	ctx context.Context,
	key uint64,
	ttlMS uint64,
) (*Lease, error) {
	if err := c.cancellationError(ctx); err != nil {
		return nil, &operationError{kind: ErrNotAcquired, cause: err}
	}
	sequence := c.nextSequence.Add(1)

	lease := Lease{
		client:    c,
		sequence:  sequence,
		key:       key,
		now:       time.Now(),
		lifecycle: leaseActive,
	}

	accepted := false
	future, err := c.submit(c.newAcquireRequest(lease.key, sequence, ttlMS))
	if err == nil {
		var response protocol.Response
		response, err = c.awaitResponse(ctx, future)
		if err == nil {
			accepted, err = lease.acceptAcquireResponse(ctx, response)
		}
	}
	if err == nil && accepted {
		return &lease, nil
	}

	c.release(lease.key, sequence) //nolint:contextcheck // Cleanup must outlive caller cancellation.
	return nil, &operationError{kind: ErrNotAcquired, cause: err}
}

func (l *Lease) acceptAcquireResponse(
	caller context.Context,
	response protocol.Response,
) (bool, error) {
	if err := l.client.cancellationError(caller); err != nil {
		return false, err
	}
	if response.Operation != protocol.OperationAcquire {
		return false, errors.New("Acquire received a non-Acquire response")
	}
	switch response.Status {
	case protocol.StatusOK, protocol.StatusAlreadyOwned:
		validUntil := candidateValidUntil(l.now, response.TTLMS)
		if !time.Now().Before(validUntil) {
			return false, nil
		}
		l.validUntil = validUntil
		return true, nil
	case protocol.StatusKeyLimitReached:
		return false, ErrKeyLimitReached
	default:
		return false, nil
	}
}

// RemainingTTLms returns the remaining local validity in milliseconds.
func (l *Lease) RemainingTTLms() uint64 {
	if l.lifecycle != leaseActive {
		return 0
	}
	remaining := time.Until(l.validUntil).Milliseconds()
	if remaining <= 0 {
		return 0
	}
	return uint64(remaining)
}

// Renew attempts to extend this lease on its lock-server. Failure leaves the
// previously confirmed validUntil unchanged.
func (l *Lease) Renew(ctx context.Context, ttlMS uint64) error {
	if l.lifecycle != leaseActive {
		return &operationError{kind: ErrNotRenewed, cause: ErrLeaseReleased}
	}
	l.now = time.Now()

	renewed := false
	future, err := l.client.submit(l.client.newRenewRequest(l.key, l.sequence, ttlMS))

	if err == nil {
		var response protocol.Response
		response, err = l.client.awaitResponse(ctx, future)
		if err == nil {
			renewed, err = l.acceptRenewResponse(ctx, response)
		}
	}
	if err != nil || !renewed {
		return &operationError{kind: ErrNotRenewed, cause: err}
	}
	return nil
}

func (l *Lease) acceptRenewResponse(
	caller context.Context,
	response protocol.Response,
) (bool, error) {
	if err := l.client.cancellationError(caller); err != nil {
		return false, err
	}
	if response.Operation != protocol.OperationRenew {
		return false, errors.New("Renew received a non-Renew response")
	}
	if response.Status != protocol.StatusOK {
		return false, nil
	}
	validUntil := candidateValidUntil(l.now, response.TTLMS)
	if !time.Now().Before(validUntil) {
		return false, nil
	}

	if validUntil.After(l.validUntil) {
		l.validUntil = validUntil
	}
	return true, nil
}

// Release immediately makes the local lease invalid and queues one idempotent
// best-effort Release to the server. It does not wait for a server response.
// Repeated calls do nothing.
func (l *Lease) Release() {
	if l.lifecycle == leaseReleased {
		return
	}
	l.lifecycle = leaseReleased
	l.validUntil = time.Time{}
	l.client.release(l.key, l.sequence)
}

func (c *Client) release(key, sequence uint64) {
	_ = c.submitNoResponse(c.newReleaseRequest(key, sequence))
}

func candidateValidUntil(operationStart time.Time, ttlMS uint64) time.Time {
	if ttlMS <= safetyMarginMS || ttlMS > protocol.MaxTTLMS {
		return operationStart
	}
	return operationStart.Add(time.Duration(ttlMS-safetyMarginMS) * time.Millisecond)
}

func (c *Client) newAcquireRequest(key, sequence, ttlMS uint64) *outboundConnectionRequest {
	outbound := c.newOutboundRequest()
	builder := outbound.builder
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddRequestId(builder, c.nextRequestID.Add(1))
	redleasev1.ClientRequestAddOperation(builder, redleasev1.ClientOperationACQUIRE)
	redleasev1.ClientRequestAddAcquire(builder, redleasev1.CreateAcquireRequest(
		builder,
		key,
		c.clientID,
		c.bootID,
		sequence,
		ttlMS,
	))
	c.finishOutboundRequest(outbound)
	return outbound
}

func (c *Client) newRenewRequest(key, sequence, ttlMS uint64) *outboundConnectionRequest {
	outbound := c.newOutboundRequest()
	builder := outbound.builder
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddRequestId(builder, c.nextRequestID.Add(1))
	redleasev1.ClientRequestAddOperation(builder, redleasev1.ClientOperationRENEW)
	redleasev1.ClientRequestAddRenew(builder, redleasev1.CreateRenewRequest(
		builder,
		key,
		c.clientID,
		c.bootID,
		sequence,
		ttlMS,
	))
	c.finishOutboundRequest(outbound)
	return outbound
}

func (c *Client) newReleaseRequest(key, sequence uint64) *outboundConnectionRequest {
	outbound := c.newOutboundRequest()
	builder := outbound.builder
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddRequestId(builder, c.nextRequestID.Add(1))
	redleasev1.ClientRequestAddOperation(builder, redleasev1.ClientOperationRELEASE)
	redleasev1.ClientRequestAddRelease(builder, redleasev1.CreateReleaseRequest(
		builder,
		key,
		c.clientID,
		c.bootID,
		sequence,
	))
	c.finishOutboundRequest(outbound)
	return outbound
}

func (c *Client) newOutboundRequest() *outboundConnectionRequest {
	if pooled := c.requestPool.Get(); pooled != nil {
		if outbound, ok := pooled.(*outboundConnectionRequest); ok {
			outbound.builder.Reset()
			return outbound
		}
	}
	return &outboundConnectionRequest{
		builder: flatbuffers.NewBuilder(protocol.NewBuilderSize),
		pool:    &c.requestPool,
	}
}

func (*Client) finishOutboundRequest(outbound *outboundConnectionRequest) {
	root := redleasev1.ClientRequestEnd(outbound.builder)
	redleasev1.FinishSizePrefixedClientRequestBuffer(outbound.builder, root)
	frame := outbound.builder.FinishedBytes()
	rootOffset := flatbuffers.GetUOffsetT(frame[flatbuffers.SizeUint32:]) +
		flatbuffers.UOffsetT(flatbuffers.SizeUint32)
	outbound.request.Init(frame, rootOffset)
}
