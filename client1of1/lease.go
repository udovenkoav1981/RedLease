package client1of1

import (
	"context"
	"errors"
	"sync"

	"github.com/udovenkoav1981/RedLease/internal/boottime"
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
	// ErrLeaseReleased is returned when Renew races with or follows Release.
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

// Lease is a locally confirmed lease on the configured lock-server.
type Lease struct {
	client   *Client
	sequence uint64
	key      uint64
	now      uint64

	stateMu    sync.RWMutex
	lifecycle  leaseLifecycle
	validUntil uint64

	renewMu     sync.Mutex
	releaseOnce sync.Once
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
		now:       boottime.Now(),
		lifecycle: leaseActive,
	}

	operationContext, cancelOperation := c.operationContext(ctx)
	accepted := false
	future, err := c.submit(operationContext, c.newAcquireRequest(lease.key, sequence, ttlMS))
	if err == nil {
		var response *redleasev1.ServerResponse
		response, err = future.await(operationContext)
		if err == nil {
			accepted, err = lease.acceptAcquireResponse(ctx, response)
		}
	}
	cancelOperation()
	if err == nil && accepted {
		return &lease, nil
	}

	c.release(lease.key, sequence) //nolint:contextcheck // Cleanup must outlive caller cancellation.
	return nil, &operationError{kind: ErrNotAcquired, cause: err}
}

func (l *Lease) acceptAcquireResponse(
	caller context.Context,
	response *redleasev1.ServerResponse,
) (bool, error) {
	if err := l.client.cancellationError(caller); err != nil {
		return false, err
	}
	acquire := response.GetAcquire()
	if acquire == nil {
		return false, errors.New("Acquire received a non-Acquire response")
	}
	switch acquire.GetStatus() {
	case redleasev1.LeaseStatus_LEASE_STATUS_OK,
		redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED:
		validUntil := candidateValidUntil(l.now, acquire.GetTtlMs())
		if boottime.Now() >= validUntil {
			return false, nil
		}
		l.validUntil = validUntil
		return true, nil
	case redleasev1.LeaseStatus_LEASE_STATUS_KEY_LIMIT_REACHED:
		return false, ErrKeyLimitReached
	default:
		return false, nil
	}
}

// RemainingTTLms returns the remaining local validity in milliseconds.
func (l *Lease) RemainingTTLms() uint64 {
	l.stateMu.RLock()
	validUntil := l.validUntil
	active := l.lifecycle == leaseActive
	l.stateMu.RUnlock()
	if !active {
		return 0
	}
	return boottime.Remaining(validUntil, boottime.Now())
}

// Renew attempts to extend this lease on its lock-server. Failure leaves the
// previously confirmed validUntil unchanged.
func (l *Lease) Renew(ctx context.Context, ttlMS uint64) error {
	l.renewMu.Lock()
	defer l.renewMu.Unlock()

	l.stateMu.Lock()
	if l.lifecycle != leaseActive {
		l.stateMu.Unlock()
		return &operationError{kind: ErrNotRenewed, cause: ErrLeaseReleased}
	}
	l.now = boottime.Now()
	l.stateMu.Unlock()

	operationContext, cancelOperation := l.client.operationContext(ctx)
	renewed := false
	future, err := l.client.submit(operationContext, l.client.newRenewRequest(l.key, l.sequence, ttlMS))

	if err == nil {
		var response *redleasev1.ServerResponse
		response, err = future.await(operationContext)
		if err == nil {
			renewed, err = l.acceptRenewResponse(ctx, response)
		}
	}
	cancelOperation()
	if err != nil || !renewed {
		return &operationError{kind: ErrNotRenewed, cause: err}
	}
	return nil
}

func (l *Lease) acceptRenewResponse(
	caller context.Context,
	response *redleasev1.ServerResponse,
) (bool, error) {
	if err := l.client.cancellationError(caller); err != nil {
		return false, err
	}
	renew := response.GetRenew()
	if renew == nil {
		return false, errors.New("Renew received a non-Renew response")
	}
	if renew.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		return false, nil
	}
	validUntil := candidateValidUntil(l.now, renew.GetTtlMs())
	if boottime.Now() >= validUntil {
		return false, nil
	}

	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.lifecycle != leaseActive {
		return false, ErrLeaseReleased
	}
	if validUntil > l.validUntil {
		l.validUntil = validUntil
	}
	return true, nil
}

// Release immediately makes the local lease invalid and queues one idempotent
// best-effort Release to the server. It does not wait for a server response.
// Repeated calls do nothing.
func (l *Lease) Release() {
	l.releaseOnce.Do(func() {
		l.stateMu.Lock()
		l.lifecycle = leaseReleased
		l.validUntil = 0
		l.stateMu.Unlock()

		l.client.release(l.key, l.sequence)
	})
}

func (c *Client) release(key, sequence uint64) {
	ctx, cancel := context.WithTimeout(c.ctx, c.responseTimeout)
	defer cancel()
	_ = c.submitNoResponse(ctx, c.newReleaseRequest(key, sequence))
}

func candidateValidUntil(operationStart, ttlMS uint64) uint64 {
	if ttlMS <= safetyMarginMS {
		return operationStart
	}
	return operationStart + (ttlMS - safetyMarginMS)
}

func (c *Client) newAcquireRequest(key, sequence, ttlMS uint64) *redleasev1.ClientRequest {
	return &redleasev1.ClientRequest{
		Operation: &redleasev1.ClientRequest_Acquire{Acquire: &redleasev1.AcquireRequest{
			Key:            key,
			LeaseId:        &redleasev1.LeaseID{ClientId: c.clientID, BootId: c.bootID, LeaseSeq: sequence},
			RequestedTtlMs: ttlMS,
		}},
	}
}

func (c *Client) newRenewRequest(key, sequence, ttlMS uint64) *redleasev1.ClientRequest {
	return &redleasev1.ClientRequest{
		Operation: &redleasev1.ClientRequest_Renew{Renew: &redleasev1.RenewRequest{
			Key:            key,
			LeaseId:        &redleasev1.LeaseID{ClientId: c.clientID, BootId: c.bootID, LeaseSeq: sequence},
			RequestedTtlMs: ttlMS,
		}},
	}
}

func (c *Client) newReleaseRequest(key, sequence uint64) *redleasev1.ClientRequest {
	return &redleasev1.ClientRequest{
		Operation: &redleasev1.ClientRequest_Release{Release: &redleasev1.ReleaseRequest{
			Key:     key,
			LeaseId: &redleasev1.LeaseID{ClientId: c.clientID, BootId: c.bootID, LeaseSeq: sequence},
		}},
	}
}
