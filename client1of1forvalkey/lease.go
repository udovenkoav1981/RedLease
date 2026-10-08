package client1of1forvalkey

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

const safetyMarginMS uint64 = 100

var (
	// ErrNotAcquired identifies an Acquire which did not establish a currently
	// valid lease in Valkey.
	ErrNotAcquired = errors.New("RedLease Valkey 1/1 lease not acquired")
	// ErrKeyLimitReached maps a Valkey OOM rejection to the closest client1of1
	// capacity error.
	ErrKeyLimitReached = errors.New("valkey memory limit reached")
	// ErrNotRenewed identifies a Renew which did not establish new validity.
	// Previously confirmed validity is not revoked.
	ErrNotRenewed = errors.New("RedLease Valkey 1/1 lease not renewed")
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

// Lease is a locally confirmed lease in the configured Valkey server. Its
// public methods must not be called concurrently on the same Lease.
type Lease struct {
	client    *Client
	valkeyKey string
	token     string
	now       time.Time

	lifecycle  leaseLifecycle
	validUntil time.Time
}

// Acquire makes one attempt to establish a currently valid lease for the
// application-defined uint64 key. The caller owns retry policy; every new call
// uses a new ownership token.
func (c *Client) Acquire(ctx context.Context, key, ttlMS uint64) (*Lease, error) {
	if err := c.cancellationError(ctx); err != nil {
		return nil, &operationError{kind: ErrNotAcquired, cause: err}
	}

	lease := Lease{
		client:    c,
		valkeyKey: valkeyLeaseKey(key),
		token:     c.newOwnershipToken(),
		now:       time.Now(),
		lifecycle: leaseActive,
	}
	effectiveTTLMS := min(ttlMS, protocol.MaxTTLMS)
	command, generation := c.execute(
		ctx,
		operationAcquire,
		lease.valkeyKey,
		lease.token,
		effectiveTTLMS,
	)
	result := c.loadOperationResult(command, generation)
	if isValkeyOOM(result.err) {
		result.status = acquireStatusMemoryLimit
		result.err = nil
	}
	err := result.err
	if err != nil {
		err = c.operationError(ctx, err)
	} else if cancellationErr := c.cancellationError(ctx); cancellationErr != nil {
		err = cancellationErr
	} else if result.status == acquireStatusAcquired || result.status == acquireStatusAlreadyOwned {
		validUntil := candidateValidUntil(lease.now, result.ttlMS)
		if time.Now().Before(validUntil) {
			lease.validUntil = validUntil
			return &lease, nil
		}
	} else if result.status == acquireStatusMemoryLimit {
		err = ErrKeyLimitReached
	}

	c.queueRelease(lease.valkeyKey, lease.token)
	return nil, &operationError{kind: ErrNotAcquired, cause: err}
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

// Renew attempts to extend this lease in Valkey. Failure leaves the previously
// confirmed validUntil unchanged.
func (l *Lease) Renew(ctx context.Context, ttlMS uint64) error {
	if l.lifecycle != leaseActive {
		return &operationError{kind: ErrNotRenewed, cause: ErrLeaseReleased}
	}
	l.now = time.Now()
	effectiveTTLMS := min(ttlMS, protocol.MaxTTLMS)
	command, generation := l.client.execute(
		ctx,
		operationRenew,
		l.valkeyKey,
		l.token,
		effectiveTTLMS,
	)
	result := l.client.loadOperationResult(command, generation)
	err := result.err
	if err != nil {
		err = l.client.operationError(ctx, err)
	} else if cancellationErr := l.client.cancellationError(ctx); cancellationErr != nil {
		err = cancellationErr
	} else if result.status == 1 {
		validUntil := candidateValidUntil(l.now, result.ttlMS)
		if time.Now().Before(validUntil) {
			if validUntil.After(l.validUntil) {
				l.validUntil = validUntil
			}
			return nil
		}
	}
	return &operationError{kind: ErrNotRenewed, cause: err}
}

// Release immediately makes the local lease invalid and queues one
// owner-checked best-effort Release in Valkey. It does not wait for a server
// response. Repeated calls do nothing.
func (l *Lease) Release() {
	if l.lifecycle == leaseReleased {
		return
	}
	l.lifecycle = leaseReleased
	l.validUntil = time.Time{}
	l.client.queueRelease(l.valkeyKey, l.token)
}

func (c *Client) queueRelease(valkeyKey, token string) {
	c.execute(c.ctx, operationRelease, valkeyKey, token, 0)
}

func candidateValidUntil(operationStart time.Time, ttlMS uint64) time.Time {
	if ttlMS <= safetyMarginMS || ttlMS > protocol.MaxTTLMS {
		return operationStart
	}
	return operationStart.Add(time.Duration(ttlMS-safetyMarginMS) * time.Millisecond)
}

func valkeyLeaseKey(key uint64) string {
	var encoded [17]byte
	copy(encoded[:9], "redlease:")
	binary.BigEndian.PutUint64(encoded[9:], key)
	return string(encoded[:])
}

func (c *Client) newOwnershipToken() string {
	var token [16]byte
	binary.BigEndian.PutUint32(token[0:4], c.clientID)
	binary.BigEndian.PutUint32(token[4:8], c.bootID)
	binary.BigEndian.PutUint64(token[8:16], c.nextSequence.Add(1))
	return string(token[:])
}
