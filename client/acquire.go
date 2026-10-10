package client

import (
	"context"
	"errors"
	"slices"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
)

// ErrNotAcquired identifies every Acquire result which did not establish a
// currently valid configured quorum.
var ErrNotAcquired = errors.New("RedLease lease not acquired")

// ErrKeyLimitReached means at least one server reported that its resident
// lease-key limit was reached during an unsuccessful Acquire.
var ErrKeyLimitReached = errors.New("RedLease server key limit reached")

type notAcquiredError struct {
	cause error
}

func (e *notAcquiredError) Error() string {
	if e.cause == nil {
		return ErrNotAcquired.Error()
	}
	return ErrNotAcquired.Error() + ": " + e.cause.Error()
}

func (e *notAcquiredError) Unwrap() error {
	return e.cause
}

func (e *notAcquiredError) Is(target error) bool {
	return target == ErrNotAcquired
}

// Acquire makes one attempt to establish a currently valid lease quorum for
// the application-defined uint64 key. The caller owns retry policy; every new
// call uses a new lease ID.
func (c *Client) Acquire(
	ctx context.Context,
	key uint64,
	ttlMS uint64,
) (*Lease, error) {
	if err := c.acquireCancellationError(ctx); err != nil {
		return nil, &notAcquiredError{cause: err}
	}
	sequence := c.nextSequence.Add(1)
	lease := newLease(c, sequence, key, ttlMS)
	operationStart := lease.now
	serverCount := len(c.replicas)
	quorumSize := c.quorum.size()

	responses := newReplicaResponses(c, serverCount)
	var firstFailure error
	received := 0

	for replica := range c.replicas {
		request := c.newAcquireRequest(lease.key, sequence, ttlMS)
		if err := responses.submit(lease.ctx, replica, request); err != nil {
			received++
			if firstFailure == nil {
				firstFailure = err
			}
		}
	}

	if err := c.acquireCancellationError(ctx); err != nil {
		responses.abort(err)
		lease.cancel()
		c.cleanupFailedAcquire(lease.key, sequence) //nolint:contextcheck // Cleanup must outlive caller cancellation.
		return nil, &notAcquiredError{cause: err}
	}

	var (
		candidates   = make([]time.Time, serverCount)
		successful   = make([]bool, serverCount)
		keyLimitSeen bool
	)

	for received < serverCount {
		completed, event := responses.next(lease.ctx.Done(), ctx.Done())
		switch event {
		case responseReceived:
			received++
			result := completed.result
			if result.err != nil {
				if firstFailure == nil {
					firstFailure = result.err
				}
			} else if result.response.Operation != redleasev1.ClientOperationACQUIRE {
				if firstFailure == nil {
					firstFailure = errors.New("Acquire received a non-Acquire response")
				}
			} else if isSuccessfulAcquire(result.response.Status) {
				now := time.Now()
				successful[completed.replica] = true
				candidates[completed.replica] = candidateValidUntil(
					operationStart,
					result.response.TTLMS,
				)
				if now.Before(candidates[completed.replica]) {
					lease.markConfirmed(completed.replica, candidates[completed.replica])
				}

				validUntil, hasQuorum := bestAcquireQuorum(
					candidates,
					successful,
					quorumSize,
				)
				if hasQuorum && now.Before(validUntil) {
					if err := c.acquireCancellationError(ctx); err != nil {
						firstFailure = err
						received = serverCount
						break
					}

					lease.setAcquireValidity(validUntil)
					go c.collectRemainingAcquireResults(
						lease,
						operationStart,
						responses,
					)
					return lease, nil
				}
			} else {
				switch result.response.Status {
				case redleasev1.LeaseStatusKEY_LIMIT_REACHED:
					keyLimitSeen = true
				default:
					// Other statuses are represented by notAcquiredError below.
				}
			}

			if !acquireQuorumStillPossible(
				candidates,
				successful,
				serverCount-received,
				time.Now(),
				quorumSize,
			) {
				received = serverCount
			}

		case callerCanceled:
			firstFailure = ctx.Err()
			received = serverCount
		case lifecycleCanceled:
			firstFailure = ErrClientClosed
			received = serverCount
		}
	}

	responses.abort(context.Canceled)
	lease.cancel()
	c.cleanupFailedAcquire(lease.key, sequence) //nolint:contextcheck // Cleanup must outlive caller cancellation.
	if keyLimitSeen {
		firstFailure = errors.Join(firstFailure, ErrKeyLimitReached)
	}
	return nil, &notAcquiredError{cause: firstFailure}
}

func (c *Client) acquireCancellationError(callerContext context.Context) error {
	if err := callerContext.Err(); err != nil {
		return err
	}
	if c.ctx.Err() != nil {
		return ErrClientClosed
	}
	return nil
}

func (c *Client) collectRemainingAcquireResults(
	lease *Lease,
	operationStart time.Time,
	responses replicaResponses,
) {
	for responses.remaining != 0 {
		completed, event := responses.next(lease.ctx.Done(), nil)
		if event == lifecycleCanceled {
			responses.abort(context.Canceled)
			return
		}
		result := completed.result
		if result.err == nil &&
			result.response.Operation == redleasev1.ClientOperationACQUIRE &&
			isSuccessfulAcquire(result.response.Status) {
			candidate := candidateValidUntil(
				operationStart,
				result.response.TTLMS,
			)
			if time.Now().Before(candidate) {
				lease.markConfirmed(completed.replica, candidate)
			}
		}
	}
	lease.backgroundHeal()
}

func acquireQuorumStillPossible(
	candidates []time.Time,
	successful []bool,
	remaining int,
	now time.Time,
	quorumSize int,
) bool {
	usable := 0
	for replica, success := range successful {
		if success && now.Before(candidates[replica]) {
			usable++
		}
	}
	return usable+remaining >= quorumSize
}

func (c *Client) cleanupFailedAcquire(key, sequence uint64) {
	c.releaseAll(key, sequence)
}

func bestAcquireQuorum(
	candidates []time.Time,
	successful []bool,
	quorumSize int,
) (time.Time, bool) {
	validities := make([]time.Time, 0, len(candidates))
	for replica, success := range successful {
		if success {
			validities = append(validities, candidates[replica])
		}
	}
	if len(validities) < quorumSize {
		return time.Time{}, false
	}

	slices.SortFunc(validities, time.Time.Compare)
	return validities[len(validities)-quorumSize], true
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
