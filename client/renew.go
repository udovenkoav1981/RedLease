package client

import (
	"context"
	"errors"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
)

var (
	// ErrNotRenewed identifies a Renew which did not establish a new valid
	// quorum. The previously confirmed validity is not revoked.
	ErrNotRenewed = errors.New("RedLease lease not renewed")

	// ErrLeaseReleased is returned when Renew races with or follows Release.
	ErrLeaseReleased = errors.New("RedLease lease released")
)

type notRenewedError struct {
	cause error
}

func (e *notRenewedError) Error() string {
	if e.cause == nil {
		return ErrNotRenewed.Error()
	}
	return ErrNotRenewed.Error() + ": " + e.cause.Error()
}

func (e *notRenewedError) Unwrap() error {
	return e.cause
}

func (e *notRenewedError) Is(target error) bool {
	return target == ErrNotRenewed
}

// Renew attempts to extend this lease on a configured quorum. Failure leaves
// the previously confirmed validUntil unchanged.
func (l *Lease) Renew(ctx context.Context, ttlMS uint64) error {
	l.renewMu.Lock()
	defer l.renewMu.Unlock()

	operationStart, started := l.beginRenewBatch()
	if !started {
		return &notRenewedError{cause: ErrLeaseReleased}
	}
	serverCount := len(l.client.replicas)
	quorumSize := l.client.quorum.size()
	batchActive := true
	defer func() {
		if batchActive {
			l.endSubmitBatch()
		}
	}()

	responses := newReplicaResponses(l.client, serverCount)
	var firstFailure error
	received := 0

	for replica := range l.client.replicas {
		request := l.client.newRenewRequest(l.key, l.sequence, ttlMS)
		if err := responses.submit(l.ctx, replica, request); err != nil {
			received++
			l.clearConfirmed(replica)
			if firstFailure == nil {
				firstFailure = err
			}
		}
	}
	l.endSubmitBatch()
	batchActive = false

	if err := l.renewCancellationError(ctx); err != nil {
		if responses.remaining != 0 {
			go l.collectRemainingRenewResults(operationStart, responses)
		}
		return &notRenewedError{cause: err}
	}

	var (
		candidates = make([]time.Time, serverCount)
		successful = make([]bool, serverCount)
	)

	collecting := true
	for received < serverCount && collecting {
		completed, event := responses.next(l.ctx.Done(), ctx.Done())
		switch event {
		case responseReceived:
			received++
			result := completed.result
			if result.err != nil {
				l.clearConfirmed(completed.replica)
				if firstFailure == nil {
					firstFailure = result.err
				}
			} else if result.response.Operation != redleasev1.ClientOperationRENEW {
				l.clearConfirmed(completed.replica)
				if firstFailure == nil {
					firstFailure = errors.New("Renew received a non-Renew response")
				}
			} else if result.response.Status != redleasev1.LeaseStatusOK {
				l.clearConfirmed(completed.replica)
			} else {
				now := time.Now()
				successful[completed.replica] = true
				candidates[completed.replica] = candidateValidUntil(
					operationStart,
					result.response.TTLMS,
				)
				if now.Before(candidates[completed.replica]) {
					l.markConfirmed(completed.replica, candidates[completed.replica])
				} else {
					l.clearConfirmed(completed.replica)
				}

				quorumValidUntil, hasQuorum := bestAcquireQuorum(
					candidates,
					successful,
					quorumSize,
				)
				if hasQuorum && now.Before(quorumValidUntil) {
					if err := l.renewCancellationError(ctx); err != nil {
						firstFailure = err
						collecting = false
						break
					}
					if !l.applyRenewValidity(quorumValidUntil) {
						responses.abort(context.Canceled)
						return &notRenewedError{cause: ErrLeaseReleased}
					}

					if responses.remaining != 0 {
						go l.collectRemainingRenewResults(operationStart, responses)
					}
					return nil
				}
			}

			if !acquireQuorumStillPossible(
				candidates,
				successful,
				serverCount-received,
				time.Now(),
				quorumSize,
			) {
				collecting = false
			}

		case callerCanceled:
			firstFailure = ctx.Err()
			collecting = false
		case lifecycleCanceled:
			firstFailure = l.renewCancellationError(ctx)
			responses.abort(context.Canceled)
			collecting = false
		}
	}

	if responses.remaining != 0 {
		go l.collectRemainingRenewResults(operationStart, responses)
	}
	return &notRenewedError{cause: firstFailure}
}

func (l *Lease) collectRemainingRenewResults(
	operationStart time.Time,
	responses replicaResponses,
) {
	for responses.remaining != 0 {
		completed, event := responses.next(l.ctx.Done(), nil)
		if event == lifecycleCanceled {
			responses.abort(context.Canceled)
			return
		}
		result := completed.result
		if result.err != nil ||
			result.response.Operation != redleasev1.ClientOperationRENEW ||
			result.response.Status != redleasev1.LeaseStatusOK {
			l.clearConfirmed(completed.replica)
			continue
		}

		candidate := candidateValidUntil(
			operationStart,
			result.response.TTLMS,
		)
		if time.Now().Before(candidate) {
			l.markConfirmed(completed.replica, candidate)
		} else {
			l.clearConfirmed(completed.replica)
		}
	}
}

func (l *Lease) renewCancellationError(callerContext context.Context) error {
	if err := callerContext.Err(); err != nil {
		return err
	}
	l.stateMu.RLock()
	active := l.lifecycle == leaseActive
	l.stateMu.RUnlock()
	if !active {
		return ErrLeaseReleased
	}
	if l.client.ctx.Err() != nil {
		return ErrClientClosed
	}
	return nil
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
