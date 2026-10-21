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

	// ErrLeaseReleased is returned when Renew follows Release.
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
	l.stateMu.Lock()
	if l.lifecycle != leaseActive {
		l.stateMu.Unlock()
		return &notRenewedError{cause: ErrLeaseReleased}
	}
	l.now = time.Now()
	operationStart := l.now
	l.stateMu.Unlock()

	serverCount := len(l.client.replicas)
	quorumSize := l.client.quorum.size()

	responses := l.client.acquireReplicaResponses()
	responsesOwned := true
	defer func() {
		if responsesOwned {
			responses.release()
			l.startBackgroundHealing()
		}
	}()
	var firstFailure error
	received := 0

	for replica := range l.client.replicas {
		request := l.client.newRenewRequest(l.key, l.sequence, ttlMS)
		if err := responses.submit(ctx, replica, request); err != nil {
			received++
			l.clearConfirmed(replica)
			if firstFailure == nil {
				firstFailure = err
			}
		}
	}
	if err := l.renewCancellationError(ctx); err != nil {
		if responses.remaining != 0 {
			responsesOwned = false
			go l.collectRemainingRenewResults(operationStart, responses)
		}
		return &notRenewedError{cause: err}
	}

	var (
		candidates [maxServerCount]time.Time
		successful [maxServerCount]bool
	)

	collecting := true
	for received < serverCount && collecting {
		completed, event := responses.next(nil, ctx.Done())
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
					candidates[:serverCount],
					successful[:serverCount],
					quorumSize,
				)
				if hasQuorum && now.Before(quorumValidUntil) {
					if err := l.renewCancellationError(ctx); err != nil {
						firstFailure = err
						collecting = false
						break
					}
					l.extendValidity(quorumValidUntil)

					if responses.remaining != 0 {
						responsesOwned = false
						go l.collectRemainingRenewResults(operationStart, responses)
					}
					return nil
				}
			}

			if !acquireQuorumStillPossible(
				candidates[:serverCount],
				successful[:serverCount],
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
			// Renew passes no lifecycle channel because public operations on one
			// Lease must be sequential.
			firstFailure = ErrLeaseReleased
			collecting = false
		}
	}

	if responses.remaining != 0 {
		responsesOwned = false
		go l.collectRemainingRenewResults(operationStart, responses)
	}
	return &notRenewedError{cause: firstFailure}
}

func (l *Lease) collectRemainingRenewResults(
	operationStart time.Time,
	responses *replicaResponses,
) {
	defer func() {
		responses.release()
		l.startBackgroundHealing()
	}()
	for responses.remaining != 0 {
		completed, event := responses.next(l.releaseDone, nil)
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
	if l.client.ctx.Err() != nil {
		return ErrClientClosed
	}
	return nil
}

func (c *Client) newRenewRequest(key, sequence, ttlMS uint64) outboundConnectionRequest {
	return outboundConnectionRequest{
		requestID: c.nextRequestID.Add(1),
		key:       key,
		sequence:  sequence,
		ttlMS:     ttlMS,
		clientID:  c.clientID,
		bootID:    c.bootID,
		operation: redleasev1.ClientOperationRENEW,
	}
}
