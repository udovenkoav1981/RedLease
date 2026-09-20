package client

import (
	"context"
	"errors"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
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

type renewReplicaResult struct {
	replica  int
	response protocol.Response
	err      error
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

	operationContext, cancelOperation := context.WithTimeout(l.ctx, l.client.responseTimeout)
	stopCallerCancellation := context.AfterFunc(ctx, cancelOperation)
	defer func() {
		stopCallerCancellation()
		cancelOperation()
	}()

	collectionContext, cancelCollection := context.WithCancel(l.ctx)
	submissions := make(chan acquireSubmission, serverCount)
	results := make(chan renewReplicaResult, serverCount)

	for replica := range l.client.replicas {
		request := l.client.newRenewRequest(l.key, l.sequence, ttlMS)
		//nolint:contextcheck // Submission and response collection intentionally have different lifetimes.
		go l.submitRenew(
			operationContext,
			collectionContext,
			replica,
			request,
			submissions,
			results,
		)
	}

	for range serverCount {
		<-submissions
	}
	l.endSubmitBatch()
	batchActive = false

	if err := l.renewCancellationError(ctx); err != nil {
		go l.collectRemainingRenewResults(
			cancelCollection,
			operationStart,
			results,
			serverCount,
		)
		return &notRenewedError{cause: err}
	}

	var (
		candidates   = make([]time.Time, serverCount)
		successful   = make([]bool, serverCount)
		firstFailure error
		received     int
	)

	collecting := true
	for received < serverCount && collecting {
		select {
		case result := <-results:
			received++
			if result.err != nil {
				l.clearConfirmed(result.replica)
				if firstFailure == nil {
					firstFailure = result.err
				}
			} else if result.response.Status != protocol.StatusOK {
				l.clearConfirmed(result.replica)
			} else {
				now := time.Now()
				successful[result.replica] = true
				candidates[result.replica] = candidateValidUntil(
					operationStart,
					result.response.TTLMS,
				)
				if now.Before(candidates[result.replica]) {
					l.markConfirmed(result.replica, candidates[result.replica])
				} else {
					l.clearConfirmed(result.replica)
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
						cancelCollection()
						return &notRenewedError{cause: ErrLeaseReleased}
					}

					go l.collectRemainingRenewResults(
						cancelCollection,
						operationStart,
						results,
						serverCount-received,
					)
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

		case <-ctx.Done():
			firstFailure = ctx.Err()
			collecting = false
		case <-l.ctx.Done():
			firstFailure = l.renewCancellationError(ctx)
			collecting = false
		}
	}

	if remaining := serverCount - received; remaining > 0 {
		go l.collectRemainingRenewResults(
			cancelCollection,
			operationStart,
			results,
			remaining,
		)
	} else {
		cancelCollection()
	}
	return &notRenewedError{cause: firstFailure}
}

func (l *Lease) submitRenew(
	submitContext context.Context,
	collectionContext context.Context,
	replica int,
	request protocol.Request,
	submissions chan<- acquireSubmission,
	results chan<- renewReplicaResult,
) {
	future, err := l.client.replicas[replica].submit(submitContext, request)
	submissions <- acquireSubmission{replica: replica, future: future, err: err}
	if err != nil {
		results <- renewReplicaResult{replica: replica, err: err}
		return
	}

	responseContext, cancelResponse := context.WithTimeout(collectionContext, l.client.responseTimeout)
	response, err := future.await(responseContext)
	cancelResponse()
	if err != nil {
		results <- renewReplicaResult{replica: replica, err: err}
		return
	}
	if response.Operation != protocol.OperationRenew {
		results <- renewReplicaResult{
			replica: replica,
			err:     errors.New("Renew received a non-Renew response"),
		}
		return
	}
	results <- renewReplicaResult{replica: replica, response: response}
}

func (l *Lease) collectRemainingRenewResults(
	cancelCollection context.CancelFunc,
	operationStart time.Time,
	results <-chan renewReplicaResult,
	remaining int,
) {
	defer cancelCollection()
	for range remaining {
		result := <-results
		if result.err != nil ||
			result.response.Status != protocol.StatusOK {
			l.clearConfirmed(result.replica)
			continue
		}

		candidate := candidateValidUntil(
			operationStart,
			result.response.TTLMS,
		)
		if time.Now().Before(candidate) {
			l.markConfirmed(result.replica, candidate)
		} else {
			l.clearConfirmed(result.replica)
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

func (c *Client) newRenewRequest(key, sequence, ttlMS uint64) protocol.Request {
	return protocol.Request{
		Operation:      protocol.OperationRenew,
		Key:            key,
		ClientID:       c.clientID,
		BootID:         c.bootID,
		LeaseSequence:  sequence,
		RequestedTTLMS: ttlMS,
	}
}
