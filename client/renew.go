package client

import (
	"bytes"
	"context"
	"errors"

	"github.com/udovenkoav1981/RedLease/internal/boottime"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
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
	response *redleasev1.RenewResponse
	err      error
}

// Renew attempts to extend this lease on a configured quorum. Failure leaves
// the previously confirmed validUntil unchanged.
func (l *Lease) Renew(ctx context.Context, ttl Milliseconds) error {
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
		request := newRenewRequest(l.key, l.id, ttl)
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
		candidates   = make([]uint64, serverCount)
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
			} else if result.response == nil ||
				result.response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
				l.clearConfirmed(result.replica)
			} else {
				now := boottime.Now()
				successful[result.replica] = true
				candidates[result.replica] = candidateValidUntil(
					operationStart,
					Milliseconds(result.response.GetTtlMs()),
				)
				if now < candidates[result.replica] {
					l.markConfirmed(result.replica, candidates[result.replica])
				} else {
					l.clearConfirmed(result.replica)
				}

				quorumValidUntil, hasQuorum := bestAcquireQuorum(
					candidates,
					successful,
					quorumSize,
				)
				if hasQuorum && now < quorumValidUntil {
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
				boottime.Now(),
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
	request *redleasev1.ClientRequest,
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
	renewResponse := response.GetRenew()
	if renewResponse == nil {
		results <- renewReplicaResult{
			replica: replica,
			err:     errors.New("Renew received a non-Renew response"),
		}
		return
	}
	results <- renewReplicaResult{replica: replica, response: renewResponse}
}

func (l *Lease) collectRemainingRenewResults(
	cancelCollection context.CancelFunc,
	operationStart uint64,
	results <-chan renewReplicaResult,
	remaining int,
) {
	defer cancelCollection()
	for range remaining {
		result := <-results
		if result.err != nil ||
			result.response == nil ||
			result.response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
			l.clearConfirmed(result.replica)
			continue
		}

		candidate := candidateValidUntil(
			operationStart,
			Milliseconds(result.response.GetTtlMs()),
		)
		if boottime.Now() < candidate {
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

func newRenewRequest(key []byte, id leaseID, ttl Milliseconds) *redleasev1.ClientRequest {
	return &redleasev1.ClientRequest{
		Operation: &redleasev1.ClientRequest_Renew{
			Renew: &redleasev1.RenewRequest{
				Key:            bytes.Clone(key),
				LeaseId:        id.protobuf(),
				RequestedTtlMs: uint64(ttl),
			},
		},
	}
}
