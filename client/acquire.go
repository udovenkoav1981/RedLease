package client

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

// ErrNotAcquired identifies every Acquire result which did not establish a
// currently valid 3/5 quorum.
var ErrNotAcquired = errors.New("RedLease lease not acquired")

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

type acquireSubmission struct {
	replica int
	future  *streamFuture
	err     error
}

type acquireReplicaResult struct {
	replica  int
	response *redleasev1.AcquireResponse
	err      error
}

// Acquire attempts to establish a currently valid lease quorum on any three
// of the five configured lock-servers.
func (c *Client) Acquire(
	ctx context.Context,
	key []byte,
	ttl Milliseconds,
) (*Lease, error) {
	if c.ctx.Err() != nil {
		return nil, &notAcquiredError{cause: ErrClientClosed}
	}

	id := c.idGenerator.next()
	lease := newLease(c, id, key, ttl)
	operationStart := c.wall.now()

	operationContext, cancelOperation := context.WithTimeout(c.ctx, c.responseTimeout)
	stopCallerCancellation := context.AfterFunc(ctx, cancelOperation)
	defer func() {
		stopCallerCancellation()
		cancelOperation()
	}()

	collectionContext, cancelCollection := context.WithCancel(c.ctx)
	submissions := make(chan acquireSubmission, ServerCount)
	results := make(chan acquireReplicaResult, ServerCount)

	for replica := range c.replicas {
		request := newAcquireRequest(lease.key, id, ttl)
		go c.submitAcquire(
			operationContext,
			collectionContext,
			replica,
			request,
			submissions,
			results,
		)
	}

	// A Release cleanup may only be submitted after every Acquire submission
	// attempt has crossed (or definitively failed before) its stream barrier.
	for range ServerCount {
		<-submissions
	}

	if err := c.acquireCancellationError(ctx); err != nil {
		cancelCollection()
		c.cleanupFailedAcquire(lease.key, id)
		return nil, &notAcquiredError{cause: err}
	}

	var (
		candidates   [ServerCount]time.Time
		successful   [ServerCount]bool
		firstFailure error
		received     int
	)

	for received < ServerCount {
		select {
		case result := <-results:
			received++
			if result.err != nil {
				if firstFailure == nil {
					firstFailure = result.err
				}
			} else if result.response != nil && isSuccessfulAcquire(result.response.GetStatus()) {
				now := c.wall.now()
				successful[result.replica] = true
				candidates[result.replica] = candidateValidUntil(
					operationStart,
					Milliseconds(result.response.GetTtlMs()),
				)
				if now.Before(candidates[result.replica]) {
					lease.markConfirmed(result.replica)
				}

				validUntil, hasQuorum := bestAcquireQuorum(candidates, successful)
				if hasQuorum && now.Before(validUntil) {
					if err := c.acquireCancellationError(ctx); err != nil {
						firstFailure = err
						received = ServerCount
						break
					}

					lease.setAcquireValidity(validUntil)
					go c.collectRemainingAcquireResults(
						cancelCollection,
						lease,
						operationStart,
						results,
						ServerCount-received,
					)
					return lease, nil
				}
			}

			if !acquireQuorumStillPossible(
				candidates,
				successful,
				ServerCount-received,
				c.wall.now(),
			) {
				received = ServerCount
			}

		case <-ctx.Done():
			firstFailure = ctx.Err()
			received = ServerCount
		case <-c.ctx.Done():
			firstFailure = ErrClientClosed
			received = ServerCount
		}
	}

	cancelCollection()
	c.cleanupFailedAcquire(lease.key, id)
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

func (c *Client) submitAcquire(
	submitContext context.Context,
	collectionContext context.Context,
	replica int,
	request *redleasev1.ClientRequest,
	submissions chan<- acquireSubmission,
	results chan<- acquireReplicaResult,
) {
	future, err := c.replicas[replica].submit(submitContext, request)
	submissions <- acquireSubmission{replica: replica, future: future, err: err}
	if err != nil {
		results <- acquireReplicaResult{replica: replica, err: err}
		return
	}

	responseContext, cancelResponse := context.WithTimeout(collectionContext, c.responseTimeout)
	response, err := future.await(responseContext)
	cancelResponse()
	if err != nil {
		results <- acquireReplicaResult{replica: replica, err: err}
		return
	}
	acquireResponse := response.GetAcquire()
	if acquireResponse == nil {
		results <- acquireReplicaResult{
			replica: replica,
			err:     errors.New("Acquire received a non-Acquire response"),
		}
		return
	}
	results <- acquireReplicaResult{replica: replica, response: acquireResponse}
}

func (c *Client) collectRemainingAcquireResults(
	cancelCollection context.CancelFunc,
	lease *Lease,
	operationStart time.Time,
	results <-chan acquireReplicaResult,
	remaining int,
) {
	defer cancelCollection()
	for range remaining {
		result := <-results
		if result.err == nil &&
			result.response != nil &&
			isSuccessfulAcquire(result.response.GetStatus()) {
			candidate := candidateValidUntil(
				operationStart,
				Milliseconds(result.response.GetTtlMs()),
			)
			if c.wall.now().Before(candidate) {
				lease.markConfirmed(result.replica)
			}
		}
	}
}

func acquireQuorumStillPossible(
	candidates [ServerCount]time.Time,
	successful [ServerCount]bool,
	remaining int,
	now time.Time,
) bool {
	usable := 0
	for replica, success := range successful {
		if success && now.Before(candidates[replica]) {
			usable++
		}
	}
	return usable+remaining >= quorumSize
}

func (c *Client) cleanupFailedAcquire(key []byte, id leaseID) {
	cleanupContext, cancelCleanup := context.WithTimeout(c.ctx, c.responseTimeout)
	defer cancelCleanup()

	submissions := make(chan acquireSubmission, ServerCount)
	for replica := range c.replicas {
		request := newReleaseRequest(key, id)
		go func() {
			future, err := c.replicas[replica].submit(cleanupContext, request)
			submissions <- acquireSubmission{replica: replica, future: future, err: err}
		}()
	}

	for range ServerCount {
		submission := <-submissions
		if submission.err != nil {
			continue
		}
		go c.drainCleanupResponse(submission.future)
	}
}

func (c *Client) drainCleanupResponse(future *streamFuture) {
	ctx, cancel := context.WithTimeout(c.ctx, c.responseTimeout)
	defer cancel()
	_, _ = future.await(ctx)
}

func bestAcquireQuorum(
	candidates [ServerCount]time.Time,
	successful [ServerCount]bool,
) (time.Time, bool) {
	validities := make([]time.Time, 0, ServerCount)
	for replica, success := range successful {
		if success {
			validities = append(validities, candidates[replica])
		}
	}
	if len(validities) < quorumSize {
		return time.Time{}, false
	}

	sort.Slice(validities, func(i, j int) bool {
		return validities[i].Before(validities[j])
	})
	return validities[len(validities)-quorumSize], true
}

func newAcquireRequest(key []byte, id leaseID, ttl Milliseconds) *redleasev1.ClientRequest {
	return &redleasev1.ClientRequest{
		Operation: &redleasev1.ClientRequest_Acquire{
			Acquire: &redleasev1.AcquireRequest{
				Key:            bytes.Clone(key),
				LeaseId:        id.protobuf(),
				RequestedTtlMs: uint64(ttl),
			},
		},
	}
}

func newReleaseRequest(key []byte, id leaseID) *redleasev1.ClientRequest {
	return &redleasev1.ClientRequest{
		Operation: &redleasev1.ClientRequest_Release{
			Release: &redleasev1.ReleaseRequest{
				Key:     bytes.Clone(key),
				LeaseId: id.protobuf(),
			},
		},
	}
}
