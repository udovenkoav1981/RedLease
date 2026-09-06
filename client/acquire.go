package client

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/udovenkoav1981/RedLease/internal/boottime"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

// ErrNotAcquired identifies every Acquire result which did not establish a
// currently valid configured quorum.
var ErrNotAcquired = errors.New("RedLease lease not acquired")

var (
	// ErrKeyLimitReached means at least one server reported that its resident
	// lease-key limit was reached during an unsuccessful Acquire.
	ErrKeyLimitReached = errors.New("RedLease server key limit reached")

	// ErrKeyTooLarge means the supplied key exceeds protocol.MaxKeyBytes.
	ErrKeyTooLarge = errors.New("RedLease key is too large")
)

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

// Acquire makes one attempt to establish a currently valid lease quorum. The
// caller owns retry policy; every new call uses a new lease ID.
func (c *Client) Acquire(
	ctx context.Context,
	key []byte,
	ttl Milliseconds,
) (*Lease, error) {
	if c.ctx.Err() != nil {
		return nil, &notAcquiredError{cause: ErrClientClosed}
	}
	if len(key) > protocol.MaxKeyBytes {
		return nil, &notAcquiredError{cause: ErrKeyTooLarge}
	}

	id := c.idGenerator.next()
	lease := newLease(c, id, key, ttl)
	operationStart := lease.now
	serverCount := len(c.replicas)
	quorumSize := c.quorum.size()

	operationContext, cancelOperation := context.WithTimeout(c.ctx, c.responseTimeout)
	stopCallerCancellation := context.AfterFunc(ctx, cancelOperation)
	defer func() {
		stopCallerCancellation()
		cancelOperation()
	}()

	collectionContext, cancelCollection := context.WithCancel(lease.ctx)
	submissions := make(chan acquireSubmission, serverCount)
	results := make(chan acquireReplicaResult, serverCount)

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
	for range serverCount {
		<-submissions
	}

	if err := c.acquireCancellationError(ctx); err != nil {
		cancelCollection()
		lease.cancel()
		c.cleanupFailedAcquire(lease.key, id)
		return nil, &notAcquiredError{cause: err}
	}

	var (
		candidates   = make([]uint64, serverCount)
		successful   = make([]bool, serverCount)
		firstFailure error
		keyLimitSeen bool
		largeKeySeen bool
		received     int
	)

	for received < serverCount {
		select {
		case result := <-results:
			received++
			if result.err != nil {
				if firstFailure == nil {
					firstFailure = result.err
				}
			} else if result.response != nil && isSuccessfulAcquire(result.response.GetStatus()) {
				now := boottime.Now()
				successful[result.replica] = true
				candidates[result.replica] = candidateValidUntil(
					operationStart,
					Milliseconds(result.response.GetTtlMs()),
				)
				if now < candidates[result.replica] {
					lease.markConfirmed(result.replica, candidates[result.replica])
				}

				validUntil, hasQuorum := bestAcquireQuorum(
					candidates,
					successful,
					quorumSize,
				)
				if hasQuorum && now < validUntil {
					if err := c.acquireCancellationError(ctx); err != nil {
						firstFailure = err
						received = serverCount
						break
					}

					lease.setAcquireValidity(validUntil)
					go c.collectRemainingAcquireResults(
						cancelCollection,
						lease,
						operationStart,
						results,
						serverCount-received,
					)
					return lease, nil
				}
			} else if result.response != nil {
				switch result.response.GetStatus() {
				case redleasev1.LeaseStatus_LEASE_STATUS_KEY_LIMIT_REACHED:
					keyLimitSeen = true
				case redleasev1.LeaseStatus_LEASE_STATUS_KEY_TOO_LARGE:
					largeKeySeen = true
				}
			}

			if !acquireQuorumStillPossible(
				candidates,
				successful,
				serverCount-received,
				boottime.Now(),
				quorumSize,
			) {
				received = serverCount
			}

		case <-ctx.Done():
			firstFailure = ctx.Err()
			received = serverCount
		case <-c.ctx.Done():
			firstFailure = ErrClientClosed
			received = serverCount
		}
	}

	cancelCollection()
	lease.cancel()
	c.cleanupFailedAcquire(lease.key, id)
	if keyLimitSeen {
		firstFailure = errors.Join(firstFailure, ErrKeyLimitReached)
	}
	if largeKeySeen {
		firstFailure = errors.Join(firstFailure, ErrKeyTooLarge)
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
	operationStart uint64,
	results <-chan acquireReplicaResult,
	remaining int,
) {
	for range remaining {
		result := <-results
		if result.err == nil &&
			result.response != nil &&
			isSuccessfulAcquire(result.response.GetStatus()) {
			candidate := candidateValidUntil(
				operationStart,
				Milliseconds(result.response.GetTtlMs()),
			)
			if boottime.Now() < candidate {
				lease.markConfirmed(result.replica, candidate)
			}
		}
	}
	cancelCollection()
	lease.backgroundHeal()
}

func acquireQuorumStillPossible(
	candidates []uint64,
	successful []bool,
	remaining int,
	now uint64,
	quorumSize int,
) bool {
	usable := 0
	for replica, success := range successful {
		if success && now < candidates[replica] {
			usable++
		}
	}
	return usable+remaining >= quorumSize
}

func (c *Client) cleanupFailedAcquire(key []byte, id leaseID) {
	c.releaseAll(key, id)
}

func bestAcquireQuorum(
	candidates []uint64,
	successful []bool,
	quorumSize int,
) (uint64, bool) {
	validities := make([]uint64, 0, len(candidates))
	for replica, success := range successful {
		if success {
			validities = append(validities, candidates[replica])
		}
	}
	if len(validities) < quorumSize {
		return 0, false
	}

	sort.Slice(validities, func(i, j int) bool {
		return validities[i] < validities[j]
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
