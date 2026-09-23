package client

import (
	"context"
	"errors"
	"slices"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
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

type acquireSubmission struct {
	replica int
	future  *connectionFuture
	err     error
}

type acquireReplicaResult struct {
	replica  int
	response protocol.Response
	err      error
}

// Acquire makes one attempt to establish a currently valid lease quorum for
// the application-defined uint64 key. The caller owns retry policy; every new
// call uses a new lease ID.
func (c *Client) Acquire(
	ctx context.Context,
	key uint64,
	ttlMS uint64,
) (*Lease, error) {
	if c.ctx.Err() != nil {
		return nil, &notAcquiredError{cause: ErrClientClosed}
	}
	sequence := c.nextSequence.Add(1)
	lease := newLease(c, sequence, key, ttlMS)
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
		request := c.newAcquireRequest(lease.key, sequence, ttlMS)
		//nolint:contextcheck // Submission and response collection intentionally have different lifetimes.
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
	// attempt has crossed (or definitively failed before) its send-queue barrier.
	for range serverCount {
		<-submissions
	}

	if err := c.acquireCancellationError(ctx); err != nil {
		cancelCollection()
		lease.cancel()
		c.cleanupFailedAcquire(lease.key, sequence) //nolint:contextcheck // Cleanup must outlive caller cancellation.
		return nil, &notAcquiredError{cause: err}
	}

	var (
		candidates   = make([]time.Time, serverCount)
		successful   = make([]bool, serverCount)
		firstFailure error
		keyLimitSeen bool
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
			} else if isSuccessfulAcquire(result.response.Status) {
				now := time.Now()
				successful[result.replica] = true
				candidates[result.replica] = candidateValidUntil(
					operationStart,
					result.response.TTLMS,
				)
				if now.Before(candidates[result.replica]) {
					lease.markConfirmed(result.replica, candidates[result.replica])
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
					//nolint:contextcheck // Remaining responses belong to the lease lifecycle, not the caller context.
					go c.collectRemainingAcquireResults(
						cancelCollection,
						lease,
						operationStart,
						results,
						serverCount-received,
					)
					return lease, nil
				}
			} else {
				switch result.response.Status {
				case protocol.StatusKeyLimitReached:
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

func (c *Client) submitAcquire(
	submitContext context.Context,
	collectionContext context.Context,
	replica int,
	request *outboundConnectionRequest,
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
	if response.Operation != protocol.OperationAcquire {
		results <- acquireReplicaResult{
			replica: replica,
			err:     errors.New("Acquire received a non-Acquire response"),
		}
		return
	}
	results <- acquireReplicaResult{replica: replica, response: response}
}

func (c *Client) collectRemainingAcquireResults(
	cancelCollection context.CancelFunc,
	lease *Lease,
	operationStart time.Time,
	results <-chan acquireReplicaResult,
	remaining int,
) {
	for range remaining {
		result := <-results
		if result.err == nil &&
			isSuccessfulAcquire(result.response.Status) {
			candidate := candidateValidUntil(
				operationStart,
				result.response.TTLMS,
			)
			if time.Now().Before(candidate) {
				lease.markConfirmed(result.replica, candidate)
			}
		}
	}
	cancelCollection()
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
