package client

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/backoff"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

type releaseSubmission struct {
	replica int
	future  *connectionFuture
}

type releaseRetries struct {
	sync.WaitGroup

	failed atomic.Uint64
}

// Release immediately makes the local lease invalid and asynchronously sends
// an idempotent best-effort Release to all configured replicas. Repeated calls
// do nothing.
func (l *Lease) Release() {
	l.releaseOnce.Do(func() {
		l.startRelease()
		go l.finishRelease()
	})
}

// releaseAll waits for one submission attempt on every replica, then leaves
// response handling and bounded retries in the background.
func (c *Client) releaseAll(key, sequence uint64) {
	serverCount := len(c.replicas)
	retryTimeout := time.Duration(protocol.MaxTTLMS)*time.Millisecond + c.responseTimeout
	retryContext, cancelRetries := context.WithTimeout(c.ctx, retryTimeout)
	initialContext, cancelInitial := context.WithTimeout(retryContext, c.responseTimeout)

	submissions := make(chan releaseSubmission, serverCount)
	for replica := range c.replicas {
		request := c.newReleaseRequest(key, sequence)
		go func() {
			future, _ := c.replicas[replica].submit(initialContext, request)
			submissions <- releaseSubmission{replica: replica, future: future}
		}()
	}

	initial := make([]releaseSubmission, 0, serverCount)
	for range serverCount {
		initial = append(initial, <-submissions)
	}
	cancelInitial()

	var retries releaseRetries
	retries.Add(serverCount)
	for _, submission := range initial {
		go func() {
			defer retries.Done()
			if c.retryReleaseReplica(
				retryContext,
				submission.replica,
				key,
				sequence,
				submission.future,
			) {
				retries.failed.Or(uint64(1) << uint(submission.replica))
			}
		}()
	}
	go func() {
		retries.Wait()
		cancelRetries()
		failed := retries.failed.Load()
		if failed != 0 && c.ctx.Err() == nil {
			c.logger.Warn(
				"release cleanup did not complete before retry deadline",
				slog.String("operation", "release"),
				slog.Uint64("key", key),
				slog.Uint64("lease_client_id", uint64(c.clientID)),
				slog.Uint64("lease_boot_id", uint64(c.bootID)),
				slog.Uint64("lease_sequence", sequence),
				slog.Any("replica_indices", replicaIndices(failed, serverCount)),
			)
		}
	}()
}

func replicaIndices(mask uint64, serverCount int) []int {
	indices := make([]int, 0, serverCount)
	for replica := range serverCount {
		if mask&(uint64(1)<<uint(replica)) != 0 {
			indices = append(indices, replica)
		}
	}
	return indices
}

func (c *Client) retryReleaseReplica(
	ctx context.Context,
	replica int,
	key uint64,
	sequence uint64,
	future *connectionFuture,
) bool {
	retryBackoff := backoff.Default()
	var attempt uint

	for {
		if future != nil && c.releaseResponseOK(ctx, future) {
			return false
		}
		if !backoff.Wait(ctx, retryBackoff.Duration(attempt)) {
			return ctx.Err() == context.DeadlineExceeded
		}
		attempt++

		submitContext, cancelSubmit := context.WithTimeout(ctx, c.responseTimeout)
		future, _ = c.replicas[replica].submit(submitContext, c.newReleaseRequest(key, sequence))
		cancelSubmit()
	}
}

func (c *Client) releaseResponseOK(ctx context.Context, future *connectionFuture) bool {
	responseContext, cancelResponse := context.WithTimeout(ctx, c.responseTimeout)
	defer cancelResponse()

	response, err := future.await(responseContext)
	if err != nil || response.Operation != redleasev1.ClientOperationRELEASE {
		return false
	}
	status := response.Status
	// A quarantined process has empty RAM state and rejected this Release
	// without applying any lease mutation. There is nothing from the previous
	// process incarnation left to clean on that replica.
	return status == redleasev1.LeaseStatusOK || status == redleasev1.LeaseStatusNOT_READY
}
