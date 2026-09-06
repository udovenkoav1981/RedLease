package client

import (
	"context"
	"sync"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

const protocolMaxTTL = 5 * time.Second

type releaseSubmission struct {
	replica int
	future  *streamFuture
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
func (c *Client) releaseAll(key []byte, id leaseID) {
	serverCount := len(c.replicas)
	retryContext, cancelRetries := context.WithTimeout(c.ctx, releaseRetryWindow(c.responseTimeout))
	initialContext, cancelInitial := context.WithTimeout(retryContext, c.responseTimeout)

	submissions := make(chan releaseSubmission, serverCount)
	for replica := range c.replicas {
		request := newReleaseRequest(key, id)
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

	var retries sync.WaitGroup
	retries.Add(serverCount)
	for _, submission := range initial {
		go func() {
			defer retries.Done()
			c.retryReleaseReplica(
				retryContext,
				submission.replica,
				key,
				id,
				submission.future,
			)
		}()
	}
	go func() {
		retries.Wait()
		cancelRetries()
	}()
}

func (c *Client) retryReleaseReplica(
	ctx context.Context,
	replica int,
	key []byte,
	id leaseID,
	future *streamFuture,
) {
	backoff := defaultReconnectBackoff()
	var attempt uint

	for {
		if future != nil && c.releaseResponseOK(ctx, future) {
			return
		}
		if !waitBackoff(ctx, backoff.duration(attempt)) {
			return
		}
		attempt++

		submitContext, cancelSubmit := context.WithTimeout(ctx, c.responseTimeout)
		future, _ = c.replicas[replica].submit(submitContext, newReleaseRequest(key, id))
		cancelSubmit()
	}
}

func (c *Client) releaseResponseOK(ctx context.Context, future *streamFuture) bool {
	responseContext, cancelResponse := context.WithTimeout(ctx, c.responseTimeout)
	defer cancelResponse()

	response, err := future.await(responseContext)
	if err != nil || response.GetRelease() == nil {
		return false
	}
	status := response.GetRelease().GetStatus()
	// A quarantined process has empty RAM state and rejected this Release
	// without applying any lease mutation. There is nothing from the previous
	// process incarnation left to clean on that replica.
	return status == redleasev1.LeaseStatus_LEASE_STATUS_OK ||
		status == redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY ||
		status == redleasev1.LeaseStatus_LEASE_STATUS_KEY_TOO_LARGE
}

func releaseRetryWindow(responseTimeout time.Duration) time.Duration {
	if responseTimeout > time.Duration(1<<63-1)-protocolMaxTTL {
		return time.Duration(1<<63 - 1)
	}
	return protocolMaxTTL + responseTimeout
}
