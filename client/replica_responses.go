package client

import (
	"context"
	"time"
)

type responseWaitEvent uint8

const (
	responseReceived responseWaitEvent = iota
	callerCanceled
	lifecycleCanceled
)

// replicaResponses joins the independently completed replica futures without
// starting one waiting goroutine per replica. Each future retains its own
// response deadline; one timer is reset to the nearest outstanding deadline.
type replicaResponses struct {
	client    *Client
	results   chan replicaResponse
	futures   [maxServerCount]*connectionFuture
	remaining int
	timer     *time.Timer
}

func newReplicaResponses(client *Client, capacity int) replicaResponses {
	return replicaResponses{
		client:  client,
		results: make(chan replicaResponse, capacity),
	}
}

func (r *replicaResponses) submit(
	ctx context.Context,
	replica int,
	request *outboundConnectionRequest,
) error {
	future, err := r.client.replicas[replica].submitForOperation(
		ctx,
		request,
		replica,
		r.results,
	)
	if err != nil {
		return err
	}
	r.futures[replica] = future
	r.remaining++
	return nil
}

func (r *replicaResponses) next(
	lifecycle <-chan struct{},
	caller <-chan struct{},
) (replicaResponse, responseWaitEvent) {
	for {
		timeout := r.nextTimeout()
		select {
		case result := <-r.results:
			r.consume(result)
			return result, responseReceived
		case <-timeout:
			if r.expireResponses() {
				result := <-r.results
				r.consume(result)
				return result, responseReceived
			}
		case <-caller:
			return replicaResponse{}, callerCanceled
		case <-lifecycle:
			return replicaResponse{}, lifecycleCanceled
		}
	}
}

func (r *replicaResponses) abort(err error) {
	for _, future := range r.futures {
		if future != nil {
			future.replica.complete(future.requestID, connectionCallResult{err: err})
		}
	}
	for r.remaining != 0 {
		r.consume(<-r.results)
	}
}

func (r *replicaResponses) consume(result replicaResponse) {
	if r.futures[result.replica] != result.future {
		panic("client: unexpected replica response future")
	}
	r.futures[result.replica] = nil
	r.remaining--
	r.client.releaseFuture(result.future)
	if r.remaining == 0 {
		r.stopTimer()
	}
}

func (r *replicaResponses) nextTimeout() <-chan time.Time {
	var next time.Time
	for _, future := range r.futures {
		if future != nil && (next.IsZero() || future.responseExpiry.Before(next)) {
			next = future.responseExpiry
		}
	}
	if next.IsZero() {
		return nil
	}

	if r.timer == nil {
		if pooled, ok := r.client.responseTimerPool.Get().(*time.Timer); ok {
			r.timer = pooled
		} else {
			r.timer = time.NewTimer(time.Hour)
		}
	}
	delay := time.Until(next)
	if delay < 0 {
		delay = 0
	}
	r.timer.Reset(delay)
	return r.timer.C
}

func (r *replicaResponses) expireResponses() bool {
	now := time.Now()
	expired := false
	for _, future := range r.futures {
		if future != nil && !now.Before(future.responseExpiry) {
			expired = true
			future.replica.complete(
				future.requestID,
				connectionCallResult{err: context.DeadlineExceeded},
			)
		}
	}
	return expired
}

func (r *replicaResponses) stopTimer() {
	if r.timer == nil {
		return
	}
	r.timer.Stop()
	r.client.responseTimerPool.Put(r.timer)
	r.timer = nil
}
