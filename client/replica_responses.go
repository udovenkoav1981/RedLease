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

type replicaWait struct {
	requestID      uint64
	responseExpiry time.Duration
}

// replicaResponses joins independently completed replica requests without
// starting one waiting goroutine per replica. Each request retains its own
// response deadline; one timer is reset to the nearest outstanding deadline.
type replicaResponses struct {
	client    *Client
	results   chan replicaResponse
	waits     [maxServerCount]replicaWait
	timeBase  time.Time
	remaining int
	timer     *time.Timer
}

func (c *Client) acquireReplicaResponses() *replicaResponses {
	if pooled := c.replicaResponsesPool.Get(); pooled != nil {
		if responses, ok := pooled.(*replicaResponses); ok {
			responses.client = c
			return responses
		}
	}
	return &replicaResponses{
		client:  c,
		results: make(chan replicaResponse, maxServerCount),
	}
}

func (r *replicaResponses) release() {
	if r.remaining != 0 || len(r.results) != 0 || r.timer != nil {
		panic("client: releasing active replica responses")
	}
	client := r.client
	r.client = nil
	r.waits = [maxServerCount]replicaWait{}
	r.timeBase = time.Time{}
	client.replicaResponsesPool.Put(r)
}

func (r *replicaResponses) submit(
	ctx context.Context,
	replica int,
	request *outboundConnectionRequest,
) error {
	requestID := request.request.RequestId()
	now := time.Now()
	if r.timeBase.IsZero() {
		r.timeBase = now
	}
	responseExpiry := now.Sub(r.timeBase) + r.client.responseTimeout
	if err := r.client.replicas[replica].submitForOperation(ctx, request, r.results); err != nil {
		return err
	}
	r.waits[replica] = replicaWait{
		requestID:      requestID,
		responseExpiry: responseExpiry,
	}
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
	for replica, wait := range r.waits {
		if wait.requestID != 0 {
			r.client.replicas[replica].complete(
				wait.requestID,
				connectionCallResult{err: err},
			)
		}
	}
	for r.remaining != 0 {
		r.consume(<-r.results)
	}
}

func (r *replicaResponses) consume(result replicaResponse) {
	wait := &r.waits[result.replica]
	if wait.requestID != result.requestID {
		panic("client: unexpected replica response")
	}
	*wait = replicaWait{}
	r.remaining--
	if r.remaining == 0 {
		r.stopTimer()
	}
}

func (r *replicaResponses) nextTimeout() <-chan time.Time {
	var next time.Duration
	for _, wait := range r.waits {
		if wait.requestID != 0 && (next == 0 || wait.responseExpiry < next) {
			next = wait.responseExpiry
		}
	}
	if next == 0 {
		return nil
	}

	if r.timer == nil {
		if pooled, ok := r.client.responseTimerPool.Get().(*time.Timer); ok {
			r.timer = pooled
		} else {
			r.timer = time.NewTimer(time.Hour)
		}
	}
	delay := next - time.Since(r.timeBase)
	if delay < 0 {
		delay = 0
	}
	r.timer.Reset(delay)
	return r.timer.C
}

func (r *replicaResponses) expireResponses() bool {
	now := time.Since(r.timeBase)
	expired := false
	for replica, wait := range r.waits {
		if wait.requestID != 0 && now >= wait.responseExpiry {
			expired = true
			r.client.replicas[replica].complete(
				wait.requestID,
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
