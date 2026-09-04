package client

import (
	"context"
	"time"
)

// backgroundHeal keeps trying to place this lease on every replica while the
// locally confirmed lease remains valid. It never changes validUntil.
func (l *Lease) backgroundHeal() {
	backoff := defaultReconnectBackoff()
	timer := systemBackoffTimer{}
	var attempt uint

	for {
		_, active := l.healingTargets()
		if !active {
			return
		}

		if !timer.wait(l.ctx, backoff.duration(attempt)) {
			return
		}

		targets, active := l.healingTargets()
		if !active {
			return
		}
		if len(targets) != 0 && l.healReplicas(targets) != 0 {
			attempt = 0
		} else {
			attempt++
		}
	}
}

func (l *Lease) healingTargets() ([]int, bool) {
	now := time.Now().Round(0)
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()

	if l.lifecycle != leaseActive || !now.Before(l.validUntil) {
		return nil, false
	}

	targets := make([]int, 0, ServerCount)
	for replica, confirmedUntil := range l.confirmedUntil {
		if !now.Before(confirmedUntil) {
			targets = append(targets, replica)
		}
	}
	return targets, true
}

func (l *Lease) healReplicas(replicas []int) int {
	operationStart, started := l.beginHealingBatch()
	if !started {
		return 0
	}
	batchActive := true
	defer func() {
		if batchActive {
			l.endSubmitBatch()
		}
	}()

	submitContext, cancelSubmissions := context.WithTimeout(l.ctx, l.client.responseTimeout)
	results := make(chan acquireReplicaResult, len(replicas))
	submissions := make(chan acquireSubmission, len(replicas))

	for _, replica := range replicas {
		request := newAcquireRequest(l.key, l.id, l.requestedTTL)
		go l.client.submitAcquire(
			submitContext,
			l.ctx,
			replica,
			request,
			submissions,
			results,
		)
	}

	for range replicas {
		<-submissions
	}
	cancelSubmissions()
	l.endSubmitBatch()
	batchActive = false

	confirmed := 0
	for range replicas {
		result := <-results
		if result.err != nil ||
			result.response == nil ||
			!isSuccessfulAcquire(result.response.GetStatus()) {
			continue
		}

		candidate := candidateValidUntil(
			operationStart,
			Milliseconds(result.response.GetTtlMs()),
		)
		if time.Now().Round(0).Before(candidate) {
			l.markConfirmed(result.replica, candidate)
			confirmed++
		}
	}
	return confirmed
}
