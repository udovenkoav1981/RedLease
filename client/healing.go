package client

import (
	"context"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/backoff"
)

// backgroundHeal keeps trying to place this lease on every replica while the
// locally confirmed lease remains valid. It never changes validUntil.
func (l *Lease) backgroundHeal() {
	retryBackoff := backoff.Default()
	var attempt uint

	for {
		_, active := l.healingTargets()
		if !active {
			return
		}

		if !backoff.Wait(l.ctx, retryBackoff.Duration(attempt)) {
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
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	now := time.Now()

	if l.lifecycle != leaseActive || !now.Before(l.validUntil) {
		return nil, false
	}

	targets := make([]int, 0, len(l.confirmedUntil))
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

	responses := newReplicaResponses(l.client, len(replicas))

	for _, replica := range replicas {
		request := l.client.newAcquireRequest(l.key, l.sequence, l.requestedTTLMS)
		_ = responses.submit(l.ctx, replica, request)
	}
	l.endSubmitBatch()
	batchActive = false

	confirmed := 0
	for responses.remaining != 0 {
		completed, event := responses.next(l.ctx.Done(), nil)
		if event == lifecycleCanceled {
			responses.abort(context.Canceled)
			return confirmed
		}
		result := completed.result
		if result.err != nil ||
			result.response.Operation != redleasev1.ClientOperationACQUIRE ||
			!isSuccessfulAcquire(result.response.Status) {
			continue
		}

		candidate := candidateValidUntil(
			operationStart,
			result.response.TTLMS,
		)
		if time.Now().Before(candidate) {
			l.markConfirmed(completed.replica, candidate)
			confirmed++
		}
	}
	return confirmed
}
