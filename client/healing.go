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
	defer l.finishBackgroundHealing()

	retryBackoff := backoff.Default()
	var attempt uint

	for {
		_, needed := l.healingTargets()
		if !needed {
			return
		}

		if !backoff.Wait(l.ctx, retryBackoff.Duration(attempt)) {
			return
		}

		targets, needed := l.healingTargets()
		if !needed {
			return
		}
		if len(targets) != 0 && l.healReplicas(targets) != 0 {
			attempt = 0
		} else {
			attempt++
		}
	}
}

func (l *Lease) startBackgroundHealing() {
	l.stateMu.Lock()
	if l.healing || !l.backgroundHealingNeededLocked(time.Now()) {
		l.stateMu.Unlock()
		return
	}
	l.healing = true
	l.stateMu.Unlock()
	go l.backgroundHeal()
}

func (l *Lease) finishBackgroundHealing() {
	l.stateMu.Lock()
	l.healing = false
	restart := l.backgroundHealingNeededLocked(time.Now())
	if restart {
		l.healing = true
	}
	l.stateMu.Unlock()
	if restart {
		go l.backgroundHeal()
	}
}

func (l *Lease) backgroundHealingNeededLocked(now time.Time) bool {
	if l.lifecycle != leaseActive || l.ctx.Err() != nil || !now.Before(l.validUntil) {
		return false
	}
	for _, confirmedUntil := range l.confirmedUntil {
		if confirmedUntil.Before(l.validUntil) {
			return true
		}
	}
	return false
}

func (l *Lease) healingTargets() ([]int, bool) {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	now := time.Now()

	if !l.backgroundHealingNeededLocked(now) {
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

	responses := l.client.acquireReplicaResponses()
	defer responses.release()

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
