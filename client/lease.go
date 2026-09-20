package client

import (
	"context"
	"sync"
	"time"
)

type leaseLifecycle uint8

const (
	leaseActive leaseLifecycle = iota
	leaseReleasing
	leaseReleased
)

// Lease is a locally confirmed distributed lease. Its validity is based only
// on the quorum selected by Acquire; later replica responses never extend it.
type Lease struct {
	client         *Client
	sequence       uint64
	key            uint64
	requestedTTLMS uint64
	now            time.Time
	ctx            context.Context //nolint:containedctx // Lease owns healing and cancellation lifecycle.
	cancel         context.CancelFunc

	stateMu        sync.RWMutex
	lifecycle      leaseLifecycle
	validUntil     time.Time
	confirmedUntil []time.Time
	submitBatches  sync.WaitGroup

	renewMu sync.Mutex

	releaseOnce sync.Once
	releaseDone chan struct{}
}

func newLease(client *Client, sequence, key, requestedTTLMS uint64) *Lease {
	ctx, cancel := context.WithCancel(client.ctx)
	return &Lease{
		client:         client,
		sequence:       sequence,
		key:            key,
		requestedTTLMS: requestedTTLMS,
		now:            time.Now(),
		confirmedUntil: make([]time.Time, len(client.replicas)),
		ctx:            ctx,
		cancel:         cancel,
		lifecycle:      leaseActive,
		releaseDone:    make(chan struct{}),
	}
}

// Key returns the lease key.
func (l *Lease) Key() uint64 {
	return l.key
}

// RemainingTTLms returns the remaining local validity in milliseconds.
func (l *Lease) RemainingTTLms() uint64 {
	l.stateMu.RLock()
	validUntil := l.validUntil
	active := l.lifecycle == leaseActive
	l.stateMu.RUnlock()
	if !active {
		return 0
	}
	remaining := time.Until(validUntil).Milliseconds()
	if remaining <= 0 {
		return 0
	}
	return uint64(remaining)
}

func (l *Lease) setAcquireValidity(validUntil time.Time) {
	l.stateMu.Lock()
	if l.lifecycle == leaseActive {
		l.validUntil = validUntil
	}
	l.stateMu.Unlock()
}

func (l *Lease) markConfirmed(replica int, confirmedUntil time.Time) {
	l.stateMu.Lock()
	if l.lifecycle == leaseActive &&
		time.Now().Before(confirmedUntil) &&
		confirmedUntil.After(l.confirmedUntil[replica]) {
		l.confirmedUntil[replica] = confirmedUntil
	}
	l.stateMu.Unlock()
}

func (l *Lease) confirmedReplicas() []bool {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	now := time.Now()

	confirmed := make([]bool, len(l.confirmedUntil))
	if l.lifecycle != leaseActive {
		return confirmed
	}
	for replica, validUntil := range l.confirmedUntil {
		confirmed[replica] = now.Before(validUntil)
	}
	return confirmed
}

func (l *Lease) clearConfirmed(replica int) {
	l.stateMu.Lock()
	if l.lifecycle == leaseActive {
		l.confirmedUntil[replica] = time.Time{}
	}
	l.stateMu.Unlock()
}

func (l *Lease) beginRenewBatch() (time.Time, bool) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.lifecycle != leaseActive {
		return time.Time{}, false
	}
	l.now = time.Now()
	l.submitBatches.Add(1)
	return l.now, true
}

func (l *Lease) beginHealingBatch() (time.Time, bool) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.lifecycle != leaseActive || !time.Now().Before(l.validUntil) {
		return time.Time{}, false
	}
	l.submitBatches.Add(1)
	return l.now, true
}

func (l *Lease) endSubmitBatch() {
	l.submitBatches.Done()
}

func (l *Lease) applyRenewValidity(validUntil time.Time) bool {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.lifecycle != leaseActive {
		return false
	}
	if validUntil.After(l.validUntil) {
		l.validUntil = validUntil
	}
	return true
}

func (l *Lease) startRelease() {
	l.stateMu.Lock()
	l.lifecycle = leaseReleasing
	l.validUntil = time.Time{}
	clear(l.confirmedUntil)
	l.stateMu.Unlock()
	l.cancel()
}

func (l *Lease) finishRelease() {
	l.submitBatches.Wait()
	l.client.releaseAll(l.key, l.sequence)

	l.stateMu.Lock()
	l.lifecycle = leaseReleased
	l.stateMu.Unlock()
	close(l.releaseDone)
}
