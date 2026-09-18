package client

import (
	"bytes"
	"context"
	"sync"

	"github.com/udovenkoav1981/RedLease/internal/boottime"
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
	key            []byte
	requestedTTLMS uint64
	now            uint64
	ctx            context.Context //nolint:containedctx // Lease owns healing and cancellation lifecycle.
	cancel         context.CancelFunc

	stateMu        sync.RWMutex
	lifecycle      leaseLifecycle
	validUntil     uint64
	confirmedUntil []uint64
	submitBatches  sync.WaitGroup

	renewMu sync.Mutex

	releaseOnce sync.Once
	releaseDone chan struct{}
}

func newLease(client *Client, sequence uint64, key []byte, requestedTTLMS uint64) *Lease {
	ctx, cancel := context.WithCancel(client.ctx)
	return &Lease{
		client:         client,
		sequence:       sequence,
		key:            bytes.Clone(key),
		requestedTTLMS: requestedTTLMS,
		now:            boottime.Now(),
		confirmedUntil: make([]uint64, len(client.replicas)),
		ctx:            ctx,
		cancel:         cancel,
		lifecycle:      leaseActive,
		releaseDone:    make(chan struct{}),
	}
}

// Key returns a copy of the lease key.
func (l *Lease) Key() []byte {
	return bytes.Clone(l.key)
}

// RemainingTTLms returns the remaining local validity in milliseconds.
func (l *Lease) RemainingTTLms() uint64 {
	l.stateMu.RLock()
	now := boottime.Now()
	validUntil := l.validUntil
	active := l.lifecycle == leaseActive
	l.stateMu.RUnlock()
	if !active {
		return 0
	}
	return boottime.Remaining(validUntil, now)
}

func (l *Lease) setAcquireValidity(validUntil uint64) {
	l.stateMu.Lock()
	if l.lifecycle == leaseActive {
		l.validUntil = validUntil
	}
	l.stateMu.Unlock()
}

func (l *Lease) markConfirmed(replica int, confirmedUntil uint64) {
	l.stateMu.Lock()
	if l.lifecycle == leaseActive &&
		boottime.Now() < confirmedUntil &&
		confirmedUntil > l.confirmedUntil[replica] {
		l.confirmedUntil[replica] = confirmedUntil
	}
	l.stateMu.Unlock()
}

func (l *Lease) confirmedReplicas() []bool {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	now := boottime.Now()

	confirmed := make([]bool, len(l.confirmedUntil))
	if l.lifecycle != leaseActive {
		return confirmed
	}
	for replica, validUntil := range l.confirmedUntil {
		confirmed[replica] = now < validUntil
	}
	return confirmed
}

func (l *Lease) clearConfirmed(replica int) {
	l.stateMu.Lock()
	if l.lifecycle == leaseActive {
		l.confirmedUntil[replica] = 0
	}
	l.stateMu.Unlock()
}

func (l *Lease) beginRenewBatch() (uint64, bool) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.lifecycle != leaseActive {
		return 0, false
	}
	l.now = boottime.Now()
	l.submitBatches.Add(1)
	return l.now, true
}

func (l *Lease) beginHealingBatch() (uint64, bool) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.lifecycle != leaseActive || boottime.Now() >= l.validUntil {
		return 0, false
	}
	l.submitBatches.Add(1)
	return l.now, true
}

func (l *Lease) endSubmitBatch() {
	l.submitBatches.Done()
}

func (l *Lease) applyRenewValidity(validUntil uint64) bool {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.lifecycle != leaseActive {
		return false
	}
	if validUntil > l.validUntil {
		l.validUntil = validUntil
	}
	return true
}

func (l *Lease) startRelease() {
	l.stateMu.Lock()
	l.lifecycle = leaseReleasing
	l.validUntil = 0
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
