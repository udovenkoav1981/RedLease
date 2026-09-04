package client

import (
	"bytes"
	"context"
	"sync"
	"time"
)

// LeaseID identifies one lease attempt created by a RedLease client process.
type LeaseID struct {
	ClientID uint32
	BootID   uint32
	LeaseSeq uint64
}

type leaseLifecycle uint8

const (
	leaseActive leaseLifecycle = iota
	leaseReleasing
	leaseReleased
)

// Lease is a locally confirmed distributed lease. Its validity is based only
// on the quorum selected by Acquire; later replica responses never extend it.
type Lease struct {
	client       *Client
	id           leaseID
	key          []byte
	requestedTTL Milliseconds
	now          func() time.Time
	ctx          context.Context
	cancel       context.CancelFunc

	stateMu        sync.RWMutex
	lifecycle      leaseLifecycle
	validUntil     time.Time
	confirmedUntil [ServerCount]time.Time
	submitBatches  sync.WaitGroup

	renewMu sync.Mutex

	releaseOnce sync.Once
	releaseDone chan struct{}
}

func newLease(client *Client, id leaseID, key []byte, requestedTTL Milliseconds) *Lease {
	ctx, cancel := context.WithCancel(client.ctx)
	return &Lease{
		client:       client,
		id:           id,
		key:          bytes.Clone(key),
		requestedTTL: requestedTTL,
		now:          client.now,
		ctx:          ctx,
		cancel:       cancel,
		lifecycle:    leaseActive,
		releaseDone:  make(chan struct{}),
	}
}

// ID returns the immutable identity assigned to this lease attempt.
func (l *Lease) ID() LeaseID {
	return LeaseID{
		ClientID: l.id.clientID,
		BootID:   l.id.bootID,
		LeaseSeq: l.id.sequence,
	}
}

// Key returns a copy of the lease key.
func (l *Lease) Key() []byte {
	return bytes.Clone(l.key)
}

// ValidUntil returns the current local wall-clock validity bound.
func (l *Lease) ValidUntil() time.Time {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	return l.validUntil
}

// Valid reports whether a new protected operation may start now.
func (l *Lease) Valid() bool {
	l.stateMu.RLock()
	validUntil := l.validUntil
	active := l.lifecycle == leaseActive
	l.stateMu.RUnlock()
	return active && l.now().Round(0).Before(validUntil)
}

func (l *Lease) setAcquireValidity(validUntil time.Time) {
	l.stateMu.Lock()
	if l.lifecycle == leaseActive {
		l.validUntil = validUntil.Round(0)
	}
	l.stateMu.Unlock()
}

func (l *Lease) markConfirmed(replica int, confirmedUntil time.Time) {
	l.stateMu.Lock()
	if l.lifecycle == leaseActive &&
		l.now().Round(0).Before(confirmedUntil) &&
		confirmedUntil.After(l.confirmedUntil[replica]) {
		l.confirmedUntil[replica] = confirmedUntil.Round(0)
	}
	l.stateMu.Unlock()
}

func (l *Lease) confirmedReplicas() [ServerCount]bool {
	now := l.now().Round(0)
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()

	var confirmed [ServerCount]bool
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

func (l *Lease) beginSubmitBatch() bool {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.lifecycle != leaseActive {
		return false
	}
	l.submitBatches.Add(1)
	return true
}

func (l *Lease) beginHealingBatch() bool {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.lifecycle != leaseActive || !l.now().Round(0).Before(l.validUntil) {
		return false
	}
	l.submitBatches.Add(1)
	return true
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
		l.validUntil = validUntil.Round(0)
	}
	return true
}

func (l *Lease) startRelease() {
	l.stateMu.Lock()
	l.lifecycle = leaseReleasing
	l.validUntil = time.Time{}
	l.confirmedUntil = [ServerCount]time.Time{}
	l.stateMu.Unlock()
	l.cancel()
}

func (l *Lease) finishRelease() {
	l.submitBatches.Wait()
	l.client.releaseAll(l.key, l.id)

	l.stateMu.Lock()
	l.lifecycle = leaseReleased
	l.stateMu.Unlock()
	close(l.releaseDone)
}
