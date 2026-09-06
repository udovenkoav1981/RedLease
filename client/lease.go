package client

import (
	"bytes"
	"context"
	"sync"

	"github.com/udovenkoav1981/RedLease/internal/boottime"
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
	now          uint64
	ctx          context.Context
	cancel       context.CancelFunc

	stateMu        sync.RWMutex
	lifecycle      leaseLifecycle
	validUntil     uint64
	confirmedUntil [ServerCount]uint64
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
		now:          boottime.Now(),
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

// RemainingTTL returns the remaining local validity in milliseconds.
func (l *Lease) RemainingTTL() Milliseconds {
	l.stateMu.RLock()
	now := boottime.Now()
	validUntil := l.validUntil
	active := l.lifecycle == leaseActive
	l.stateMu.RUnlock()
	if !active {
		return 0
	}
	return Milliseconds(boottime.Remaining(validUntil, now))
}

// Valid reports whether a new protected operation may start now.
func (l *Lease) Valid() bool {
	return l.RemainingTTL() != 0
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

func (l *Lease) confirmedReplicas() [ServerCount]bool {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	now := boottime.Now()

	var confirmed [ServerCount]bool
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
	l.confirmedUntil = [ServerCount]uint64{}
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
