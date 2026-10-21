package client

import (
	"sync"
	"time"
)

type leaseLifecycle uint8

const (
	leaseActive leaseLifecycle = iota
	leaseReleased
)

// Lease is a locally confirmed distributed lease. Its validity is based only
// on the quorum selected by Acquire; later replica responses never extend it.
// Its public methods must not be called concurrently on the same Lease.
type Lease struct {
	client         *Client
	sequence       uint64
	key            uint64
	requestedTTLMS uint64
	now            time.Time

	stateMu        sync.RWMutex
	lifecycle      leaseLifecycle
	validUntil     time.Time
	confirmedUntil []time.Time
	healing        bool
	healingBatches sync.WaitGroup

	releaseDone chan struct{}
}

func newLease(
	client *Client,
	sequence uint64,
	key uint64,
	requestedTTLMS uint64,
	now time.Time,
) *Lease {
	return &Lease{
		client:         client,
		sequence:       sequence,
		key:            key,
		requestedTTLMS: requestedTTLMS,
		now:            now,
		confirmedUntil: make([]time.Time, len(client.replicas)),
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
	if l.lifecycle != leaseActive {
		return 0
	}
	remaining := time.Until(l.validUntil).Milliseconds()
	if remaining <= 0 {
		return 0
	}
	return uint64(remaining)
}

func (l *Lease) setAcquireValidity(validUntil time.Time) {
	l.validUntil = validUntil
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

func (l *Lease) extendValidity(validUntil time.Time) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if validUntil.After(l.validUntil) {
		l.validUntil = validUntil
	}
}
