package client

import (
	"bytes"
	"sync"
	"time"
)

// LeaseID identifies one lease attempt created by a RedLease client process.
type LeaseID struct {
	ClientID uint32
	BootID   uint32
	LeaseSeq uint64
}

// Lease is a locally confirmed distributed lease. Its validity is based only
// on the quorum selected by Acquire; later replica responses never extend it.
type Lease struct {
	client       *Client
	id           leaseID
	key          []byte
	requestedTTL Milliseconds
	wall         wallClock

	stateMu    sync.RWMutex
	validUntil time.Time
	confirmed  [ServerCount]bool
	released   bool
}

func newLease(client *Client, id leaseID, key []byte, requestedTTL Milliseconds) *Lease {
	return &Lease{
		client:       client,
		id:           id,
		key:          bytes.Clone(key),
		requestedTTL: requestedTTL,
		wall:         client.wall,
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
	released := l.released
	l.stateMu.RUnlock()
	return !released && l.wall.now().Before(validUntil)
}

func (l *Lease) setAcquireValidity(validUntil time.Time) {
	l.stateMu.Lock()
	l.validUntil = validUntil.Round(0)
	l.stateMu.Unlock()
}

func (l *Lease) markConfirmed(replica int) {
	l.stateMu.Lock()
	l.confirmed[replica] = true
	l.stateMu.Unlock()
}

func (l *Lease) confirmedReplicas() [ServerCount]bool {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	return l.confirmed
}
