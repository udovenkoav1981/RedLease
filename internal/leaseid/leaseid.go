// Package leaseid generates process-unique lease identifiers shared by the
// RedLease client implementations.
package leaseid

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

// LeaseID identifies one lease attempt created by a client process.
type LeaseID struct {
	ClientID uint32
	BootID   uint32
	Sequence uint64
}

// Protobuf converts id to its wire representation.
func (id LeaseID) Protobuf() *redleasev1.LeaseID {
	return &redleasev1.LeaseID{
		ClientId: id.ClientID,
		BootId:   id.BootID,
		LeaseSeq: id.Sequence,
	}
}

// Generator generates lease IDs for one client process.
type Generator struct {
	clientID     uint32
	bootID       uint32
	nextSequence atomic.Uint64
}

// NewGenerator constructs a generator with a cryptographically random boot ID.
func NewGenerator(clientID uint32) (*Generator, error) {
	var bootIDBytes [4]byte
	if _, err := rand.Read(bootIDBytes[:]); err != nil {
		return nil, fmt.Errorf("generate boot ID: %w", err)
	}

	return &Generator{
		clientID: clientID,
		bootID:   binary.BigEndian.Uint32(bootIDBytes[:]),
	}, nil
}

// Next returns the next lease ID. The sequence starts at one.
func (g *Generator) Next() LeaseID {
	return LeaseID{
		ClientID: g.clientID,
		BootID:   g.bootID,
		Sequence: g.nextSequence.Add(1),
	}
}
