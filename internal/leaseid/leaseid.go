// Package leaseid provides lease identifiers and random boot IDs shared by the
// RedLease client implementations.
package leaseid

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

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

// NewBootID generates a cryptographically random boot ID for one client process.
func NewBootID() (uint32, error) {
	var bootIDBytes [4]byte
	if _, err := rand.Read(bootIDBytes[:]); err != nil {
		return 0, fmt.Errorf("generate boot ID: %w", err)
	}

	return binary.BigEndian.Uint32(bootIDBytes[:]), nil
}
