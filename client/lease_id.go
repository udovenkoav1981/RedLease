package client

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

type leaseID struct {
	clientID uint32
	bootID   uint32
	sequence uint64
}

func (id leaseID) protobuf() *redleasev1.LeaseID {
	return &redleasev1.LeaseID{
		ClientId: id.clientID,
		BootId:   id.bootID,
		LeaseSeq: id.sequence,
	}
}

type leaseIDGenerator struct {
	clientID     uint32
	bootID       uint32
	nextSequence atomic.Uint64
}

func newLeaseIDGenerator(clientID uint32) (*leaseIDGenerator, error) {
	return newLeaseIDGeneratorFromReader(clientID, rand.Reader)
}

func newLeaseIDGeneratorFromReader(clientID uint32, random io.Reader) (*leaseIDGenerator, error) {
	var bootIDBytes [4]byte
	if _, err := io.ReadFull(random, bootIDBytes[:]); err != nil {
		return nil, fmt.Errorf("generate boot ID: %w", err)
	}

	return &leaseIDGenerator{
		clientID: clientID,
		bootID:   binary.BigEndian.Uint32(bootIDBytes[:]),
	}, nil
}

func (g *leaseIDGenerator) next() leaseID {
	return leaseID{
		clientID: g.clientID,
		bootID:   g.bootID,
		sequence: g.nextSequence.Add(1) - 1,
	}
}
