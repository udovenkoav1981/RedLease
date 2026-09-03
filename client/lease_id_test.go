package client

import (
	"bytes"
	"sync"
	"testing"
)

func TestLeaseIDGeneratorAndProtobufConversion(t *testing.T) {
	generator, err := newLeaseIDGeneratorFromReader(42, bytes.NewReader([]byte{1, 2, 3, 4}))
	if err != nil {
		t.Fatalf("new lease ID generator: %v", err)
	}

	first := generator.next()
	second := generator.next()
	if first.clientID != 42 || first.bootID != 0x01020304 || first.sequence != 0 {
		t.Fatalf("unexpected first lease ID: %+v", first)
	}
	if second.sequence != 1 {
		t.Fatalf("second sequence = %d, want 1", second.sequence)
	}

	protobuf := first.protobuf()
	if protobuf.GetClientId() != first.clientID ||
		protobuf.GetBootId() != first.bootID ||
		protobuf.GetLeaseSeq() != first.sequence {
		t.Fatalf("unexpected protobuf lease ID: %+v", protobuf)
	}
}

func TestLeaseIDGeneratorIsUniqueUnderConcurrency(t *testing.T) {
	generator, err := newLeaseIDGeneratorFromReader(7, bytes.NewReader([]byte{5, 6, 7, 8}))
	if err != nil {
		t.Fatalf("new lease ID generator: %v", err)
	}

	const (
		goroutines = 32
		perRoutine = 500
	)

	ids := make(chan leaseID, goroutines*perRoutine)
	var workers sync.WaitGroup
	workers.Add(goroutines)
	for range goroutines {
		go func() {
			defer workers.Done()
			for range perRoutine {
				ids <- generator.next()
			}
		}()
	}
	workers.Wait()
	close(ids)

	seen := make(map[uint64]struct{}, goroutines*perRoutine)
	for id := range ids {
		if id.clientID != 7 || id.bootID != 0x05060708 {
			t.Fatalf("unexpected stable fields: %+v", id)
		}
		if _, exists := seen[id.sequence]; exists {
			t.Fatalf("duplicate sequence %d", id.sequence)
		}
		seen[id.sequence] = struct{}{}
	}
	if len(seen) != goroutines*perRoutine {
		t.Fatalf("generated %d unique IDs, want %d", len(seen), goroutines*perRoutine)
	}
}
