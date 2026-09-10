package leaseid

import (
	"sync"
	"testing"
)

func TestGeneratorAndProtobufConversion(t *testing.T) {
	generator, err := NewGenerator(42)
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}

	first := generator.Next()
	second := generator.Next()
	if first.ClientID != 42 || first.Sequence != 1 {
		t.Fatalf("unexpected first lease ID: %+v", first)
	}
	if second.ClientID != first.ClientID || second.BootID != first.BootID || second.Sequence != 2 {
		t.Fatalf("unexpected second lease ID: %+v", second)
	}

	protobuf := first.Protobuf()
	if protobuf.GetClientId() != first.ClientID ||
		protobuf.GetBootId() != first.BootID ||
		protobuf.GetLeaseSeq() != first.Sequence {
		t.Fatalf("unexpected protobuf lease ID: %+v", protobuf)
	}
}

func TestGeneratorIsUniqueUnderConcurrency(t *testing.T) {
	generator, err := NewGenerator(7)
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}

	const (
		goroutines = 32
		perRoutine = 500
	)

	ids := make(chan LeaseID, goroutines*perRoutine)
	var workers sync.WaitGroup
	workers.Add(goroutines)
	for range goroutines {
		go func() {
			defer workers.Done()
			for range perRoutine {
				ids <- generator.Next()
			}
		}()
	}
	workers.Wait()
	close(ids)

	seen := make(map[uint64]struct{}, goroutines*perRoutine)
	for id := range ids {
		if id.ClientID != 7 {
			t.Fatalf("unexpected client ID: %+v", id)
		}
		if _, exists := seen[id.Sequence]; exists {
			t.Fatalf("duplicate sequence %d", id.Sequence)
		}
		seen[id.Sequence] = struct{}{}
	}
	if len(seen) != goroutines*perRoutine {
		t.Fatalf("generated %d unique IDs, want %d", len(seen), goroutines*perRoutine)
	}
}
