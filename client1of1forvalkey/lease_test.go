package client1of1forvalkey

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

func TestCandidateValidityRejectsTTLAboveProtocolMaximum(t *testing.T) {
	start := time.Now()
	validUntil := candidateValidUntil(start, protocol.MaxTTLMS+1)
	if !validUntil.Equal(start) {
		t.Fatalf("validUntil = %v, want expired at %v", validUntil, start)
	}
}

func TestValkeyLeaseKeyPreservesUint64(t *testing.T) {
	const key uint64 = 0xfedcba9876543210
	encoded := valkeyLeaseKey(key)
	if len(encoded) != 17 {
		t.Fatalf("key length = %d, want 17", len(encoded))
	}
	if prefix := encoded[:9]; prefix != "redlease:" {
		t.Fatalf("key prefix = %q, want redlease:", prefix)
	}
	if decoded := binary.BigEndian.Uint64([]byte(encoded[9:])); decoded != key {
		t.Fatalf("decoded key = %x, want %x", decoded, key)
	}
}

func TestOwnershipTokensUseIncreasingSequence(t *testing.T) {
	client := Client{clientID: 7, bootID: 11}
	first := client.newOwnershipToken()
	second := client.newOwnershipToken()
	if len(first) != 16 || len(second) != 16 {
		t.Fatalf("token lengths = %d, %d; want 16", len(first), len(second))
	}
	if first == second {
		t.Fatal("successive ownership tokens are equal")
	}
	if sequence := binary.BigEndian.Uint64([]byte(first[8:])); sequence != 1 {
		t.Fatalf("first sequence = %d, want 1", sequence)
	}
	if sequence := binary.BigEndian.Uint64([]byte(second[8:])); sequence != 2 {
		t.Fatalf("second sequence = %d, want 2", sequence)
	}
}
