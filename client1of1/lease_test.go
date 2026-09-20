package client1of1

import (
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
