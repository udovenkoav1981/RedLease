package client

import (
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

func TestAcquireQuorumZeroTTLIsExpired(t *testing.T) {
	start := time.Unix(1_000, 0)
	validUntil := candidateValidUntil(start, 0)
	if start.Before(validUntil) {
		t.Fatal("zero-TTL quorum is valid")
	}
	if !validUntil.Equal(start) {
		t.Fatalf("validUntil = %v, want %v", validUntil, start)
	}
}

func TestAcquireQuorumRejectsTTLAboveProtocolMaximum(t *testing.T) {
	start := time.Now()
	validUntil := candidateValidUntil(start, protocol.MaxTTLMS+1)
	if !validUntil.Equal(start) {
		t.Fatalf("validUntil = %v, want expired at %v", validUntil, start)
	}
}

func TestAcquireQuorumUsesMinimumHeterogeneousTTL(t *testing.T) {
	start := time.Unix(1_000, 0)
	candidates := []time.Time{
		candidateValidUntil(start, 2_000),
		candidateValidUntil(start, 1_500),
		candidateValidUntil(start, 3_000),
	}
	validUntil, hasQuorum := bestAcquireQuorum(candidates, []bool{true, true, true}, 3)
	if !hasQuorum || !start.Add(200*time.Millisecond).Before(validUntil) {
		t.Fatal("quorum unexpectedly expired")
	}
	want := start.Add(1_400 * time.Millisecond)
	if !validUntil.Equal(want) {
		t.Fatalf("validUntil = %v, want %v", validUntil, want)
	}
}

func TestAcquireQuorumAccountsForElapsedOperationTime(t *testing.T) {
	start := time.Unix(1_000, 0)
	now := start.Add(900 * time.Millisecond)
	validUntil := candidateValidUntil(start, 1_500)
	if !now.Before(validUntil) {
		t.Fatal("quorum unexpectedly expired")
	}
	if remaining := validUntil.Sub(now).Milliseconds(); remaining != 500 {
		t.Fatalf("remaining validity = %dms, want 500ms", remaining)
	}
}

func TestAcquireQuorumRejectsExpiredValidity(t *testing.T) {
	start := time.Unix(1_000, 0)
	now := start.Add(1_400 * time.Millisecond)
	validUntil := candidateValidUntil(start, 1_500)
	if now.Before(validUntil) {
		t.Fatal("quorum whose validUntil equals now is valid")
	}
}

func TestAcquireQuorumAcceptsAlreadyOwned(t *testing.T) {
	if !isSuccessfulAcquire(redleasev1.LeaseStatusALREADY_OWNED) {
		t.Fatal("ALREADY_OWNED did not count as an Acquire success")
	}
}

func TestQuorumParameters(t *testing.T) {
	tests := []struct {
		quorum         Quorum
		serverCount    int
		wantQuorumSize int
		valid          bool
	}{
		{quorum: Quorum1Of1, serverCount: 1, wantQuorumSize: 1, valid: true},
		{quorum: Quorum2Of3, serverCount: 3, wantQuorumSize: 2, valid: true},
		{quorum: Quorum3Of5, serverCount: 5, wantQuorumSize: 3, valid: true},
		{quorum: Quorum(4)},
	}

	for _, test := range tests {
		serverCount, quorumSize, valid := test.quorum.parameters()
		if serverCount != test.serverCount || quorumSize != test.wantQuorumSize || valid != test.valid {
			t.Fatalf(
				"quorum %d parameters = (%d, %d, %t), want (%d, %d, %t)",
				test.quorum,
				serverCount,
				quorumSize,
				valid,
				test.serverCount,
				test.wantQuorumSize,
				test.valid,
			)
		}
	}
}

func TestBestAcquireQuorumUsesEverySupportedThreshold(t *testing.T) {
	for _, quorum := range []Quorum{Quorum1Of1, Quorum2Of3, Quorum3Of5} {
		serverCount, quorumSize, _ := quorum.parameters()
		candidates := make([]time.Time, serverCount)
		successful := make([]bool, serverCount)
		for index := range candidates {
			candidates[index] = time.Unix(int64(1_000+index), 0)
		}
		for index := range quorumSize - 1 {
			successful[index] = true
		}
		if _, ok := bestAcquireQuorum(candidates, successful, quorumSize); ok {
			t.Fatalf("quorum %d succeeded below threshold", quorum)
		}

		successful[quorumSize-1] = true
		if _, ok := bestAcquireQuorum(candidates, successful, quorumSize); !ok {
			t.Fatalf("quorum %d did not succeed at threshold", quorum)
		}
	}
}
