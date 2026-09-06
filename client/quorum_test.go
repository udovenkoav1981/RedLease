package client

import (
	"math"
	"testing"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func TestAcquireQuorumZeroTTLIsExpired(t *testing.T) {
	const start uint64 = 1_000_000
	responses := acquireResponses(0, 0, 0)

	validUntil, valid := acquireQuorumValidity(start, start, responses)
	if valid {
		t.Fatal("zero-TTL quorum is valid")
	}
	if validUntil != start {
		t.Fatalf("validUntil = %d, want %d", validUntil, start)
	}
}

func TestAcquireQuorumUsesMinimumHeterogeneousTTL(t *testing.T) {
	const start uint64 = 1_000_000
	responses := acquireResponses(2_000, 1_500, 3_000)

	validUntil, valid := acquireQuorumValidity(start, start+200, responses)
	if !valid {
		t.Fatal("quorum unexpectedly expired")
	}
	want := start + 1_400
	if validUntil != want {
		t.Fatalf("validUntil = %d, want %d", validUntil, want)
	}
}

func TestAcquireQuorumAccountsForElapsedOperationTime(t *testing.T) {
	const start uint64 = 1_000_000
	responses := acquireResponses(1_500, 1_500, 1_500)
	now := start + 900

	validUntil, valid := acquireQuorumValidity(start, now, responses)
	if !valid {
		t.Fatal("quorum unexpectedly expired")
	}
	if remaining := validUntil - now; remaining != 500 {
		t.Fatalf("remaining validity = %dms, want 500ms", remaining)
	}
}

func TestAcquireQuorumRejectsExpiredValidity(t *testing.T) {
	const start uint64 = 1_000_000
	responses := acquireResponses(1_500, 1_500, 1_500)
	now := start + 1_400

	_, valid := acquireQuorumValidity(start, now, responses)
	if valid {
		t.Fatal("quorum whose validUntil equals now is valid")
	}
}

func TestAcquireQuorumAcceptsAlreadyOwned(t *testing.T) {
	const start uint64 = 1_000_000
	responses := acquireResponses(1_000, 1_000, 1_000)
	responses[1].Status = redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED

	_, valid := acquireQuorumValidity(start, start, responses)
	if !valid {
		t.Fatal("ALREADY_OWNED did not count as an Acquire success")
	}
}

func TestRenewKeepsLaterPreviousValidUntil(t *testing.T) {
	const start uint64 = 1_000_000
	previous := start + 3_000
	responses := renewResponses(2_000, 2_500, 3_000)

	validUntil, quorumValid := renewQuorumValidity(
		start,
		start+500,
		previous,
		responses,
	)
	if !quorumValid {
		t.Fatal("Renew quorum unexpectedly expired")
	}
	if validUntil != previous {
		t.Fatalf("validUntil = %d, want previous %d", validUntil, previous)
	}
}

func TestRenewUsesLaterQuorumValidUntil(t *testing.T) {
	const start uint64 = 1_000_000
	previous := start + 1_000
	responses := renewResponses(2_000, 2_500, 3_000)

	validUntil, quorumValid := renewQuorumValidity(start, start, previous, responses)
	if !quorumValid {
		t.Fatal("Renew quorum unexpectedly expired")
	}
	want := start + 1_900
	if validUntil != want {
		t.Fatalf("validUntil = %d, want %d", validUntil, want)
	}
}

func TestCandidateTTLOutOfDurationRangeDoesNotWrap(t *testing.T) {
	const start uint64 = 1_000_000
	candidate := candidateValidUntil(start, Milliseconds(math.MaxUint64))
	if candidate <= start {
		t.Fatalf("overflowed candidate %d is not after start %d", candidate, start)
	}
}

func acquireResponses(ttls ...uint64) [quorumSize]*redleasev1.AcquireResponse {
	var responses [quorumSize]*redleasev1.AcquireResponse
	for i, ttl := range ttls {
		responses[i] = &redleasev1.AcquireResponse{
			Status: redleasev1.LeaseStatus_LEASE_STATUS_OK,
			TtlMs:  ttl,
		}
	}
	return responses
}

func renewResponses(ttls ...uint64) [quorumSize]*redleasev1.RenewResponse {
	var responses [quorumSize]*redleasev1.RenewResponse
	for i, ttl := range ttls {
		responses[i] = &redleasev1.RenewResponse{
			Status: redleasev1.LeaseStatus_LEASE_STATUS_OK,
			TtlMs:  ttl,
		}
	}
	return responses
}
