package client

import (
	"math"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func TestAcquireQuorumZeroTTLIsExpired(t *testing.T) {
	start := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	responses := acquireResponses(0, 0, 0)

	validUntil, valid := acquireQuorumValidity(start, start, responses)
	if valid {
		t.Fatal("zero-TTL quorum is valid")
	}
	want := start.Add(-safetyMargin)
	if !validUntil.Equal(want) {
		t.Fatalf("validUntil = %v, want %v", validUntil, want)
	}
}

func TestAcquireQuorumUsesMinimumHeterogeneousTTL(t *testing.T) {
	start := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	responses := acquireResponses(2_000, 1_500, 3_000)

	validUntil, valid := acquireQuorumValidity(start, start.Add(200*time.Millisecond), responses)
	if !valid {
		t.Fatal("quorum unexpectedly expired")
	}
	want := start.Add(1_400 * time.Millisecond)
	if !validUntil.Equal(want) {
		t.Fatalf("validUntil = %v, want %v", validUntil, want)
	}
}

func TestAcquireQuorumAccountsForElapsedOperationTime(t *testing.T) {
	start := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	responses := acquireResponses(1_500, 1_500, 1_500)
	now := start.Add(900 * time.Millisecond)

	validUntil, valid := acquireQuorumValidity(start, now, responses)
	if !valid {
		t.Fatal("quorum unexpectedly expired")
	}
	if remaining := validUntil.Sub(now); remaining != 500*time.Millisecond {
		t.Fatalf("remaining validity = %v, want 500ms", remaining)
	}
}

func TestAcquireQuorumRejectsExpiredValidity(t *testing.T) {
	start := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	responses := acquireResponses(1_500, 1_500, 1_500)
	now := start.Add(1_400 * time.Millisecond)

	_, valid := acquireQuorumValidity(start, now, responses)
	if valid {
		t.Fatal("quorum whose validUntil equals now is valid")
	}
}

func TestAcquireQuorumAcceptsAlreadyOwned(t *testing.T) {
	start := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	responses := acquireResponses(1_000, 1_000, 1_000)
	responses[1].Status = redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED

	_, valid := acquireQuorumValidity(start, start, responses)
	if !valid {
		t.Fatal("ALREADY_OWNED did not count as an Acquire success")
	}
}

func TestRenewKeepsLaterPreviousValidUntil(t *testing.T) {
	start := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	previous := start.Add(3 * time.Second)
	responses := renewResponses(2_000, 2_500, 3_000)

	validUntil, quorumValid := renewQuorumValidity(
		start,
		start.Add(500*time.Millisecond),
		previous,
		responses,
	)
	if !quorumValid {
		t.Fatal("Renew quorum unexpectedly expired")
	}
	if !validUntil.Equal(previous) {
		t.Fatalf("validUntil = %v, want previous %v", validUntil, previous)
	}
}

func TestRenewUsesLaterQuorumValidUntil(t *testing.T) {
	start := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	previous := start.Add(time.Second)
	responses := renewResponses(2_000, 2_500, 3_000)

	validUntil, quorumValid := renewQuorumValidity(start, start, previous, responses)
	if !quorumValid {
		t.Fatal("Renew quorum unexpectedly expired")
	}
	want := start.Add(1_900 * time.Millisecond)
	if !validUntil.Equal(want) {
		t.Fatalf("validUntil = %v, want %v", validUntil, want)
	}
}

func TestCandidateTTLOutOfDurationRangeDoesNotWrap(t *testing.T) {
	start := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	candidate := candidateValidUntil(start, Milliseconds(math.MaxUint64))
	if !candidate.After(start) {
		t.Fatalf("overflowed candidate %v is not after start %v", candidate, start)
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
