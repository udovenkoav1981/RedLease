package client

import (
	"math"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

const (
	// ServerCount is fixed by the RedLease 3/5 quorum architecture.
	ServerCount  = 5
	quorumSize   = 3
	safetyMargin = 100 * time.Millisecond
)

func candidateValidUntil(operationStart time.Time, ttlMilliseconds Milliseconds) time.Time {
	return operationStart.Round(0).
		Add(millisecondsDuration(ttlMilliseconds)).
		Add(-safetyMargin)
}

func millisecondsDuration(milliseconds Milliseconds) time.Duration {
	const maxMilliseconds = Milliseconds(math.MaxInt64) / Milliseconds(time.Millisecond)
	if milliseconds > maxMilliseconds {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func selectQuorumValidUntil(candidates [quorumSize]time.Time) time.Time {
	validUntil := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.Before(validUntil) {
			validUntil = candidate
		}
	}
	return validUntil
}

func acquireQuorumValidity(
	operationStart time.Time,
	now time.Time,
	responses [quorumSize]*redleasev1.AcquireResponse,
) (time.Time, bool) {
	var candidates [quorumSize]time.Time
	for i, response := range responses {
		if response == nil || !isSuccessfulAcquire(response.GetStatus()) {
			return time.Time{}, false
		}
		candidates[i] = candidateValidUntil(operationStart, Milliseconds(response.GetTtlMs()))
	}

	validUntil := selectQuorumValidUntil(candidates)
	return validUntil, now.Round(0).Before(validUntil)
}

func renewQuorumValidity(
	operationStart time.Time,
	now time.Time,
	previousValidUntil time.Time,
	responses [quorumSize]*redleasev1.RenewResponse,
) (time.Time, bool) {
	var candidates [quorumSize]time.Time
	for i, response := range responses {
		if response == nil || response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
			return previousValidUntil.Round(0), false
		}
		candidates[i] = candidateValidUntil(operationStart, Milliseconds(response.GetTtlMs()))
	}

	quorumValidUntil := selectQuorumValidUntil(candidates)
	validUntil := previousValidUntil.Round(0)
	if quorumValidUntil.After(validUntil) {
		validUntil = quorumValidUntil
	}

	return validUntil, now.Round(0).Before(quorumValidUntil)
}

func isSuccessfulAcquire(status redleasev1.LeaseStatus) bool {
	return status == redleasev1.LeaseStatus_LEASE_STATUS_OK ||
		status == redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED
}
