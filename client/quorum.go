package client

import (
	"github.com/udovenkoav1981/RedLease/internal/boottime"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

const (
	// ServerCount is fixed by the RedLease 3/5 quorum architecture.
	ServerCount  = 5
	quorumSize   = 3
	safetyMargin = Milliseconds(100)
)

func candidateValidUntil(operationStart uint64, ttlMilliseconds Milliseconds) uint64 {
	if ttlMilliseconds <= safetyMargin {
		return operationStart
	}
	return boottime.Add(operationStart, uint64(ttlMilliseconds-safetyMargin))
}

func selectQuorumValidUntil(candidates [quorumSize]uint64) uint64 {
	validUntil := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate < validUntil {
			validUntil = candidate
		}
	}
	return validUntil
}

func acquireQuorumValidity(
	operationStart uint64,
	now uint64,
	responses [quorumSize]*redleasev1.AcquireResponse,
) (uint64, bool) {
	var candidates [quorumSize]uint64
	for i, response := range responses {
		if response == nil || !isSuccessfulAcquire(response.GetStatus()) {
			return 0, false
		}
		candidates[i] = candidateValidUntil(operationStart, Milliseconds(response.GetTtlMs()))
	}

	validUntil := selectQuorumValidUntil(candidates)
	return validUntil, now < validUntil
}

func renewQuorumValidity(
	operationStart uint64,
	now uint64,
	previousValidUntil uint64,
	responses [quorumSize]*redleasev1.RenewResponse,
) (uint64, bool) {
	var candidates [quorumSize]uint64
	for i, response := range responses {
		if response == nil || response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
			return previousValidUntil, false
		}
		candidates[i] = candidateValidUntil(operationStart, Milliseconds(response.GetTtlMs()))
	}

	quorumValidUntil := selectQuorumValidUntil(candidates)
	validUntil := previousValidUntil
	if quorumValidUntil > validUntil {
		validUntil = quorumValidUntil
	}

	return validUntil, now < quorumValidUntil
}

func isSuccessfulAcquire(status redleasev1.LeaseStatus) bool {
	return status == redleasev1.LeaseStatus_LEASE_STATUS_OK ||
		status == redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED
}
