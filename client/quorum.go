package client

import (
	"github.com/udovenkoav1981/RedLease/internal/boottime"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

// Quorum selects one supported majority configuration.
type Quorum uint8

const (
	// Quorum1Of1 selects one required response from one server.
	Quorum1Of1 Quorum = iota + 1
	// Quorum2Of3 selects two required responses from three servers.
	Quorum2Of3
	// Quorum3Of5 selects three required responses from five servers.
	Quorum3Of5

	safetyMargin = Milliseconds(100)
)

func (q Quorum) String() string {
	switch q {
	case Quorum1Of1:
		return "1/1"
	case Quorum2Of3:
		return "2/3"
	case Quorum3Of5:
		return "3/5"
	default:
		return "unknown"
	}
}

func (q Quorum) parameters() (serverCount, quorumSize int, valid bool) {
	switch q {
	case Quorum1Of1:
		return 1, 1, true
	case Quorum2Of3:
		return 3, 2, true
	case Quorum3Of5:
		return 5, 3, true
	default:
		return 0, 0, false
	}
}

func (q Quorum) size() int {
	_, size, _ := q.parameters()
	return size
}

func candidateValidUntil(operationStart uint64, ttlMilliseconds Milliseconds) uint64 {
	if ttlMilliseconds <= safetyMargin {
		return operationStart
	}
	return boottime.Add(operationStart, uint64(ttlMilliseconds-safetyMargin))
}

func selectQuorumValidUntil(candidates []uint64) uint64 {
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
	responses []*redleasev1.AcquireResponse,
) (uint64, bool) {
	candidates := make([]uint64, len(responses))
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
	responses []*redleasev1.RenewResponse,
) (uint64, bool) {
	candidates := make([]uint64, len(responses))
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
