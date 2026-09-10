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

func isSuccessfulAcquire(status redleasev1.LeaseStatus) bool {
	return status == redleasev1.LeaseStatus_LEASE_STATUS_OK ||
		status == redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED
}
