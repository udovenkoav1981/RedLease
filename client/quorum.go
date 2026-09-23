package client

import (
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
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

	safetyMarginMS uint64 = 100
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

func candidateValidUntil(operationStart time.Time, ttlMS uint64) time.Time {
	if ttlMS <= safetyMarginMS || ttlMS > protocol.MaxTTLMS {
		return operationStart
	}
	return operationStart.Add(time.Duration(ttlMS-safetyMarginMS) * time.Millisecond)
}

func isSuccessfulAcquire(status redleasev1.LeaseStatus) bool {
	return status == redleasev1.LeaseStatusOK || status == redleasev1.LeaseStatusALREADY_OWNED
}
