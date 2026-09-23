// Package protocol defines RedLease wire framing, shared constants, and owned
// response values. Server requests are decoded directly into server operations.
package protocol

import redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"

// MaxTTLMS is the maximum lease TTL representable by the RedLease protocol.
const MaxTTLMS uint64 = 5_000

type Operation = redleasev1.ClientOperation

const (
	OperationAcquire = redleasev1.ClientOperationACQUIRE
	OperationRenew   = redleasev1.ClientOperationRENEW
	OperationRelease = redleasev1.ClientOperationRELEASE
	OperationGetTTL  = redleasev1.ClientOperationGET_TTL
)

// Response is the owned scalar representation returned by a server. TTLMS is
// the effective TTL for Acquire/Renew and configuredMaxTTL for GetTTL.
type Response struct {
	RequestID uint64
	Operation Operation
	Status    redleasev1.LeaseStatus
	TTLMS     uint64
}
