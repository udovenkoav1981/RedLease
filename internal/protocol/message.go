// Package protocol defines RedLease wire messages and their FlatBuffers
// encoding. Decoded messages own their scalar values and never retain a view
// of a receive buffer.
package protocol

import redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"

// MaxTTLMS is the maximum lease TTL representable by the RedLease protocol.
const MaxTTLMS uint64 = 5_000

type Operation = redleasev1.ClientOperation

const (
	OperationAcquire = redleasev1.ClientOperationACQUIRE
	OperationRenew   = redleasev1.ClientOperationRENEW
	OperationRelease = redleasev1.ClientOperationRELEASE
	OperationGetTTL  = redleasev1.ClientOperationGET_TTL
)

type Status = redleasev1.LeaseStatus

const (
	StatusOK              = redleasev1.LeaseStatusOK
	StatusAlreadyOwned    = redleasev1.LeaseStatusALREADY_OWNED
	StatusBusy            = redleasev1.LeaseStatusBUSY
	StatusStale           = redleasev1.LeaseStatusSTALE
	StatusNotReady        = redleasev1.LeaseStatusNOT_READY
	StatusKeyLimitReached = redleasev1.LeaseStatusKEY_LIMIT_REACHED
)

// Request is the owned scalar representation queued by clients and dispatched
// by servers. Fields not used by Operation are zero.
type Request struct {
	RequestID      uint64
	Operation      Operation
	Key            uint64
	ClientID       uint32
	BootID         uint32
	LeaseSequence  uint64
	RequestedTTLMS uint64
}

// Response is the owned scalar representation returned by a server. TTLMS is
// the effective TTL for Acquire/Renew and configuredMaxTTL for GetTTL.
type Response struct {
	RequestID uint64
	Operation Operation
	Status    Status
	TTLMS     uint64
}
