package server

import (
	"hash/maphash"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

type operationKind uint8

const (
	operationAcquire operationKind = iota
	operationRenew
	operationRelease
)

type leaseID struct {
	clientID uint32
	bootID   uint32
	leaseSeq uint64
}

func makeLeaseID(id *redleasev1.LeaseID) leaseID {
	return leaseID{
		clientID: id.GetClientId(),
		bootID:   id.GetBootId(),
		leaseSeq: id.GetLeaseSeq(),
	}
}

type lease struct {
	id       leaseID
	deadline time.Time
}

type operation struct {
	requestID      uint64
	kind           operationKind
	key            string
	leaseID        leaseID
	requestedTTLMS uint64
}

type shardJob struct {
	operation operation
	complete  func(*redleasev1.ServerResponse)
}

type leaseShard struct {
	leases map[string]lease
	jobs   chan shardJob
}

func (s *Server) runShard(shard *leaseShard) {
	defer s.wg.Done()
	cleanup := time.NewTicker(leaseCleanupInterval)
	defer cleanup.Stop()

	for {
		select {
		case job, open := <-shard.jobs:
			if !open {
				return
			}
			job.complete(s.apply(shard, job.operation))
		case <-cleanup.C:
			deleteExpiredLeases(shard, time.Now().Round(0))
		}
	}
}

func deleteExpiredLeases(shard *leaseShard, now time.Time) {
	for key, current := range shard.leases {
		if !current.deadline.After(now) {
			delete(shard.leases, key)
		}
	}
}

func (s *Server) apply(shard *leaseShard, op operation) *redleasev1.ServerResponse {
	if !s.active() {
		return notReadyResponse(op)
	}

	now := time.Now().Round(0)
	switch op.kind {
	case operationAcquire:
		return s.acquire(shard, op, now)
	case operationRenew:
		return s.renew(shard, op, now)
	case operationRelease:
		return s.release(shard, op, now)
	default:
		panic("server: unknown operation kind")
	}
}

func (s *Server) acquire(shard *leaseShard, op operation, now time.Time) *redleasev1.ServerResponse {
	current, exists := shard.leases[op.key]
	if !exists || !current.deadline.After(now) {
		effectiveTTLMS := min(op.requestedTTLMS, s.config.configuredMaxTTLMS)
		shard.leases[op.key] = lease{
			id:       op.leaseID,
			deadline: now.Add(time.Duration(effectiveTTLMS) * time.Millisecond),
		}
		return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK, effectiveTTLMS)
	}
	if current.id == op.leaseID {
		return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED,
			remainingTTLMS(current.deadline, now, s.config.configuredMaxTTLMS))
	}
	return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
}

func (s *Server) renew(shard *leaseShard, op operation, now time.Time) *redleasev1.ServerResponse {
	current, exists := shard.leases[op.key]
	if !exists {
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_STALE, 0)
	}
	if !current.deadline.After(now) {
		delete(shard.leases, op.key)
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_STALE, 0)
	}
	if current.id != op.leaseID {
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_STALE, 0)
	}

	effectiveTTLMS := min(op.requestedTTLMS, s.config.configuredMaxTTLMS)
	candidate := now.Add(time.Duration(effectiveTTLMS) * time.Millisecond)
	if candidate.After(current.deadline) {
		current.deadline = candidate
		shard.leases[op.key] = current
	}
	return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK,
		remainingTTLMS(current.deadline, now, s.config.configuredMaxTTLMS))
}

func (s *Server) release(shard *leaseShard, op operation, now time.Time) *redleasev1.ServerResponse {
	current, exists := shard.leases[op.key]
	if exists && (!current.deadline.After(now) || current.id == op.leaseID) {
		delete(shard.leases, op.key)
	}
	return releaseResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK)
}

func remainingTTLMS(deadline, now time.Time, maximum uint64) uint64 {
	remaining := deadline.Sub(now)
	if remaining <= 0 {
		return 0
	}
	result := uint64(remaining / time.Millisecond)
	return min(result, maximum)
}

func acquireResponse(requestID uint64, status redleasev1.LeaseStatus, ttlMS uint64) *redleasev1.ServerResponse {
	return &redleasev1.ServerResponse{
		RequestId: requestID,
		Result: &redleasev1.ServerResponse_Acquire{Acquire: &redleasev1.AcquireResponse{
			Status: status,
			TtlMs:  ttlMS,
		}},
	}
}

func renewResponse(requestID uint64, status redleasev1.LeaseStatus, ttlMS uint64) *redleasev1.ServerResponse {
	return &redleasev1.ServerResponse{
		RequestId: requestID,
		Result: &redleasev1.ServerResponse_Renew{Renew: &redleasev1.RenewResponse{
			Status: status,
			TtlMs:  ttlMS,
		}},
	}
}

func releaseResponse(requestID uint64, status redleasev1.LeaseStatus) *redleasev1.ServerResponse {
	return &redleasev1.ServerResponse{
		RequestId: requestID,
		Result: &redleasev1.ServerResponse_Release{Release: &redleasev1.ReleaseResponse{
			Status: status,
		}},
	}
}

func notReadyResponse(op operation) *redleasev1.ServerResponse {
	switch op.kind {
	case operationAcquire:
		return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY, 0)
	case operationRenew:
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY, 0)
	case operationRelease:
		return releaseResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY)
	default:
		panic("server: unknown operation kind")
	}
}

var hashSeed = maphash.MakeSeed()

func (s *Server) shardIndex(key string) int {
	return int(maphash.String(hashSeed, key) % uint64(len(s.shards)))
}

func (s *Server) dispatch(ctxDone <-chan struct{}, job shardJob) bool {
	if s.closed.Load() {
		return false
	}

	shard := s.shards[s.shardIndex(job.operation.key)]
	s.dispatchMu.RLock()
	defer s.dispatchMu.RUnlock()
	if s.closed.Load() {
		return false
	}
	select {
	case shard.jobs <- job:
		return true
	case <-ctxDone:
		return false
	}
}
