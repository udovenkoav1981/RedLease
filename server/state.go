package server

import (
	"container/heap"
	"fmt"
	"hash/maphash"
	"runtime/debug"
	"sync"

	"github.com/udovenkoav1981/RedLease/internal/boottime"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
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
	key       string
	id        leaseID
	deadline  uint64
	heapIndex int
}

type leaseDeadlineHeap []*lease

func (h leaseDeadlineHeap) Len() int { return len(h) }

func (h leaseDeadlineHeap) Less(i, j int) bool {
	return h[i].deadline < h[j].deadline
}

func (h leaseDeadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *leaseDeadlineHeap) Push(value any) {
	current := value.(*lease)
	current.heapIndex = len(*h)
	*h = append(*h, current)
}

func (h *leaseDeadlineHeap) Pop() any {
	old := *h
	last := len(old) - 1
	current := old[last]
	old[last] = nil
	current.heapIndex = -1
	*h = old[:last]
	return current
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
	mu        sync.Mutex
	leases    map[string]*lease
	deadlines leaseDeadlineHeap
	jobs      chan shardJob
}

func (s *Server) runShard(shard *leaseShard) {
	defer s.wg.Done()

	var current shardJob
	hasCurrent := false
	completing := false
	defer func() {
		if recovered := recover(); recovered != nil {
			s.failRecoveredPanic("processing shard operation", recovered)
			if hasCurrent && !completing {
				s.completeFailedJob(current)
			}
			// The owner closes a failed Server after receiving Fatal. Drain the
			// already accepted jobs so their stream waiters can also finish.
			for queued := range shard.jobs {
				s.completeFailedJob(queued)
			}
		}
	}()

	for job := range shard.jobs {
		current = job
		hasCurrent = true
		response := s.apply(shard, job.operation)
		completing = true
		job.complete(response)
		completing = false
		hasCurrent = false
	}
}

func (s *Server) completeFailedJob(job shardJob) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.failRecoveredPanic("completing failed shard operation", recovered)
		}
	}()
	job.complete(notReadyResponse(job.operation))
}

func (s *Server) failRecoveredPanic(scope string, recovered any) {
	s.fail(fmt.Errorf(
		"panic while %s: %v\n%s",
		scope,
		recovered,
		debug.Stack(),
	))
}

func (shard *leaseShard) addLease(key string, id leaseID, deadline uint64) {
	current := &lease{
		key:       key,
		id:        id,
		deadline:  deadline,
		heapIndex: -1,
	}
	shard.leases[key] = current
	heap.Push(&shard.deadlines, current)
}

func (shard *leaseShard) removeLease(current *lease) {
	delete(shard.leases, current.key)
	heap.Remove(&shard.deadlines, current.heapIndex)
}

func (shard *leaseShard) removeExpiredLeases(now uint64) uint64 {
	var deleted uint64
	for len(shard.deadlines) != 0 && shard.deadlines[0].deadline <= now {
		current := heap.Pop(&shard.deadlines).(*lease)
		delete(shard.leases, current.key)
		deleted++
	}
	return deleted
}

func (s *Server) removeExpiredKeys(now uint64) bool {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()

	var deleted uint64
	for _, shard := range s.shards {
		deleted += removeExpiredKeysFromShard(shard, now)
	}
	return s.releaseKeys(deleted)
}

func removeExpiredKeysFromShard(shard *leaseShard, now uint64) uint64 {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return shard.removeExpiredLeases(now)
}

func (s *Server) apply(shard *leaseShard, op operation) *redleasev1.ServerResponse {
	if op.kind > operationRelease {
		s.fail(fmt.Errorf("unknown operation kind %d", op.kind))
		return &redleasev1.ServerResponse{RequestId: op.requestID}
	}
	if !s.active() {
		return notReadyResponse(op)
	}
	if len(op.key) > protocol.MaxKeyBytes {
		return statusResponse(op, redleasev1.LeaseStatus_LEASE_STATUS_KEY_TOO_LARGE)
	}

	now := boottime.Now()
	switch op.kind {
	case operationAcquire:
		return s.acquire(shard, op, now)
	case operationRenew:
		return s.renew(shard, op, now)
	case operationRelease:
		return s.release(shard, op, now)
	default:
		return &redleasev1.ServerResponse{RequestId: op.requestID}
	}
}

func (s *Server) acquire(shard *leaseShard, op operation, now uint64) *redleasev1.ServerResponse {
	effectiveTTLMS := min(op.requestedTTLMS, s.config.MaxTTL)
	cleanupAttempted := false
	for {
		response, capacityFull := s.acquireLocked(shard, op, now, effectiveTTLMS)
		if !capacityFull {
			return response
		}

		if cleanupAttempted {
			return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_KEY_LIMIT_REACHED, 0)
		}
		if !s.removeExpiredKeys(now) {
			return notReadyResponse(op)
		}
		cleanupAttempted = true
	}
}

func (s *Server) acquireLocked(
	shard *leaseShard,
	op operation,
	now uint64,
	effectiveTTLMS uint64,
) (*redleasev1.ServerResponse, bool) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.leases[op.key]
	if exists && current.deadline > now && current.id == op.leaseID {
		return acquireResponse(
			op.requestID,
			redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED,
			remainingTTLMS(current.deadline, now, s.config.MaxTTL),
		), false
	}
	if exists && current.deadline > now {
		return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0), false
	}
	if exists {
		shard.removeLease(current)
		if !s.releaseKeys(1) {
			return notReadyResponse(op), false
		}
	}

	if effectiveTTLMS == 0 {
		return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK, 0), false
	}
	if !s.reserveKey() {
		return nil, true
	}
	shard.addLease(
		op.key,
		op.leaseID,
		boottime.Add(now, effectiveTTLMS),
	)
	return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK, effectiveTTLMS), false
}

func (s *Server) renew(shard *leaseShard, op operation, now uint64) *redleasev1.ServerResponse {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.leases[op.key]
	if !exists {
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_STALE, 0)
	}
	if current.deadline <= now {
		shard.removeLease(current)
		if !s.releaseKeys(1) {
			return notReadyResponse(op)
		}
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_STALE, 0)
	}
	if current.id != op.leaseID {
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_STALE, 0)
	}

	effectiveTTLMS := min(op.requestedTTLMS, s.config.MaxTTL)
	candidate := boottime.Add(now, effectiveTTLMS)
	if candidate > current.deadline {
		current.deadline = candidate
		heap.Fix(&shard.deadlines, current.heapIndex)
	}
	return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK,
		remainingTTLMS(current.deadline, now, s.config.MaxTTL))
}

func (s *Server) release(shard *leaseShard, op operation, now uint64) *redleasev1.ServerResponse {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.leases[op.key]
	if exists && (current.deadline <= now || current.id == op.leaseID) {
		shard.removeLease(current)
		if !s.releaseKeys(1) {
			return notReadyResponse(op)
		}
	}
	return releaseResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK)
}

func remainingTTLMS(deadline, now, maximum uint64) uint64 {
	return min(boottime.Remaining(deadline, now), maximum)
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
	return statusResponse(op, redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY)
}

func statusResponse(op operation, status redleasev1.LeaseStatus) *redleasev1.ServerResponse {
	switch op.kind {
	case operationAcquire:
		return acquireResponse(op.requestID, status, 0)
	case operationRenew:
		return renewResponse(op.requestID, status, 0)
	case operationRelease:
		return releaseResponse(op.requestID, status)
	default:
		return &redleasev1.ServerResponse{RequestId: op.requestID}
	}
}

func (s *Server) reserveKey() bool {
	for {
		current := s.keys.Load()
		if current >= s.config.MaxKeys {
			return false
		}
		if s.keys.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (s *Server) releaseKeys(count uint64) bool {
	for count != 0 {
		current := s.keys.Load()
		if count > current {
			s.fail(fmt.Errorf(
				"lease key count underflow: release %d reservations from %d",
				count,
				current,
			))
			return false
		}
		if s.keys.CompareAndSwap(current, current-count) {
			return true
		}
	}
	return true
}

var hashSeed = maphash.MakeSeed()

func (s *Server) shardIndex(key string) int {
	return int(maphash.String(hashSeed, key) % uint64(len(s.shards)))
}

func (s *Server) dispatch(ctxDone <-chan struct{}, job shardJob) bool {
	if !s.active() {
		return false
	}

	shard := s.shards[s.shardIndex(job.operation.key)]
	s.dispatchMu.RLock()
	defer s.dispatchMu.RUnlock()
	if !s.active() {
		return false
	}
	select {
	case shard.jobs <- job:
		return true
	case <-ctxDone:
		return false
	}
}
