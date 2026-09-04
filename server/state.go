package server

import (
	"container/heap"
	"hash/maphash"
	"sync"
	"time"

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
	deadline  time.Time
	heapIndex int
}

type leaseDeadlineHeap []*lease

func (h leaseDeadlineHeap) Len() int { return len(h) }

func (h leaseDeadlineHeap) Less(i, j int) bool {
	return h[i].deadline.Before(h[j].deadline)
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
	for job := range shard.jobs {
		job.complete(s.apply(shard, job.operation))
	}
}

func (shard *leaseShard) addLease(key string, id leaseID, deadline time.Time) {
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

func (shard *leaseShard) removeExpiredLeases(now time.Time) uint64 {
	var deleted uint64
	for len(shard.deadlines) != 0 && !shard.deadlines[0].deadline.After(now) {
		current := heap.Pop(&shard.deadlines).(*lease)
		delete(shard.leases, current.key)
		deleted++
	}
	return deleted
}

func (s *Server) removeExpiredKeys(now time.Time) {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()

	var deleted uint64
	for _, shard := range s.shards {
		shard.mu.Lock()
		deleted += shard.removeExpiredLeases(now)
		shard.mu.Unlock()
	}
	s.releaseKeys(deleted)
}

func (s *Server) apply(shard *leaseShard, op operation) *redleasev1.ServerResponse {
	if !s.active() {
		return notReadyResponse(op)
	}
	if len(op.key) > protocol.MaxKeyBytes {
		return statusResponse(op, redleasev1.LeaseStatus_LEASE_STATUS_KEY_TOO_LARGE)
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
	effectiveTTLMS := min(op.requestedTTLMS, s.config.MaxTTL)
	cleanupAttempted := false
	for {
		shard.mu.Lock()
		current, exists := shard.leases[op.key]
		if exists && current.deadline.After(now) && current.id == op.leaseID {
			response := acquireResponse(
				op.requestID,
				redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED,
				remainingTTLMS(current.deadline, now, s.config.MaxTTL),
			)
			shard.mu.Unlock()
			return response
		}
		if exists && current.deadline.After(now) {
			shard.mu.Unlock()
			return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
		}
		if exists {
			shard.removeLease(current)
			s.releaseKeys(1)
		}

		if effectiveTTLMS == 0 {
			shard.mu.Unlock()
			return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK, 0)
		}
		if s.reserveKey() {
			shard.addLease(
				op.key,
				op.leaseID,
				now.Add(time.Duration(effectiveTTLMS)*time.Millisecond),
			)
			shard.mu.Unlock()
			return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK, effectiveTTLMS)
		}
		shard.mu.Unlock()

		if cleanupAttempted {
			return acquireResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_KEY_LIMIT_REACHED, 0)
		}
		s.removeExpiredKeys(now)
		cleanupAttempted = true
	}
}

func (s *Server) renew(shard *leaseShard, op operation, now time.Time) *redleasev1.ServerResponse {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.leases[op.key]
	if !exists {
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_STALE, 0)
	}
	if !current.deadline.After(now) {
		shard.removeLease(current)
		s.releaseKeys(1)
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_STALE, 0)
	}
	if current.id != op.leaseID {
		return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_STALE, 0)
	}

	effectiveTTLMS := min(op.requestedTTLMS, s.config.MaxTTL)
	candidate := now.Add(time.Duration(effectiveTTLMS) * time.Millisecond)
	if candidate.After(current.deadline) {
		current.deadline = candidate
		heap.Fix(&shard.deadlines, current.heapIndex)
	}
	return renewResponse(op.requestID, redleasev1.LeaseStatus_LEASE_STATUS_OK,
		remainingTTLMS(current.deadline, now, s.config.MaxTTL))
}

func (s *Server) release(shard *leaseShard, op operation, now time.Time) *redleasev1.ServerResponse {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.leases[op.key]
	if exists && (!current.deadline.After(now) || current.id == op.leaseID) {
		shard.removeLease(current)
		s.releaseKeys(1)
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
		panic("server: unknown operation kind")
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

func (s *Server) releaseKeys(count uint64) {
	for count != 0 {
		current := s.keys.Load()
		if count > current {
			panic("server: lease key count underflow")
		}
		if s.keys.CompareAndSwap(current, current-count) {
			return
		}
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
