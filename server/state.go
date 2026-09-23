package server

import (
	"container/heap"
	"context"
	"fmt"
	"hash/maphash"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

type operationKind uint8

const (
	operationAcquire operationKind = iota
	operationRenew
	operationRelease
	operationKindCount
)

type leaseID struct {
	clientID uint32
	bootID   uint32
	leaseSeq uint64
}

type lease struct {
	key       uint64
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
	key            uint64
	leaseID        leaseID
	requestedTTLMS uint64
}

type shardJob struct {
	operation operation
	complete  func(protocol.Response)
}

type leaseShard struct {
	mu        sync.Mutex
	leases    map[uint64]*lease
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
			// already accepted jobs so their connection waiters can also finish.
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

func (shard *leaseShard) addLease(key uint64, id leaseID, deadline time.Time) {
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

func (s *Server) removeExpiredKeys(now time.Time) bool {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()

	var deleted uint64
	for _, shard := range s.shards {
		deleted += removeExpiredKeysFromShard(shard, now)
	}
	return s.releaseKeys(deleted)
}

func removeExpiredKeysFromShard(shard *leaseShard, now time.Time) uint64 {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return shard.removeExpiredLeases(now)
}

func (s *Server) apply(shard *leaseShard, op operation) protocol.Response {
	if op.kind >= operationKindCount {
		s.fail(fmt.Errorf("unknown operation kind %d", op.kind))
		return protocol.Response{RequestID: op.requestID}
	}
	if !s.active() {
		return notReadyResponse(op)
	}
	s.operationTotals[op.kind].Add(1)
	now := time.Now()
	switch op.kind {
	case operationAcquire:
		return s.acquire(shard, op, now)
	case operationRenew:
		return s.renew(shard, op, now)
	case operationRelease:
		return s.release(shard, op, now)
	default:
		return protocol.Response{RequestID: op.requestID}
	}
}

func (s *Server) acquire(shard *leaseShard, op operation, now time.Time) protocol.Response {
	effectiveTTLMS := min(op.requestedTTLMS, s.config.MaxTTL)
	cleanupAttempted := false
	for {
		response, capacityFull := s.acquireLocked(shard, op, now, effectiveTTLMS)
		if !capacityFull {
			return response
		}

		if cleanupAttempted {
			return acquireResponse(op.requestID, redleasev1.LeaseStatusKEY_LIMIT_REACHED, 0)
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
	now time.Time,
	effectiveTTLMS uint64,
) (protocol.Response, bool) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	current, exists := shard.leases[op.key]
	if exists && current.deadline.After(now) && current.id == op.leaseID {
		return acquireResponse(
			op.requestID,
			redleasev1.LeaseStatusALREADY_OWNED,
			remainingTTLMS(current.deadline, now, s.config.MaxTTL),
		), false
	}
	if exists && current.deadline.After(now) {
		return acquireResponse(op.requestID, redleasev1.LeaseStatusBUSY, 0), false
	}
	if exists {
		shard.removeLease(current)
		if !s.releaseKeys(1) {
			return notReadyResponse(op), false
		}
	}

	if effectiveTTLMS == 0 {
		return acquireResponse(op.requestID, redleasev1.LeaseStatusOK, 0), false
	}
	if !s.reserveKey() {
		return protocol.Response{}, true
	}
	shard.addLease(
		op.key,
		op.leaseID,
		now.Add(time.Duration(effectiveTTLMS)*time.Millisecond),
	)
	return acquireResponse(op.requestID, redleasev1.LeaseStatusOK, effectiveTTLMS), false
}

func (s *Server) renew(shard *leaseShard, op operation, now time.Time) protocol.Response {
	shard.mu.Lock()

	current, exists := shard.leases[op.key]
	if !exists {
		shard.mu.Unlock()
		s.logLeaseOperation(
			slog.LevelWarn,
			"renew rejected because lease does not exist",
			"renew",
			"not_found",
			op,
			false,
			0,
		)
		return renewResponse(op.requestID, redleasev1.LeaseStatusSTALE, 0)
	}
	if !current.deadline.After(now) {
		expiredByMS := uint64(now.Sub(current.deadline).Milliseconds())
		shard.removeLease(current)
		released := s.releaseKeys(1)
		shard.mu.Unlock()
		s.logLeaseOperation(
			slog.LevelWarn,
			"renew rejected because lease has expired",
			"renew",
			"expired",
			op,
			true,
			expiredByMS,
		)
		if !released {
			return notReadyResponse(op)
		}
		return renewResponse(op.requestID, redleasev1.LeaseStatusSTALE, 0)
	}
	if current.id != op.leaseID {
		shard.mu.Unlock()
		return renewResponse(op.requestID, redleasev1.LeaseStatusSTALE, 0)
	}

	effectiveTTLMS := min(op.requestedTTLMS, s.config.MaxTTL)
	candidate := now.Add(time.Duration(effectiveTTLMS) * time.Millisecond)
	if candidate.After(current.deadline) {
		current.deadline = candidate
		heap.Fix(&shard.deadlines, current.heapIndex)
	}
	remaining := remainingTTLMS(current.deadline, now, s.config.MaxTTL)
	shard.mu.Unlock()
	return renewResponse(op.requestID, redleasev1.LeaseStatusOK, remaining)
}

func (s *Server) release(shard *leaseShard, op operation, now time.Time) protocol.Response {
	shard.mu.Lock()

	current, exists := shard.leases[op.key]
	if !exists {
		shard.mu.Unlock()
		return releaseResponse(op.requestID, redleasev1.LeaseStatusOK)
	}
	if !current.deadline.After(now) {
		expiredByMS := uint64(now.Sub(current.deadline).Milliseconds())
		shard.removeLease(current)
		released := s.releaseKeys(1)
		shard.mu.Unlock()
		s.logLeaseOperation(
			slog.LevelWarn,
			"release found an expired lease",
			"release",
			"expired",
			op,
			true,
			expiredByMS,
		)
		if !released {
			return notReadyResponse(op)
		}
		return releaseResponse(op.requestID, redleasev1.LeaseStatusOK)
	}
	if current.id == op.leaseID {
		shard.removeLease(current)
		released := s.releaseKeys(1)
		shard.mu.Unlock()
		if !released {
			return notReadyResponse(op)
		}
		return releaseResponse(op.requestID, redleasev1.LeaseStatusOK)
	}
	shard.mu.Unlock()
	return releaseResponse(op.requestID, redleasev1.LeaseStatusOK)
}

func (s *Server) logLeaseOperation(
	level slog.Level,
	message string,
	operationName string,
	reason string,
	op operation,
	includeExpiredBy bool,
	expiredByMS uint64,
) {
	ctx := context.Background()
	if !s.logger.Enabled(ctx, level) {
		return
	}

	attrs := [...]slog.Attr{
		slog.String("operation", operationName),
		slog.String("reason", reason),
		slog.Uint64("key", op.key),
		slog.Uint64("request_id", op.requestID),
		slog.Uint64("client_id", uint64(op.leaseID.clientID)),
		slog.Uint64("boot_id", uint64(op.leaseID.bootID)),
		slog.Uint64("lease_seq", op.leaseID.leaseSeq),
		{},
	}
	count := len(attrs) - 1
	if includeExpiredBy {
		attrs[count] = slog.Uint64("expired_by_ms", expiredByMS)
		count++
	}
	s.logger.LogAttrs(ctx, level, message, attrs[:count]...)
}

func remainingTTLMS(deadline, now time.Time, maximum uint64) uint64 {
	remaining := deadline.Sub(now).Milliseconds()
	if remaining <= 0 {
		return 0
	}
	return min(uint64(remaining), maximum)
}

func acquireResponse(requestID uint64, status redleasev1.LeaseStatus, ttlMS uint64) protocol.Response {
	return protocol.Response{
		RequestID: requestID,
		Operation: redleasev1.ClientOperationACQUIRE,
		Status:    status,
		TTLMS:     ttlMS,
	}
}

func renewResponse(requestID uint64, status redleasev1.LeaseStatus, ttlMS uint64) protocol.Response {
	return protocol.Response{
		RequestID: requestID,
		Operation: redleasev1.ClientOperationRENEW,
		Status:    status,
		TTLMS:     ttlMS,
	}
}

func releaseResponse(requestID uint64, status redleasev1.LeaseStatus) protocol.Response {
	return protocol.Response{
		RequestID: requestID,
		Operation: redleasev1.ClientOperationRELEASE,
		Status:    status,
	}
}

func notReadyResponse(op operation) protocol.Response {
	return statusResponse(op, redleasev1.LeaseStatusNOT_READY)
}

func statusResponse(op operation, status redleasev1.LeaseStatus) protocol.Response {
	switch op.kind {
	case operationAcquire:
		return acquireResponse(op.requestID, status, 0)
	case operationRenew:
		return renewResponse(op.requestID, status, 0)
	case operationRelease:
		return releaseResponse(op.requestID, status)
	default:
		return protocol.Response{RequestID: op.requestID}
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

func (s *Server) shardIndex(key uint64) int {
	return int(maphash.Comparable(hashSeed, key) % uint64(len(s.shards)))
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
