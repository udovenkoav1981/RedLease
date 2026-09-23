package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

var (
	testEpoch  = time.Unix(1_000, 0)
	testLogger = slog.New(slog.DiscardHandler)
)

func newTestServer(t *testing.T, maxTTL uint64, shardCount uint32) *Server {
	t.Helper()
	s, err := New(Config{
		MaxTTL:          maxTTL,
		Logger:          testLogger,
		ShardCount:      shardCount,
		ShardQueueDepth: 8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func activateServer(t *testing.T, s *Server) {
	t.Helper()
	if !s.phase.CompareAndSwap(uint32(phaseQuarantine), uint32(phaseActive)) && !s.active() {
		t.Fatal("server did not leave quarantine")
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{name: "minimum", config: Config{MaxTTL: 1, Logger: testLogger}},
		{name: "protocol maximum", config: Config{MaxTTL: uint64(ProtocolMaxTTL / time.Millisecond), Logger: testLogger}},
		{name: "missing logger", config: Config{MaxTTL: 1}, wantErr: true},
		{name: "zero TTL", config: Config{Logger: testLogger}, wantErr: true},
		{name: "over protocol maximum", config: Config{MaxTTL: uint64(ProtocolMaxTTL/time.Millisecond) + 1, Logger: testLogger}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigDefaultsMaxKeys(t *testing.T) {
	config, err := resolveConfig(Config{MaxTTL: 1_000, Logger: testLogger})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if config.MaxKeys != DefaultMaxKeys {
		t.Fatalf("max keys = %d, want %d", config.MaxKeys, DefaultMaxKeys)
	}
}

func TestQuarantineAndGetTTL(t *testing.T) {
	s := newTestServer(t, 2_000, 1)
	shard := s.shards[0]
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}

	acquire := s.apply(shard, operation{requestID: 10, kind: operationAcquire, key: 1, leaseID: id, requestedTTLMS: 1000})
	if got := acquire.Status; got != redleasev1.LeaseStatusNOT_READY {
		t.Fatalf("Acquire during quarantine = %s", got)
	}
	renew := s.apply(shard, operation{requestID: 11, kind: operationRenew, key: 1, leaseID: id, requestedTTLMS: 1000})
	if got := renew.Status; got != redleasev1.LeaseStatusNOT_READY {
		t.Fatalf("Renew during quarantine = %s", got)
	}
	release := s.apply(shard, operation{requestID: 12, kind: operationRelease, key: 1, leaseID: id})
	if got := release.Status; got != redleasev1.LeaseStatusNOT_READY {
		t.Fatalf("Release during quarantine = %s", got)
	}

	_, getTTL, direct, err := s.decodeRequest(getTTLRequest(13).Table().Bytes)
	if err != nil {
		t.Fatalf("decode GetTTL: %v", err)
	}
	if !direct {
		t.Fatal("GetTTL was not decoded as a direct response")
	}
	if got := getTTL.TTLMS; got != 2000 {
		t.Fatalf("GetTTL during quarantine = %d, want 2000", got)
	}

	activateServer(t, s)
	acquire = s.apply(shard, operation{requestID: 14, kind: operationAcquire, key: 1, leaseID: id, requestedTTLMS: 1000})
	if got := acquire.Status; got != redleasev1.LeaseStatusOK {
		t.Fatalf("Acquire after quarantine = %s", got)
	}
}

func TestOperationMetricsCountActiveRequestsOnce(t *testing.T) {
	s := newTestServer(t, 2_000, 1)
	shard := s.shards[0]
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}

	s.apply(shard, operation{kind: operationAcquire, key: 5, leaseID: id, requestedTTLMS: 1_000})
	s.apply(shard, operation{kind: operationRenew, key: 5, leaseID: id, requestedTTLMS: 1_000})
	s.apply(shard, operation{kind: operationRelease, key: 5, leaseID: id})
	if got := s.MetricsSnapshot(); got.AcquiresTotal != 0 || got.RenewsTotal != 0 || got.ReleasesTotal != 0 {
		t.Fatalf("quarantine operation totals = (%d, %d, %d), want all zero", got.AcquiresTotal, got.RenewsTotal, got.ReleasesTotal)
	}

	activateServer(t, s)
	s.apply(shard, operation{
		kind:           operationAcquire,
		key:            1,
		leaseID:        id,
		requestedTTLMS: 1_000,
	})
	s.apply(shard, operation{kind: operationRenew, key: 6, leaseID: id, requestedTTLMS: 1_000})
	s.apply(shard, operation{kind: operationRelease, key: 6, leaseID: id})

	got := s.MetricsSnapshot()
	if got.AcquiresTotal != 1 || got.RenewsTotal != 1 || got.ReleasesTotal != 1 {
		t.Fatalf("active operation totals = (%d, %d, %d), want (1, 1, 1)", got.AcquiresTotal, got.RenewsTotal, got.ReleasesTotal)
	}
}

func TestMetricsSnapshot(t *testing.T) {
	firstJobs := make(chan shardJob, 2)
	secondJobs := make(chan shardJob, 2)
	firstJobs <- shardJob{}
	secondJobs <- shardJob{}
	secondJobs <- shardJob{}
	s := &Server{
		config: Config{SkipRestartQuarantine: true},
		shards: []*leaseShard{{jobs: firstJobs}, {jobs: secondJobs}},
	}
	s.phase.Store(uint32(phaseFailed))
	s.keys.Store(7)
	s.activeConnections.Store(2)
	s.operationTotals[operationAcquire].Store(11)
	s.operationTotals[operationRenew].Store(12)
	s.operationTotals[operationRelease].Store(13)

	got := s.MetricsSnapshot()
	if got.State != "failed" || got.ResidentKeys != 7 || got.QueuedOperations != 3 || got.ActiveConnections != 2 ||
		got.AcquiresTotal != 11 || got.RenewsTotal != 12 || got.ReleasesTotal != 13 || !got.RestartQuarantineSkipped {
		t.Fatalf("MetricsSnapshot() = %+v", got)
	}
}

func TestMetricsState(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		phase serverPhase
		want  string
	}{
		{phaseQuarantine, "quarantine"},
		{phaseActive, "active"},
		{phaseFailed, "failed"},
		{phaseClosed, "closed"},
		{serverPhase(100), "unknown"},
	} {
		if got := metricsState(test.phase); got != test.want {
			t.Errorf("metricsState(%d) = %q, want %q", test.phase, got, test.want)
		}
	}
}

func TestSkipRestartQuarantineStartsActiveWithoutTimer(t *testing.T) {
	s, err := New(Config{
		MaxTTL:                2_000,
		Logger:                testLogger,
		SkipRestartQuarantine: true,
		ShardCount:            1,
		ShardQueueDepth:       8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if !s.active() {
		t.Fatal("server with skipped restart quarantine is not active")
	}
	if s.timer != nil {
		t.Fatal("server with skipped restart quarantine created a timer")
	}

	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	response := s.apply(s.shards[0], operation{
		requestID:      1,
		kind:           operationAcquire,
		key:            1,
		leaseID:        id,
		requestedTTLMS: 1_000,
	})
	if got := response.Status; got != redleasev1.LeaseStatusOK {
		t.Fatalf("immediate Acquire = %s, want OK", got)
	}
}

func TestKeyCountUnderflowFailsServerWithoutPanicking(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	activateServer(t, s)

	if released := s.releaseKeys(1); released {
		t.Fatal("releaseKeys reported success for an underflow")
	}

	select {
	case err := <-s.Fatal():
		if !errors.Is(err, ErrServerFailed) {
			t.Fatalf("fatal error = %v, want ErrServerFailed", err)
		}
		if !strings.Contains(err.Error(), "release 1 reservations from 0") {
			t.Fatalf("fatal error lacks counter context: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not publish its fatal error")
	}

	if got := serverPhase(s.phase.Load()); got != phaseFailed {
		t.Fatalf("server phase = %d, want FAILED", got)
	}
	select {
	case <-s.ctx.Done():
	default:
		t.Fatal("server failure did not cancel active connections")
	}

	response := s.apply(s.shards[0], operation{
		kind:           operationAcquire,
		key:            1,
		leaseID:        leaseID{clientID: 1, bootID: 1, leaseSeq: 1},
		requestedTTLMS: 1_000,
	})
	if response.Status != redleasev1.LeaseStatusNOT_READY {
		t.Fatalf("Acquire after failure = %s, want NOT_READY", response.Status)
	}

	serverConn, clientConn := net.Pipe()
	if err := s.serveConnection(serverConn); !errors.Is(err, ErrServerFailed) {
		t.Fatalf("new connection after failure error = %v, want ErrServerFailed", err)
	}
	_ = serverConn.Close()
	_ = clientConn.Close()

	if released := s.releaseKeys(1); released {
		t.Fatal("second releaseKeys reported success for an underflow")
	}
	select {
	case err := <-s.Fatal():
		t.Fatalf("server published a second fatal error: %v", err)
	default:
	}
}

func TestNormalCloseDoesNotPublishFatalError(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-s.Fatal():
		t.Fatalf("normal Close published fatal error: %v", err)
	default:
	}
}

func TestShardPanicIsConvertedToControlledFailure(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	activateServer(t, s)
	shard := s.shards[0]
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	corrupted := &lease{
		key:       1,
		id:        id,
		deadline:  testEpoch.Add(time.Second),
		heapIndex: 2,
	}
	shard.leases[corrupted.key] = corrupted
	shard.deadlines = leaseDeadlineHeap{corrupted}
	s.keys.Store(1)

	responses := make(chan protocol.Response, 1)
	if !s.dispatch(context.Background().Done(), shardJob{
		operation: operation{
			requestID: 1,
			kind:      operationRelease,
			key:       corrupted.key,
			leaseID:   id,
		},
		complete: func(response protocol.Response) {
			responses <- response
		},
	}) {
		t.Fatal("dispatch corrupted Release")
	}

	select {
	case response := <-responses:
		if got := response.Status; got != redleasev1.LeaseStatusNOT_READY {
			t.Fatalf("corrupted Release = %s, want NOT_READY", got)
		}
	case <-time.After(time.Second):
		t.Fatal("corrupted Release did not complete")
	}
	select {
	case err := <-s.Fatal():
		if !errors.Is(err, ErrServerFailed) || !strings.Contains(err.Error(), "panic while processing shard operation") {
			t.Fatalf("fatal error = %v, want recovered panic", err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovered panic did not publish fatal error")
	}
}

func TestAcquireClampsMaxUint64(t *testing.T) {
	s := newTestServer(t, 2_000, 1)

	op := operation{
		requestID:      1,
		kind:           operationAcquire,
		key:            1,
		leaseID:        leaseID{clientID: 1, bootID: 2, leaseSeq: 3},
		requestedTTLMS: math.MaxUint64,
	}
	response := s.acquire(s.shards[0], op, testEpoch)
	if response.Status != redleasev1.LeaseStatusOK || response.TTLMS != 2000 {
		t.Fatalf("Acquire = (%s, %d), want (OK, 2000)", response.Status, response.TTLMS)
	}
	wantDeadline := testEpoch.Add(2 * time.Second)
	if got := s.shards[0].leases[1].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("deadline = %v, want %v", got, wantDeadline)
	}
}

func TestAcquireZeroTTLHasNoPositiveValidity(t *testing.T) {
	s := newTestServer(t, 2_000, 1)
	first := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	second := leaseID{clientID: 2, bootID: 2, leaseSeq: 2}

	response := s.acquire(s.shards[0], operation{kind: operationAcquire, key: 1, leaseID: first}, testEpoch)
	if response.Status != redleasev1.LeaseStatusOK || response.TTLMS != 0 {
		t.Fatalf("zero Acquire = (%s, %d), want (OK, 0)", response.Status, response.TTLMS)
	}
	if got := s.keys.Load(); got != 0 {
		t.Fatalf("zero-TTL Acquire reserved %d keys, want 0", got)
	}
	if _, exists := s.shards[0].leases[1]; exists {
		t.Fatal("zero-TTL Acquire stored an immediately expired key")
	}
	response = s.acquire(s.shards[0], operation{kind: operationAcquire, key: 1, leaseID: second, requestedTTLMS: 1}, testEpoch)
	if response.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("Acquire after zero TTL = %s, want OK", response.Status)
	}
}

func TestAcquireEnforcesKeyLimitAndRestoresCapacity(t *testing.T) {
	s, err := New(Config{
		MaxTTL:     1_000,
		MaxKeys:    1,
		Logger:     testLogger,
		ShardCount: 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	shard := s.shards[0]
	firstID := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	secondID := leaseID{clientID: 2, bootID: 2, leaseSeq: 2}

	first := s.acquire(shard, operation{kind: operationAcquire, key: 2, leaseID: firstID, requestedTTLMS: 1000}, testEpoch)
	if first.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("first Acquire = %s, want OK", first.Status)
	}
	repeated := s.acquire(shard, operation{kind: operationAcquire, key: 2, leaseID: firstID, requestedTTLMS: 1000}, testEpoch)
	if repeated.Status != redleasev1.LeaseStatusALREADY_OWNED {
		t.Fatalf("repeated Acquire at limit = %s, want ALREADY_OWNED", repeated.Status)
	}
	limited := s.acquire(shard, operation{kind: operationAcquire, key: 3, leaseID: secondID, requestedTTLMS: 1000}, testEpoch)
	if limited.Status != redleasev1.LeaseStatusKEY_LIMIT_REACHED {
		t.Fatalf("Acquire above key limit = %s, want KEY_LIMIT_REACHED", limited.Status)
	}

	s.release(shard, operation{kind: operationRelease, key: 2, leaseID: firstID}, testEpoch)
	if got := s.keys.Load(); got != 0 {
		t.Fatalf("key count after Release = %d, want 0", got)
	}
	afterRelease := s.acquire(shard, operation{kind: operationAcquire, key: 3, leaseID: secondID, requestedTTLMS: 1000}, testEpoch)
	if afterRelease.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("Acquire after Release = %s, want OK", afterRelease.Status)
	}

	thirdID := leaseID{clientID: 3, bootID: 3, leaseSeq: 3}
	afterCapacityCleanup := s.acquire(shard, operation{kind: operationAcquire, key: 4, leaseID: thirdID, requestedTTLMS: 1000}, testEpoch.Add(time.Second))
	if afterCapacityCleanup.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("Acquire after capacity cleanup = %s, want OK", afterCapacityCleanup.Status)
	}
	if got := s.keys.Load(); got != 1 {
		t.Fatalf("key count after capacity cleanup = %d, want 1", got)
	}
	if _, exists := shard.leases[3]; exists {
		t.Fatal("capacity cleanup kept the expired key")
	}
	if _, exists := shard.leases[4]; !exists {
		t.Fatal("capacity cleanup did not store the new key")
	}
}

func TestCapacityCleanupUsesDeadlineOrderAfterRenew(t *testing.T) {
	s, err := New(Config{
		MaxTTL:     uint64(ProtocolMaxTTL / time.Millisecond),
		MaxKeys:    2,
		Logger:     testLogger,
		ShardCount: 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	shard := s.shards[0]
	firstID := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	secondID := leaseID{clientID: 2, bootID: 2, leaseSeq: 2}
	thirdID := leaseID{clientID: 3, bootID: 3, leaseSeq: 3}

	s.acquire(shard, operation{kind: operationAcquire, key: 2, leaseID: firstID, requestedTTLMS: 1000}, testEpoch)
	s.acquire(shard, operation{kind: operationAcquire, key: 3, leaseID: secondID, requestedTTLMS: 2000}, testEpoch)
	s.renew(shard, operation{kind: operationRenew, key: 2, leaseID: firstID, requestedTTLMS: 5000}, testEpoch.Add(500*time.Millisecond))

	response := s.acquire(shard, operation{kind: operationAcquire, key: 4, leaseID: thirdID, requestedTTLMS: 1000}, testEpoch.Add(2_500*time.Millisecond))
	if response.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("Acquire after deadline-ordered cleanup = %s, want OK", response.Status)
	}
	if _, exists := shard.leases[2]; !exists {
		t.Fatal("cleanup removed the older but renewed lease")
	}
	if _, exists := shard.leases[3]; exists {
		t.Fatal("cleanup kept the newer expired lease")
	}
	if _, exists := shard.leases[4]; !exists {
		t.Fatal("cleanup did not store the new lease")
	}
	if got := s.keys.Load(); got != 2 {
		t.Fatalf("key count after deadline-ordered cleanup = %d, want 2", got)
	}
}

func TestCapacityCleanupReclaimsExpiredLeaseFromAnotherShard(t *testing.T) {
	s, err := New(Config{
		MaxTTL:     1_000,
		MaxKeys:    1,
		Logger:     testLogger,
		ShardCount: 2,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	firstKey := uint64(0)
	firstShard := s.shardIndex(firstKey)
	var secondKey uint64
	for sequence := 1; ; sequence++ {
		candidate := uint64(sequence)
		if s.shardIndex(candidate) != firstShard {
			secondKey = candidate
			break
		}
	}

	firstID := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	secondID := leaseID{clientID: 2, bootID: 2, leaseSeq: 2}
	first := s.acquire(
		s.shards[firstShard],
		operation{kind: operationAcquire, key: firstKey, leaseID: firstID, requestedTTLMS: 1000},
		testEpoch,
	)
	if first.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("first Acquire = %s, want OK", first.Status)
	}

	secondShard := s.shardIndex(secondKey)
	second := s.acquire(
		s.shards[secondShard],
		operation{kind: operationAcquire, key: secondKey, leaseID: secondID, requestedTTLMS: 1000},
		testEpoch.Add(time.Second),
	)
	if second.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("cross-shard Acquire after expiry = %s, want OK", second.Status)
	}
	if _, exists := s.shards[firstShard].leases[firstKey]; exists {
		t.Fatal("capacity cleanup kept expired lease in another shard")
	}
	if _, exists := s.shards[secondShard].leases[secondKey]; !exists {
		t.Fatal("capacity cleanup did not store lease in target shard")
	}
	if got := s.keys.Load(); got != 1 {
		t.Fatalf("key count after cross-shard cleanup = %d, want 1", got)
	}
}

func TestServerAcceptsMaximumUint64Key(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	activateServer(t, s)
	id := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	boundary := uint64(math.MaxUint64)
	response := s.apply(s.shards[0], operation{
		kind:           operationAcquire,
		key:            boundary,
		leaseID:        id,
		requestedTTLMS: 1000,
	})
	if response.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("maximum uint64 key Acquire = %s, want OK", response.Status)
	}
}

func TestAcquireAlreadyOwnedDoesNotExtendDeadline(t *testing.T) {
	s := newTestServer(t, 2_000, 1)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	now := testEpoch

	s.acquire(s.shards[0], operation{kind: operationAcquire, key: 1, leaseID: id, requestedTTLMS: 1000}, now)
	wantDeadline := s.shards[0].leases[1].deadline
	now = now.Add(250 * time.Millisecond)
	response := s.acquire(s.shards[0], operation{kind: operationAcquire, key: 1, leaseID: id, requestedTTLMS: 2000}, now)
	if response.Status != redleasev1.LeaseStatusALREADY_OWNED || response.TTLMS != 750 {
		t.Fatalf("repeated Acquire = (%s, %d), want (ALREADY_OWNED, 750)", response.Status, response.TTLMS)
	}
	if got := s.shards[0].leases[1].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("repeated Acquire changed deadline from %v to %v", wantDeadline, got)
	}

	other := leaseID{clientID: 9, bootID: 9, leaseSeq: 9}
	busy := s.acquire(s.shards[0], operation{kind: operationAcquire, key: 1, leaseID: other, requestedTTLMS: 1000}, now)
	if busy.Status != redleasev1.LeaseStatusBUSY || busy.TTLMS != 0 {
		t.Fatalf("foreign Acquire = (%s, %d), want (BUSY, 0)", busy.Status, busy.TTLMS)
	}
}

func TestRenewExtendsToConfiguredMaximumAndNeverShortens(t *testing.T) {
	s := newTestServer(t, 2_000, 1)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	shard := s.shards[0]
	now := testEpoch

	s.acquire(shard, operation{kind: operationAcquire, key: 1, leaseID: id, requestedTTLMS: 1000}, now)
	now = now.Add(200 * time.Millisecond)
	response := s.renew(shard, operation{kind: operationRenew, key: 1, leaseID: id, requestedTTLMS: math.MaxUint64}, now)
	if response.Status != redleasev1.LeaseStatusOK || response.TTLMS != 2000 {
		t.Fatalf("max Renew = (%s, %d), want (OK, 2000)", response.Status, response.TTLMS)
	}
	wantDeadline := now.Add(2 * time.Second)

	zero := s.renew(shard, operation{kind: operationRenew, key: 1, leaseID: id, requestedTTLMS: 0}, now)
	if zero.Status != redleasev1.LeaseStatusOK || zero.TTLMS != 2000 {
		t.Fatalf("zero Renew = (%s, %d), want (OK, 2000)", zero.Status, zero.TTLMS)
	}
	if got := shard.leases[1].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("zero Renew changed deadline from %v to %v", wantDeadline, got)
	}
}

func TestRenewStaleAndExpiry(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	other := leaseID{clientID: 4, bootID: 5, leaseSeq: 6}
	shard := s.shards[0]

	missing := s.renew(shard, operation{kind: operationRenew, key: 6, leaseID: id, requestedTTLMS: 1000}, testEpoch)
	if missing.Status != redleasev1.LeaseStatusSTALE {
		t.Fatalf("missing Renew = %s, want STALE", missing.Status)
	}
	s.acquire(shard, operation{kind: operationAcquire, key: 1, leaseID: id, requestedTTLMS: 1000}, testEpoch)
	wantDeadline := shard.leases[1].deadline
	foreign := s.renew(shard, operation{kind: operationRenew, key: 1, leaseID: other, requestedTTLMS: 1000}, testEpoch)
	if foreign.Status != redleasev1.LeaseStatusSTALE {
		t.Fatalf("foreign Renew = %s, want STALE", foreign.Status)
	}
	if got := shard.leases[1].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("foreign Renew changed deadline from %v to %v", wantDeadline, got)
	}

	expired := s.renew(shard, operation{kind: operationRenew, key: 1, leaseID: id, requestedTTLMS: 1000}, testEpoch.Add(time.Second))
	if expired.Status != redleasev1.LeaseStatusSTALE {
		t.Fatalf("expired Renew = %s, want STALE", expired.Status)
	}
	if _, exists := shard.leases[1]; exists {
		t.Fatal("expired Renew did not lazily delete lease")
	}
	if got := s.keys.Load(); got != 0 {
		t.Fatalf("key count after expired Renew = %d, want 0", got)
	}
}

func TestMissingAndExpiredLeaseOperationsAreLogged(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s, err := New(Config{
		MaxTTL:                1_000,
		Logger:                logger,
		SkipRestartQuarantine: true,
		ShardCount:            1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	output.Reset() // Ignore the startup lifecycle record in this operation test.

	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	shard := s.shards[0]
	s.renew(shard, operation{
		requestID:      11,
		kind:           operationRenew,
		key:            7,
		leaseID:        id,
		requestedTTLMS: 1_000,
	}, testEpoch)
	s.acquire(shard, operation{
		kind:           operationAcquire,
		key:            8,
		leaseID:        id,
		requestedTTLMS: 1_000,
	}, testEpoch)
	s.renew(shard, operation{
		requestID:      12,
		kind:           operationRenew,
		key:            8,
		leaseID:        id,
		requestedTTLMS: 1_000,
	}, testEpoch.Add(time.Second))
	s.release(shard, operation{
		requestID: 13,
		kind:      operationRelease,
		key:       9,
		leaseID:   id,
	}, testEpoch)
	s.acquire(shard, operation{
		kind:           operationAcquire,
		key:            10,
		leaseID:        id,
		requestedTTLMS: 1_000,
	}, testEpoch)
	s.release(shard, operation{
		requestID: 14,
		kind:      operationRelease,
		key:       10,
		leaseID:   id,
	}, testEpoch.Add(time.Second))

	type record struct {
		Level       string  `json:"level"`
		Message     string  `json:"msg"`
		Component   string  `json:"component"`
		Operation   string  `json:"operation"`
		Reason      string  `json:"reason"`
		Key         uint64  `json:"key"`
		RequestID   uint64  `json:"request_id"`
		ClientID    uint64  `json:"client_id"`
		BootID      uint64  `json:"boot_id"`
		LeaseSeq    uint64  `json:"lease_seq"`
		ExpiredByMS *uint64 `json:"expired_by_ms"`
	}
	var records []record
	decoder := json.NewDecoder(&output)
	for {
		var current record
		if err := decoder.Decode(&current); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode log record: %v", err)
		}
		records = append(records, current)
	}

	want := []record{
		{Level: "WARN", Message: "renew rejected because lease does not exist", Operation: "renew", Reason: "not_found", Key: 7, RequestID: 11},
		{Level: "WARN", Message: "renew rejected because lease has expired", Operation: "renew", Reason: "expired", Key: 8, RequestID: 12},
		{Level: "WARN", Message: "release found an expired lease", Operation: "release", Reason: "expired", Key: 10, RequestID: 14},
	}
	if len(records) != len(want) {
		t.Fatalf("log record count = %d, want %d; output:\n%s", len(records), len(want), output.String())
	}
	for index := range want {
		got := records[index]
		expected := want[index]
		if got.Level != expected.Level || got.Message != expected.Message ||
			got.Operation != expected.Operation || got.Reason != expected.Reason ||
			got.Key != expected.Key || got.RequestID != expected.RequestID {
			t.Errorf("record %d = %+v, want matching %+v", index, got, expected)
		}
		if got.Component != "redlease-server" || got.ClientID != 1 || got.BootID != 2 || got.LeaseSeq != 3 {
			t.Errorf("record %d lacks lease context: %+v", index, got)
		}
		if expected.Reason == "expired" && got.ExpiredByMS == nil {
			t.Errorf("record %d lacks expired_by_ms: %+v", index, got)
		}
	}
}

func TestReleaseIsIdempotentAndDeletesOnlyMatchingLease(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	other := leaseID{clientID: 4, bootID: 5, leaseSeq: 6}
	shard := s.shards[0]
	s.acquire(shard, operation{kind: operationAcquire, key: 1, leaseID: id, requestedTTLMS: 1000}, testEpoch)

	foreign := s.release(shard, operation{kind: operationRelease, key: 1, leaseID: other}, testEpoch)
	if foreign.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("foreign Release = %s, want OK", foreign.Status)
	}
	if _, exists := shard.leases[1]; !exists {
		t.Fatal("foreign Release deleted lease")
	}
	if got := s.keys.Load(); got != 1 {
		t.Fatalf("foreign Release changed key count to %d, want 1", got)
	}

	matching := s.release(shard, operation{kind: operationRelease, key: 1, leaseID: id}, testEpoch)
	if matching.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("matching Release = %s, want OK", matching.Status)
	}
	if _, exists := shard.leases[1]; exists {
		t.Fatal("matching Release did not delete lease")
	}
	if got := s.keys.Load(); got != 0 {
		t.Fatalf("matching Release left key count at %d, want 0", got)
	}
	if len(shard.deadlines) != 0 {
		t.Fatalf("matching Release left %d heap entries, want 0", len(shard.deadlines))
	}

	missing := s.release(shard, operation{kind: operationRelease, key: 1, leaseID: id}, testEpoch)
	if missing.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("missing Release = %s, want OK", missing.Status)
	}
}

func TestRemainingTTLExpiresAndClamps(t *testing.T) {
	t.Parallel()
	if got := remainingTTLMS(testEpoch.Add(time.Millisecond), testEpoch, 10); got != 1 {
		t.Fatalf("remaining TTL = %d, want 1", got)
	}
	if got := remainingTTLMS(testEpoch.Add(20*time.Millisecond), testEpoch, 10); got != 10 {
		t.Fatalf("clamped TTL = %d, want 10", got)
	}
	if got := remainingTTLMS(testEpoch, testEpoch, 10); got != 0 {
		t.Fatalf("expired TTL = %d, want 0", got)
	}
}

func TestDeleteExpiredLeases(t *testing.T) {
	shard := &leaseShard{leases: make(map[uint64]*lease)}
	shard.addLease(11, leaseID{}, testEpoch.Add(time.Millisecond))
	shard.addLease(12, leaseID{}, testEpoch.Add(-time.Millisecond))
	shard.addLease(13, leaseID{}, testEpoch)

	if deleted := shard.removeExpiredLeases(testEpoch); deleted != 2 {
		t.Fatalf("deleted leases = %d, want 2", deleted)
	}

	if _, exists := shard.leases[12]; exists {
		t.Fatal("expired lease was not deleted")
	}
	if _, exists := shard.leases[13]; exists {
		t.Fatal("lease at deadline boundary was not deleted")
	}
	if _, exists := shard.leases[11]; !exists {
		t.Fatal("active lease was deleted")
	}
	if len(shard.deadlines) != 1 || shard.deadlines[0] != shard.leases[11] {
		t.Fatalf("deadline heap is inconsistent after cleanup: %+v", shard.deadlines)
	}
}

func TestConnectionRejectsRequestDuringQuarantine(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	connection, errDone := newTestConnection(t, s)

	if err := sendClientRequest(connection, acquireRequest(1, 1, 1)); err != nil {
		t.Fatalf("send Acquire: %v", err)
	}
	response, err := connection.Recv()
	if err != nil {
		t.Fatalf("receive Acquire: %v", err)
	}
	if response.Status != redleasev1.LeaseStatusNOT_READY {
		t.Fatalf("Acquire received during quarantine = %s, want NOT_READY", response.Status)
	}
	closeTestConnection(t, connection, errDone)
}

func TestActiveConnectionsMetricTracksConnectionLifetime(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	connection, errDone := newTestConnection(t, s)

	deadline := time.Now().Add(time.Second)
	for s.MetricsSnapshot().ActiveConnections != 1 {
		if time.Now().After(deadline) {
			t.Fatal("active connection was not reflected in metrics")
		}
		time.Sleep(time.Millisecond)
	}

	closeTestConnection(t, connection, errDone)
	if got := s.MetricsSnapshot().ActiveConnections; got != 0 {
		t.Fatalf("active connections after EOF = %d, want 0", got)
	}
}

func TestConnectionPreservesSameKeyFIFO(t *testing.T) {
	s := newTestServer(t, 1_000, 2)
	activateServer(t, s)

	key := uint64(1)
	shard := s.shards[s.shardIndex(key)]
	unblockShard := blockShard(t, shard, key)
	connection, errDone := newTestConnection(t, s)
	if err := sendClientRequest(connection, acquireRequest(1, key, 1)); err != nil {
		t.Fatalf("send first Acquire: %v", err)
	}
	if err := sendClientRequest(connection, acquireRequest(2, key, 1)); err != nil {
		t.Fatalf("send second Acquire: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for len(shard.jobs) != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("same-key requests queued = %d, want 2", len(shard.jobs))
		}
		time.Sleep(time.Millisecond)
	}
	unblockShard()
	firstResponse, err := connection.Recv()
	if err != nil {
		t.Fatalf("receive first response: %v", err)
	}
	secondResponse, err := connection.Recv()
	if err != nil {
		t.Fatalf("receive second response: %v", err)
	}
	if firstResponse.RequestID != 1 || firstResponse.Status != redleasev1.LeaseStatusOK {
		t.Fatalf("first response = (%d, %s), want (1, OK)", firstResponse.RequestID, firstResponse.Status)
	}
	if secondResponse.RequestID != 2 || secondResponse.Status != redleasev1.LeaseStatusALREADY_OWNED {
		t.Fatalf("second response = (%d, %s), want (2, ALREADY_OWNED)", secondResponse.RequestID, secondResponse.Status)
	}
	closeTestConnection(t, connection, errDone)
}

func TestConnectionCanReplyOutOfOrderAcrossShards(t *testing.T) {
	s := newTestServer(t, 1_000, 2)
	activateServer(t, s)

	firstKey, secondKey := keysForDifferentShards(t, s)
	unblockFirstShard := blockShard(
		t,
		s.shards[s.shardIndex(firstKey)],
		firstKey,
	)
	connection, errDone := newTestConnection(t, s)
	if err := sendClientRequest(connection, acquireRequest(1, firstKey, 1)); err != nil {
		t.Fatalf("send first Acquire: %v", err)
	}
	if err := sendClientRequest(connection, acquireRequest(2, secondKey, 2)); err != nil {
		t.Fatalf("send second Acquire: %v", err)
	}

	response, err := connection.Recv()
	if err != nil {
		t.Fatalf("receive second-shard response: %v", err)
	}
	if response.RequestID != 2 {
		t.Fatalf("first response request_id = %d, want 2", response.RequestID)
	}
	unblockFirstShard()
	response, err = connection.Recv()
	if err != nil {
		t.Fatalf("receive first-shard response: %v", err)
	}
	if response.RequestID != 1 {
		t.Fatalf("second response request_id = %d, want 1", response.RequestID)
	}
	closeTestConnection(t, connection, errDone)
}

type writeCountingConn struct {
	net.Conn

	writes int
}

func (c *writeCountingConn) Write(value []byte) (int, error) {
	c.writes++
	return c.Conn.Write(value)
}

func TestConnectionResponseWriterFlushesAvailableResponsesAsOneBatch(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	activateServer(t, s)

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	countingConn := &writeCountingConn{Conn: serverConn}
	session := &connectionSession{
		server:         s,
		conn:           countingConn,
		ctx:            t.Context(),
		responses:      mpscring.New[*outboundResponse](),
		responsesReady: make(chan struct{}, 1),
		responsesDone:  make(chan struct{}),
		slots:          make(chan struct{}, 2),
		recvDone:       make(chan error),
	}
	session.slots <- struct{}{}
	session.slots <- struct{}{}
	first, err := s.newOutboundResponse(acquireResponse(1, redleasev1.LeaseStatusOK, 1_000))
	if err != nil {
		t.Fatalf("encode Acquire response: %v", err)
	}
	second, err := s.newOutboundResponse(releaseResponse(2, redleasev1.LeaseStatusOK))
	if err != nil {
		t.Fatalf("encode Release response: %v", err)
	}
	if !session.responses.TryEnqueue(first) || !session.responses.TryEnqueue(second) {
		t.Fatal("enqueue response failed")
	}
	session.responsesReady <- struct{}{}

	done := make(chan error, 1)
	go func() {
		done <- session.writeResponses(transport.NewFrameWriter(countingConn))
	}()
	client := transport.NewConnection(clientConn)
	for requestID := uint64(1); requestID <= 2; requestID++ {
		response, err := client.Recv()
		if err != nil {
			t.Fatalf("receive response %d: %v", requestID, err)
		}
		if response.RequestID != requestID {
			t.Fatalf("response request ID = %d, want %d", response.RequestID, requestID)
		}
	}
	close(session.responsesDone)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write responses: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("response writer did not stop")
	}
	if countingConn.writes != 1 {
		t.Fatalf("TCP writes = %d, want 1", countingConn.writes)
	}
}

func blockShard(t *testing.T, shard *leaseShard, key uint64) func() {
	t.Helper()
	started := make(chan struct{})
	unblock := make(chan struct{})
	var unblockOnce sync.Once
	release := func() { unblockOnce.Do(func() { close(unblock) }) }
	t.Cleanup(release)

	shard.jobs <- shardJob{
		operation: operation{kind: operationRelease, key: key},
		complete: func(protocol.Response) {
			close(started)
			<-unblock
		},
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("shard worker did not start blocker")
	}
	return release
}

func keysForDifferentShards(t *testing.T, s *Server) (uint64, uint64) {
	t.Helper()
	first := uint64(0)
	firstShard := s.shardIndex(first)
	for i := 1; i < 1000; i++ {
		candidate := uint64(i)
		if s.shardIndex(candidate) != firstShard {
			return first, candidate
		}
	}
	t.Fatal("could not find keys in different shards")
	return 0, 0
}

func newTestConnection(t *testing.T, s *Server) (*transport.Connection, <-chan error) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	errDone := make(chan error, 1)
	go func() {
		errDone <- s.serveConnection(serverConn)
		_ = serverConn.Close()
	}()
	return transport.NewConnection(clientConn), errDone
}

func closeTestConnection(t *testing.T, connection *transport.Connection, errDone <-chan error) {
	t.Helper()
	if err := connection.Close(); err != nil {
		t.Fatalf("close test connection: %v", err)
	}
	select {
	case err := <-errDone:
		if err != nil {
			t.Fatalf("serve connection: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("connection did not finish after close")
	}
}

func sendClientRequest(
	connection *transport.Connection,
	request *redleasev1.ClientRequest,
) error {
	if err := connection.BufferClientRequest(request); err != nil {
		return err
	}
	return connection.FlushClientRequests()
}

func acquireRequest(requestID, key, sequence uint64) *redleasev1.ClientRequest {
	builder := flatbuffers.NewBuilder(protocol.NewBuilderSize)
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddRequestId(builder, requestID)
	redleasev1.ClientRequestAddOperation(builder, redleasev1.ClientOperationACQUIRE)
	redleasev1.ClientRequestAddAcquire(builder, redleasev1.CreateAcquireRequest(
		builder,
		key,
		1,
		1,
		sequence,
		1000,
	))
	return finishTestClientRequest(builder)
}

func getTTLRequest(requestID uint64) *redleasev1.ClientRequest {
	builder := flatbuffers.NewBuilder(protocol.NewBuilderSize)
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddRequestId(builder, requestID)
	redleasev1.ClientRequestAddOperation(builder, redleasev1.ClientOperationGET_TTL)
	return finishTestClientRequest(builder)
}

func requestWithoutPayload(operation redleasev1.ClientOperation) *redleasev1.ClientRequest {
	builder := flatbuffers.NewBuilder(protocol.NewBuilderSize)
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddOperation(builder, operation)
	return finishTestClientRequest(builder)
}

func finishTestClientRequest(builder *flatbuffers.Builder) *redleasev1.ClientRequest {
	root := redleasev1.ClientRequestEnd(builder)
	redleasev1.FinishSizePrefixedClientRequestBuffer(builder, root)
	return redleasev1.GetSizePrefixedRootAsClientRequest(builder.FinishedBytes(), 0)
}

func TestDecodeInvalidRequest(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	for _, request := range []*redleasev1.ClientRequest{
		requestWithoutPayload(redleasev1.ClientOperationACQUIRE),
		requestWithoutPayload(redleasev1.ClientOperationRENEW),
		requestWithoutPayload(redleasev1.ClientOperationRELEASE),
		requestWithoutPayload(redleasev1.ClientOperation(255)),
	} {
		_, _, _, err := s.decodeRequest(request.Table().Bytes)
		if !errors.Is(err, protocol.ErrMalformedFrame) {
			t.Fatalf("decodeRequest(%v) error = %v, want ErrMalformedFrame", request, err)
		}
	}
	for _, frame := range [][]byte{
		nil,
		{1, 0, 0, 0, 0},
		{255, 0, 0, 0, 0, 0, 0, 0},
		{4, 0, 0, 0, 255, 255, 255, 127},
	} {
		_, _, _, err := s.decodeRequest(frame)
		if !errors.Is(err, protocol.ErrMalformedFrame) {
			t.Fatalf("decodeRequest(%x) error = %v, want ErrMalformedFrame", frame, err)
		}
	}
}

func TestDecodeRequestOwnsScalarsAfterReceiveBufferReuse(t *testing.T) {
	s := newTestServer(t, 1_000, 1)
	frame := acquireRequest(21, 22, 23).Table().Bytes
	op, _, direct, err := s.decodeRequest(frame)
	if err != nil || direct {
		t.Fatalf("decode Acquire: operation=%+v direct=%t error=%v", op, direct, err)
	}
	clear(frame)
	if op.requestID != 21 || op.kind != operationAcquire || op.key != 22 ||
		op.leaseID != (leaseID{clientID: 1, bootID: 1, leaseSeq: 23}) ||
		op.requestedTTLMS != 1_000 {
		t.Fatalf("decoded operation changed after frame reuse: %+v", op)
	}
}
