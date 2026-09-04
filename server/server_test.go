package server

import (
	"context"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var testEpoch = time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)

func newTestServer(t *testing.T, maxTTL time.Duration, shardCount uint32) *Server {
	t.Helper()
	s, err := New(Config{
		ConfiguredMaxTTL:     maxTTL,
		ShardCount:           shardCount,
		ShardQueueDepth:      8,
		MaxInFlightPerStream: 8,
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
		{name: "minimum", config: Config{ConfiguredMaxTTL: time.Millisecond}},
		{name: "protocol maximum", config: Config{ConfiguredMaxTTL: ProtocolMaxTTL}},
		{name: "zero TTL", config: Config{}, wantErr: true},
		{name: "negative TTL", config: Config{ConfiguredMaxTTL: -time.Millisecond}, wantErr: true},
		{name: "over protocol maximum", config: Config{ConfiguredMaxTTL: ProtocolMaxTTL + time.Millisecond}, wantErr: true},
		{name: "sub-millisecond", config: Config{ConfiguredMaxTTL: time.Millisecond + time.Nanosecond}, wantErr: true},
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
	config, err := resolveConfig(Config{ConfiguredMaxTTL: time.Second})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if config.maxKeys != DefaultMaxKeys {
		t.Fatalf("max keys = %d, want %d", config.maxKeys, DefaultMaxKeys)
	}
}

func TestQuarantineAndGetTTL(t *testing.T) {
	s := newTestServer(t, 2*time.Second, 1)
	shard := s.shards[0]
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}

	acquire := s.apply(shard, operation{requestID: 10, kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000})
	if got := acquire.GetAcquire().GetStatus(); got != redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY {
		t.Fatalf("Acquire during quarantine = %s", got)
	}
	renew := s.apply(shard, operation{requestID: 11, kind: operationRenew, key: "key", leaseID: id, requestedTTLMS: 1000})
	if got := renew.GetRenew().GetStatus(); got != redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY {
		t.Fatalf("Renew during quarantine = %s", got)
	}
	release := s.apply(shard, operation{requestID: 12, kind: operationRelease, key: "key", leaseID: id})
	if got := release.GetRelease().GetStatus(); got != redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY {
		t.Fatalf("Release during quarantine = %s", got)
	}

	_, getTTL, err := s.decodeRequest(getTTLRequest(13))
	if err != nil {
		t.Fatalf("decode GetTTL: %v", err)
	}
	if got := getTTL.GetGetTtl().GetConfiguredMaxTtlMs(); got != 2000 {
		t.Fatalf("GetTTL during quarantine = %d, want 2000", got)
	}

	activateServer(t, s)
	acquire = s.apply(shard, operation{requestID: 14, kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000})
	if got := acquire.GetAcquire().GetStatus(); got != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("Acquire after quarantine = %s", got)
	}
}

func TestAcquireClampsMaxUint64BeforeDurationConversion(t *testing.T) {
	s := newTestServer(t, 2*time.Second, 1)

	op := operation{
		requestID:      1,
		kind:           operationAcquire,
		key:            "key",
		leaseID:        leaseID{clientID: 1, bootID: 2, leaseSeq: 3},
		requestedTTLMS: math.MaxUint64,
	}
	response := s.acquire(s.shards[0], op, testEpoch).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK || response.GetTtlMs() != 2000 {
		t.Fatalf("Acquire = (%s, %d), want (OK, 2000)", response.GetStatus(), response.GetTtlMs())
	}
	wantDeadline := testEpoch.Add(2 * time.Second)
	if got := s.shards[0].leases["key"].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("deadline = %s, want %s", got, wantDeadline)
	}
}

func TestAcquireZeroTTLHasNoPositiveValidity(t *testing.T) {
	s := newTestServer(t, 2*time.Second, 1)
	first := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	second := leaseID{clientID: 2, bootID: 2, leaseSeq: 2}

	response := s.acquire(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: first}, testEpoch).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK || response.GetTtlMs() != 0 {
		t.Fatalf("zero Acquire = (%s, %d), want (OK, 0)", response.GetStatus(), response.GetTtlMs())
	}
	if got := s.keys.Load(); got != 0 {
		t.Fatalf("zero-TTL Acquire reserved %d keys, want 0", got)
	}
	if _, exists := s.shards[0].leases["key"]; exists {
		t.Fatal("zero-TTL Acquire stored an immediately expired key")
	}
	response = s.acquire(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: second, requestedTTLMS: 1}, testEpoch).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("Acquire after zero TTL = %s, want OK", response.GetStatus())
	}
}

func TestAcquireEnforcesKeyLimitAndRestoresCapacity(t *testing.T) {
	s, err := New(Config{
		ConfiguredMaxTTL: time.Second,
		MaxKeys:          1,
		ShardCount:       1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	shard := s.shards[0]
	firstID := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	secondID := leaseID{clientID: 2, bootID: 2, leaseSeq: 2}

	first := s.acquire(shard, operation{kind: operationAcquire, key: "first", leaseID: firstID, requestedTTLMS: 1000}, testEpoch).GetAcquire()
	if first.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("first Acquire = %s, want OK", first.GetStatus())
	}
	repeated := s.acquire(shard, operation{kind: operationAcquire, key: "first", leaseID: firstID, requestedTTLMS: 1000}, testEpoch).GetAcquire()
	if repeated.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED {
		t.Fatalf("repeated Acquire at limit = %s, want ALREADY_OWNED", repeated.GetStatus())
	}
	limited := s.acquire(shard, operation{kind: operationAcquire, key: "second", leaseID: secondID, requestedTTLMS: 1000}, testEpoch).GetAcquire()
	if limited.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_KEY_LIMIT_REACHED {
		t.Fatalf("Acquire above key limit = %s, want KEY_LIMIT_REACHED", limited.GetStatus())
	}

	s.release(shard, operation{kind: operationRelease, key: "first", leaseID: firstID}, testEpoch)
	if got := s.keys.Load(); got != 0 {
		t.Fatalf("key count after Release = %d, want 0", got)
	}
	afterRelease := s.acquire(shard, operation{kind: operationAcquire, key: "second", leaseID: secondID, requestedTTLMS: 1000}, testEpoch).GetAcquire()
	if afterRelease.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("Acquire after Release = %s, want OK", afterRelease.GetStatus())
	}

	thirdID := leaseID{clientID: 3, bootID: 3, leaseSeq: 3}
	afterCapacityCleanup := s.acquire(shard, operation{kind: operationAcquire, key: "third", leaseID: thirdID, requestedTTLMS: 1000}, testEpoch.Add(time.Second)).GetAcquire()
	if afterCapacityCleanup.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("Acquire after capacity cleanup = %s, want OK", afterCapacityCleanup.GetStatus())
	}
	if got := s.keys.Load(); got != 1 {
		t.Fatalf("key count after capacity cleanup = %d, want 1", got)
	}
	if _, exists := shard.leases["second"]; exists {
		t.Fatal("capacity cleanup kept the expired key")
	}
	if _, exists := shard.leases["third"]; !exists {
		t.Fatal("capacity cleanup did not store the new key")
	}
}

func TestCapacityCleanupUsesDeadlineOrderAfterRenew(t *testing.T) {
	s, err := New(Config{
		ConfiguredMaxTTL: ProtocolMaxTTL,
		MaxKeys:          2,
		ShardCount:       1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	shard := s.shards[0]
	firstID := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	secondID := leaseID{clientID: 2, bootID: 2, leaseSeq: 2}
	thirdID := leaseID{clientID: 3, bootID: 3, leaseSeq: 3}

	s.acquire(shard, operation{kind: operationAcquire, key: "first", leaseID: firstID, requestedTTLMS: 1000}, testEpoch)
	s.acquire(shard, operation{kind: operationAcquire, key: "second", leaseID: secondID, requestedTTLMS: 2000}, testEpoch)
	s.renew(shard, operation{kind: operationRenew, key: "first", leaseID: firstID, requestedTTLMS: 5000}, testEpoch.Add(500*time.Millisecond))

	response := s.acquire(shard, operation{kind: operationAcquire, key: "third", leaseID: thirdID, requestedTTLMS: 1000}, testEpoch.Add(2500*time.Millisecond)).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("Acquire after deadline-ordered cleanup = %s, want OK", response.GetStatus())
	}
	if _, exists := shard.leases["first"]; !exists {
		t.Fatal("cleanup removed the older but renewed lease")
	}
	if _, exists := shard.leases["second"]; exists {
		t.Fatal("cleanup kept the newer expired lease")
	}
	if _, exists := shard.leases["third"]; !exists {
		t.Fatal("cleanup did not store the new lease")
	}
	if got := s.keys.Load(); got != 2 {
		t.Fatalf("key count after deadline-ordered cleanup = %d, want 2", got)
	}
}

func TestCapacityCleanupReclaimsExpiredLeaseFromAnotherShard(t *testing.T) {
	s, err := New(Config{
		ConfiguredMaxTTL: time.Second,
		MaxKeys:          1,
		ShardCount:       2,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	firstKey := "key-0"
	firstShard := s.shardIndex(firstKey)
	secondKey := ""
	for sequence := 1; ; sequence++ {
		candidate := "key-" + strconv.Itoa(sequence)
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
	).GetAcquire()
	if first.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("first Acquire = %s, want OK", first.GetStatus())
	}

	secondShard := s.shardIndex(secondKey)
	second := s.acquire(
		s.shards[secondShard],
		operation{kind: operationAcquire, key: secondKey, leaseID: secondID, requestedTTLMS: 1000},
		testEpoch.Add(time.Second),
	).GetAcquire()
	if second.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("cross-shard Acquire after expiry = %s, want OK", second.GetStatus())
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

func TestServerRejectsKeysLargerThanProtocolLimit(t *testing.T) {
	s := newTestServer(t, time.Second, 1)
	activateServer(t, s)
	id := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	tooLarge := strings.Repeat("x", protocol.MaxKeyBytes+1)

	for _, op := range []operation{
		{kind: operationAcquire, key: tooLarge, leaseID: id, requestedTTLMS: 1000},
		{kind: operationRenew, key: tooLarge, leaseID: id, requestedTTLMS: 1000},
		{kind: operationRelease, key: tooLarge, leaseID: id},
	} {
		response := s.apply(s.shards[0], op)
		var status redleasev1.LeaseStatus
		switch op.kind {
		case operationAcquire:
			status = response.GetAcquire().GetStatus()
		case operationRenew:
			status = response.GetRenew().GetStatus()
		case operationRelease:
			status = response.GetRelease().GetStatus()
		}
		if status != redleasev1.LeaseStatus_LEASE_STATUS_KEY_TOO_LARGE {
			t.Fatalf("operation %d oversized key status = %s, want KEY_TOO_LARGE", op.kind, status)
		}
	}
	if got := s.keys.Load(); got != 0 {
		t.Fatalf("oversized operations reserved %d keys, want 0", got)
	}

	boundary := strings.Repeat("x", protocol.MaxKeyBytes)
	response := s.apply(s.shards[0], operation{
		kind:           operationAcquire,
		key:            boundary,
		leaseID:        id,
		requestedTTLMS: 1000,
	}).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("%d-byte key Acquire = %s, want OK", protocol.MaxKeyBytes, response.GetStatus())
	}
}

func TestAcquireAlreadyOwnedDoesNotExtendDeadline(t *testing.T) {
	s := newTestServer(t, 2*time.Second, 1)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	now := testEpoch

	s.acquire(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000}, now)
	wantDeadline := s.shards[0].leases["key"].deadline
	now = now.Add(250 * time.Millisecond)
	response := s.acquire(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 2000}, now).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED || response.GetTtlMs() != 750 {
		t.Fatalf("repeated Acquire = (%s, %d), want (ALREADY_OWNED, 750)", response.GetStatus(), response.GetTtlMs())
	}
	if got := s.shards[0].leases["key"].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("repeated Acquire changed deadline from %s to %s", wantDeadline, got)
	}

	other := leaseID{clientID: 9, bootID: 9, leaseSeq: 9}
	busy := s.acquire(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: other, requestedTTLMS: 1000}, now).GetAcquire()
	if busy.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_BUSY || busy.GetTtlMs() != 0 {
		t.Fatalf("foreign Acquire = (%s, %d), want (BUSY, 0)", busy.GetStatus(), busy.GetTtlMs())
	}
}

func TestRenewExtendsToConfiguredMaximumAndNeverShortens(t *testing.T) {
	s := newTestServer(t, 2*time.Second, 1)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	shard := s.shards[0]
	now := testEpoch

	s.acquire(shard, operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000}, now)
	now = now.Add(200 * time.Millisecond)
	response := s.renew(shard, operation{kind: operationRenew, key: "key", leaseID: id, requestedTTLMS: math.MaxUint64}, now).GetRenew()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK || response.GetTtlMs() != 2000 {
		t.Fatalf("max Renew = (%s, %d), want (OK, 2000)", response.GetStatus(), response.GetTtlMs())
	}
	wantDeadline := now.Add(2 * time.Second)

	zero := s.renew(shard, operation{kind: operationRenew, key: "key", leaseID: id, requestedTTLMS: 0}, now).GetRenew()
	if zero.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK || zero.GetTtlMs() != 2000 {
		t.Fatalf("zero Renew = (%s, %d), want (OK, 2000)", zero.GetStatus(), zero.GetTtlMs())
	}
	if got := shard.leases["key"].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("zero Renew changed deadline from %s to %s", wantDeadline, got)
	}
}

func TestRenewStaleAndExpiry(t *testing.T) {
	s := newTestServer(t, time.Second, 1)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	other := leaseID{clientID: 4, bootID: 5, leaseSeq: 6}
	shard := s.shards[0]

	missing := s.renew(shard, operation{kind: operationRenew, key: "missing", leaseID: id, requestedTTLMS: 1000}, testEpoch).GetRenew()
	if missing.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_STALE {
		t.Fatalf("missing Renew = %s, want STALE", missing.GetStatus())
	}
	s.acquire(shard, operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000}, testEpoch)
	wantDeadline := shard.leases["key"].deadline
	foreign := s.renew(shard, operation{kind: operationRenew, key: "key", leaseID: other, requestedTTLMS: 1000}, testEpoch).GetRenew()
	if foreign.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_STALE {
		t.Fatalf("foreign Renew = %s, want STALE", foreign.GetStatus())
	}
	if got := shard.leases["key"].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("foreign Renew changed deadline from %s to %s", wantDeadline, got)
	}

	expired := s.renew(shard, operation{kind: operationRenew, key: "key", leaseID: id, requestedTTLMS: 1000}, testEpoch.Add(time.Second)).GetRenew()
	if expired.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_STALE {
		t.Fatalf("expired Renew = %s, want STALE", expired.GetStatus())
	}
	if _, exists := shard.leases["key"]; exists {
		t.Fatal("expired Renew did not lazily delete lease")
	}
	if got := s.keys.Load(); got != 0 {
		t.Fatalf("key count after expired Renew = %d, want 0", got)
	}
}

func TestReleaseIsIdempotentAndDeletesOnlyMatchingLease(t *testing.T) {
	s := newTestServer(t, time.Second, 1)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	other := leaseID{clientID: 4, bootID: 5, leaseSeq: 6}
	shard := s.shards[0]
	s.acquire(shard, operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000}, testEpoch)

	foreign := s.release(shard, operation{kind: operationRelease, key: "key", leaseID: other}, testEpoch).GetRelease()
	if foreign.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("foreign Release = %s, want OK", foreign.GetStatus())
	}
	if _, exists := shard.leases["key"]; !exists {
		t.Fatal("foreign Release deleted lease")
	}
	if got := s.keys.Load(); got != 1 {
		t.Fatalf("foreign Release changed key count to %d, want 1", got)
	}

	matching := s.release(shard, operation{kind: operationRelease, key: "key", leaseID: id}, testEpoch).GetRelease()
	if matching.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("matching Release = %s, want OK", matching.GetStatus())
	}
	if _, exists := shard.leases["key"]; exists {
		t.Fatal("matching Release did not delete lease")
	}
	if got := s.keys.Load(); got != 0 {
		t.Fatalf("matching Release left key count at %d, want 0", got)
	}
	if len(shard.deadlines) != 0 {
		t.Fatalf("matching Release left %d heap entries, want 0", len(shard.deadlines))
	}

	missing := s.release(shard, operation{kind: operationRelease, key: "key", leaseID: id}, testEpoch).GetRelease()
	if missing.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("missing Release = %s, want OK", missing.GetStatus())
	}
}

func TestRemainingTTLFloorsAndClamps(t *testing.T) {
	t.Parallel()
	if got := remainingTTLMS(testEpoch.Add(time.Millisecond+999*time.Microsecond), testEpoch, 10); got != 1 {
		t.Fatalf("floored TTL = %d, want 1", got)
	}
	if got := remainingTTLMS(testEpoch.Add(20*time.Millisecond), testEpoch, 10); got != 10 {
		t.Fatalf("clamped TTL = %d, want 10", got)
	}
	if got := remainingTTLMS(testEpoch, testEpoch, 10); got != 0 {
		t.Fatalf("expired TTL = %d, want 0", got)
	}
}

func TestDeleteExpiredLeases(t *testing.T) {
	shard := &leaseShard{leases: make(map[string]*lease)}
	shard.addLease("active", leaseID{}, testEpoch.Add(time.Millisecond))
	shard.addLease("expired", leaseID{}, testEpoch.Add(-time.Millisecond))
	shard.addLease("boundary", leaseID{}, testEpoch)

	if deleted := shard.removeExpiredLeases(testEpoch); deleted != 2 {
		t.Fatalf("deleted leases = %d, want 2", deleted)
	}

	if _, exists := shard.leases["expired"]; exists {
		t.Fatal("expired lease was not deleted")
	}
	if _, exists := shard.leases["boundary"]; exists {
		t.Fatal("lease at deadline boundary was not deleted")
	}
	if _, exists := shard.leases["active"]; !exists {
		t.Fatal("active lease was deleted")
	}
	if len(shard.deadlines) != 1 || shard.deadlines[0] != shard.leases["active"] {
		t.Fatalf("deadline heap is inconsistent after cleanup: %+v", shard.deadlines)
	}
}

func TestLeaseStreamRejectsRequestDuringQuarantine(t *testing.T) {
	s := newTestServer(t, time.Second, 1)

	stream := newFakeLeaseStream(acquireRequest(1, []byte("key"), 1, 1000))
	errDone := make(chan error, 1)
	go func() { errDone <- s.LeaseStream(stream) }()

	select {
	case response := <-stream.sent:
		if got := response.GetAcquire().GetStatus(); got != redleasev1.LeaseStatus_LEASE_STATUS_NOT_READY {
			t.Fatalf("Acquire received during quarantine = %s, want NOT_READY", got)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not send quarantine response")
	}
	select {
	case err := <-errDone:
		if err != nil {
			t.Fatalf("LeaseStream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("LeaseStream did not finish after EOF")
	}
}

func TestLeaseStreamPreservesSameKeyFIFO(t *testing.T) {
	s := newTestServer(t, time.Second, 2)
	activateServer(t, s)

	key := []byte("same-key")
	shard := s.shards[s.shardIndex(string(key))]
	unblockShard := blockShard(t, shard, string(key))
	stream := newFakeLeaseStream(
		acquireRequest(1, key, 1, 1000),
		acquireRequest(2, key, 1, 1000),
	)
	errDone := make(chan error, 1)
	go func() { errDone <- s.LeaseStream(stream) }()

	deadline := time.Now().Add(time.Second)
	for len(shard.jobs) != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("same-key requests queued = %d, want 2", len(shard.jobs))
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case response := <-stream.sent:
		t.Fatalf("response %d arrived while the shard was blocked", response.GetRequestId())
	default:
	}

	unblockShard()
	firstResponse := <-stream.sent
	secondResponse := <-stream.sent
	if firstResponse.GetRequestId() != 1 || firstResponse.GetAcquire().GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("first response = (%d, %s), want (1, OK)", firstResponse.GetRequestId(), firstResponse.GetAcquire().GetStatus())
	}
	if secondResponse.GetRequestId() != 2 || secondResponse.GetAcquire().GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED {
		t.Fatalf("second response = (%d, %s), want (2, ALREADY_OWNED)", secondResponse.GetRequestId(), secondResponse.GetAcquire().GetStatus())
	}
	select {
	case err := <-errDone:
		if err != nil {
			t.Fatalf("LeaseStream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("LeaseStream did not finish after EOF")
	}
}

func TestLeaseStreamCanReplyOutOfOrderAcrossShards(t *testing.T) {
	s := newTestServer(t, time.Second, 2)
	activateServer(t, s)

	firstKey, secondKey := keysForDifferentShards(t, s)
	unblockFirstShard := blockShard(
		t,
		s.shards[s.shardIndex(string(firstKey))],
		string(firstKey),
	)
	stream := newFakeLeaseStream(
		acquireRequest(1, firstKey, 1, 1000),
		acquireRequest(2, secondKey, 2, 1000),
	)
	errDone := make(chan error, 1)
	go func() { errDone <- s.LeaseStream(stream) }()

	select {
	case response := <-stream.sent:
		if response.GetRequestId() != 2 {
			t.Fatalf("first response request_id = %d, want 2", response.GetRequestId())
		}
	case <-time.After(time.Second):
		t.Fatal("second shard did not respond while first shard was blocked")
	}
	unblockFirstShard()
	select {
	case response := <-stream.sent:
		if response.GetRequestId() != 1 {
			t.Fatalf("second response request_id = %d, want 1", response.GetRequestId())
		}
	case <-time.After(time.Second):
		t.Fatal("first shard did not respond after it was unblocked")
	}
	select {
	case err := <-errDone:
		if err != nil {
			t.Fatalf("LeaseStream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("LeaseStream did not finish after EOF")
	}
}

func blockShard(t *testing.T, shard *leaseShard, key string) func() {
	t.Helper()
	started := make(chan struct{})
	unblock := make(chan struct{})
	var unblockOnce sync.Once
	release := func() { unblockOnce.Do(func() { close(unblock) }) }
	t.Cleanup(release)

	shard.jobs <- shardJob{
		operation: operation{kind: operationRelease, key: key},
		complete: func(*redleasev1.ServerResponse) {
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

func keysForDifferentShards(t *testing.T, s *Server) ([]byte, []byte) {
	t.Helper()
	first := []byte("key-0")
	firstShard := s.shardIndex(string(first))
	for i := 1; i < 1000; i++ {
		candidate := []byte("key-" + strconv.Itoa(i))
		if s.shardIndex(string(candidate)) != firstShard {
			return first, candidate
		}
	}
	t.Fatal("could not find keys in different shards")
	return nil, nil
}

type fakeLeaseStream struct {
	ctx      context.Context
	requests chan *redleasev1.ClientRequest
	sent     chan *redleasev1.ServerResponse
}

func newFakeLeaseStream(requests ...*redleasev1.ClientRequest) *fakeLeaseStream {
	input := make(chan *redleasev1.ClientRequest, len(requests))
	for _, request := range requests {
		input <- request
	}
	close(input)
	return &fakeLeaseStream{
		ctx:      context.Background(),
		requests: input,
		sent:     make(chan *redleasev1.ServerResponse, len(requests)),
	}
}

func (s *fakeLeaseStream) Recv() (*redleasev1.ClientRequest, error) {
	request, ok := <-s.requests
	if !ok {
		return nil, io.EOF
	}
	return request, nil
}

func (s *fakeLeaseStream) Send(response *redleasev1.ServerResponse) error {
	s.sent <- response
	return nil
}

func (s *fakeLeaseStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeLeaseStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeLeaseStream) SetTrailer(metadata.MD)       {}
func (s *fakeLeaseStream) Context() context.Context     { return s.ctx }
func (s *fakeLeaseStream) SendMsg(any) error            { return nil }
func (s *fakeLeaseStream) RecvMsg(any) error            { return nil }

func acquireRequest(requestID uint64, key []byte, sequence, ttlMS uint64) *redleasev1.ClientRequest {
	return &redleasev1.ClientRequest{
		RequestId: requestID,
		Operation: &redleasev1.ClientRequest_Acquire{Acquire: &redleasev1.AcquireRequest{
			Key: key,
			LeaseId: &redleasev1.LeaseID{
				ClientId: 1,
				BootId:   1,
				LeaseSeq: sequence,
			},
			RequestedTtlMs: ttlMS,
		}},
	}
}

func getTTLRequest(requestID uint64) *redleasev1.ClientRequest {
	return &redleasev1.ClientRequest{
		RequestId: requestID,
		Operation: &redleasev1.ClientRequest_GetTtl{GetTtl: &redleasev1.GetTTLRequest{}},
	}
}

func TestDecodeInvalidRequest(t *testing.T) {
	s := newTestServer(t, time.Second, 1)
	for _, request := range []*redleasev1.ClientRequest{nil, {}, {Operation: &redleasev1.ClientRequest_Acquire{}}} {
		_, _, err := s.decodeRequest(request)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("decodeRequest(%v) error = %v, want InvalidArgument", request, err)
		}
	}
}
