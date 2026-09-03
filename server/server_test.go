package server

import (
	"context"
	"io"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var testEpoch = time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestServer(t *testing.T, maxTTL time.Duration, shardCount int) (*Server, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: testEpoch}
	s, err := newWithDependencies(Config{
		ConfiguredMaxTTL:     maxTTL,
		ShardCount:           shardCount,
		ShardQueueDepth:      8,
		MaxInFlightPerStream: 8,
	}, dependencies{now: clock.Now, quarantineDelay: time.Hour})
	if err != nil {
		t.Fatalf("newWithDependencies: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s, clock
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
		{name: "negative shards", config: Config{ConfiguredMaxTTL: time.Second, ShardCount: -1}, wantErr: true},
		{name: "negative queue", config: Config{ConfiguredMaxTTL: time.Second, ShardQueueDepth: -1}, wantErr: true},
		{name: "negative inflight", config: Config{ConfiguredMaxTTL: time.Second, MaxInFlightPerStream: -1}, wantErr: true},
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

func TestQuarantineEndsWhenTimerFires(t *testing.T) {
	s, err := newWithDependencies(Config{
		ConfiguredMaxTTL:     time.Second,
		ShardCount:           1,
		ShardQueueDepth:      1,
		MaxInFlightPerStream: 1,
	}, dependencies{
		now:             (&fakeClock{now: testEpoch}).Now,
		quarantineDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("newWithDependencies: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	deadline := time.Now().Add(time.Second)
	for !s.active() {
		if time.Now().After(deadline) {
			t.Fatal("ordinary quarantine timer did not activate server")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestQuarantineAndGetTTL(t *testing.T) {
	s, _ := newTestServer(t, 2*time.Second, 1)
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
	s, clock := newTestServer(t, 2*time.Second, 1)
	activateServer(t, s)

	op := operation{
		requestID:      1,
		kind:           operationAcquire,
		key:            "key",
		leaseID:        leaseID{clientID: 1, bootID: 2, leaseSeq: 3},
		requestedTTLMS: math.MaxUint64,
	}
	response := s.apply(s.shards[0], op).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK || response.GetTtlMs() != 2000 {
		t.Fatalf("Acquire = (%s, %d), want (OK, 2000)", response.GetStatus(), response.GetTtlMs())
	}
	wantDeadline := clock.Now().Round(0).Add(2 * time.Second)
	if got := s.shards[0].leases["key"].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("deadline = %s, want %s", got, wantDeadline)
	}
}

func TestAcquireZeroTTLHasNoPositiveValidity(t *testing.T) {
	s, _ := newTestServer(t, 2*time.Second, 1)
	activateServer(t, s)
	first := leaseID{clientID: 1, bootID: 1, leaseSeq: 1}
	second := leaseID{clientID: 2, bootID: 2, leaseSeq: 2}

	response := s.apply(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: first}).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK || response.GetTtlMs() != 0 {
		t.Fatalf("zero Acquire = (%s, %d), want (OK, 0)", response.GetStatus(), response.GetTtlMs())
	}
	response = s.apply(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: second, requestedTTLMS: 1}).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("Acquire after zero TTL = %s, want OK", response.GetStatus())
	}
}

func TestAcquireAlreadyOwnedDoesNotExtendDeadline(t *testing.T) {
	s, clock := newTestServer(t, 2*time.Second, 1)
	activateServer(t, s)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}

	s.apply(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000})
	wantDeadline := s.shards[0].leases["key"].deadline
	clock.Advance(250 * time.Millisecond)
	response := s.apply(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 2000}).GetAcquire()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED || response.GetTtlMs() != 750 {
		t.Fatalf("repeated Acquire = (%s, %d), want (ALREADY_OWNED, 750)", response.GetStatus(), response.GetTtlMs())
	}
	if got := s.shards[0].leases["key"].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("repeated Acquire changed deadline from %s to %s", wantDeadline, got)
	}

	other := leaseID{clientID: 9, bootID: 9, leaseSeq: 9}
	busy := s.apply(s.shards[0], operation{kind: operationAcquire, key: "key", leaseID: other, requestedTTLMS: 1000}).GetAcquire()
	if busy.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_BUSY || busy.GetTtlMs() != 0 {
		t.Fatalf("foreign Acquire = (%s, %d), want (BUSY, 0)", busy.GetStatus(), busy.GetTtlMs())
	}
}

func TestRenewExtendsToConfiguredMaximumAndNeverShortens(t *testing.T) {
	s, clock := newTestServer(t, 2*time.Second, 1)
	activateServer(t, s)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	shard := s.shards[0]

	s.apply(shard, operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000})
	clock.Advance(200 * time.Millisecond)
	response := s.apply(shard, operation{kind: operationRenew, key: "key", leaseID: id, requestedTTLMS: math.MaxUint64}).GetRenew()
	if response.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK || response.GetTtlMs() != 2000 {
		t.Fatalf("max Renew = (%s, %d), want (OK, 2000)", response.GetStatus(), response.GetTtlMs())
	}
	wantDeadline := clock.Now().Round(0).Add(2 * time.Second)

	zero := s.apply(shard, operation{kind: operationRenew, key: "key", leaseID: id, requestedTTLMS: 0}).GetRenew()
	if zero.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK || zero.GetTtlMs() != 2000 {
		t.Fatalf("zero Renew = (%s, %d), want (OK, 2000)", zero.GetStatus(), zero.GetTtlMs())
	}
	if got := shard.leases["key"].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("zero Renew changed deadline from %s to %s", wantDeadline, got)
	}
}

func TestRenewStaleAndExpiry(t *testing.T) {
	s, clock := newTestServer(t, time.Second, 1)
	activateServer(t, s)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	other := leaseID{clientID: 4, bootID: 5, leaseSeq: 6}
	shard := s.shards[0]

	missing := s.apply(shard, operation{kind: operationRenew, key: "missing", leaseID: id, requestedTTLMS: 1000}).GetRenew()
	if missing.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_STALE {
		t.Fatalf("missing Renew = %s, want STALE", missing.GetStatus())
	}
	s.apply(shard, operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000})
	wantDeadline := shard.leases["key"].deadline
	foreign := s.apply(shard, operation{kind: operationRenew, key: "key", leaseID: other, requestedTTLMS: 1000}).GetRenew()
	if foreign.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_STALE {
		t.Fatalf("foreign Renew = %s, want STALE", foreign.GetStatus())
	}
	if got := shard.leases["key"].deadline; !got.Equal(wantDeadline) {
		t.Fatalf("foreign Renew changed deadline from %s to %s", wantDeadline, got)
	}

	clock.Advance(time.Second)
	expired := s.apply(shard, operation{kind: operationRenew, key: "key", leaseID: id, requestedTTLMS: 1000}).GetRenew()
	if expired.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_STALE {
		t.Fatalf("expired Renew = %s, want STALE", expired.GetStatus())
	}
	if _, exists := shard.leases["key"]; exists {
		t.Fatal("expired Renew did not lazily delete lease")
	}
}

func TestReleaseIsIdempotentAndDeletesOnlyMatchingLease(t *testing.T) {
	s, _ := newTestServer(t, time.Second, 1)
	activateServer(t, s)
	id := leaseID{clientID: 1, bootID: 2, leaseSeq: 3}
	other := leaseID{clientID: 4, bootID: 5, leaseSeq: 6}
	shard := s.shards[0]
	s.apply(shard, operation{kind: operationAcquire, key: "key", leaseID: id, requestedTTLMS: 1000})

	foreign := s.apply(shard, operation{kind: operationRelease, key: "key", leaseID: other}).GetRelease()
	if foreign.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("foreign Release = %s, want OK", foreign.GetStatus())
	}
	if _, exists := shard.leases["key"]; !exists {
		t.Fatal("foreign Release deleted lease")
	}

	matching := s.apply(shard, operation{kind: operationRelease, key: "key", leaseID: id}).GetRelease()
	if matching.GetStatus() != redleasev1.LeaseStatus_LEASE_STATUS_OK {
		t.Fatalf("matching Release = %s, want OK", matching.GetStatus())
	}
	if _, exists := shard.leases["key"]; exists {
		t.Fatal("matching Release did not delete lease")
	}

	missing := s.apply(shard, operation{kind: operationRelease, key: "key", leaseID: id}).GetRelease()
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

func TestLeaseStreamRequestReceivedDuringQuarantineStaysNotReady(t *testing.T) {
	clock := &fakeClock{now: testEpoch}
	received := make(chan serverPhase, 1)
	resumeReceive := make(chan struct{})
	var resumeOnce sync.Once
	s, err := newWithDependencies(Config{
		ConfiguredMaxTTL:     time.Second,
		ShardCount:           1,
		ShardQueueDepth:      8,
		MaxInFlightPerStream: 8,
	}, dependencies{
		now:             clock.Now,
		quarantineDelay: time.Hour,
		afterReceive: func(phase serverPhase) {
			received <- phase
			<-resumeReceive
		},
	})
	if err != nil {
		t.Fatalf("newWithDependencies: %v", err)
	}
	t.Cleanup(func() {
		resumeOnce.Do(func() { close(resumeReceive) })
		_ = s.Close()
	})

	stream := newFakeLeaseStream(acquireRequest(1, []byte("key"), 1, 1000))
	errDone := make(chan error, 1)
	go func() { errDone <- s.LeaseStream(stream) }()

	select {
	case phase := <-received:
		if phase != phaseQuarantine {
			t.Fatalf("phase at Recv = %d, want QUARANTINE", phase)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not receive request")
	}

	// Activate before the receive path is allowed to dispatch the request. A
	// worker-only phase check would incorrectly turn this response into OK.
	activateServer(t, s)
	resumeOnce.Do(func() { close(resumeReceive) })

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
	clock := &fakeClock{now: testEpoch}
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	unblockFirst := make(chan struct{})
	var firstOnce sync.Once
	var secondOnce sync.Once
	var unblockOnce sync.Once
	s, err := newWithDependencies(Config{
		ConfiguredMaxTTL:     time.Second,
		ShardCount:           2,
		ShardQueueDepth:      8,
		MaxInFlightPerStream: 8,
	}, dependencies{
		now:             clock.Now,
		quarantineDelay: time.Hour,
		beforeApply: func(op operation) {
			switch op.requestID {
			case 1:
				firstOnce.Do(func() { close(firstStarted) })
				<-unblockFirst
			case 2:
				secondOnce.Do(func() { close(secondStarted) })
			}
		},
	})
	if err != nil {
		t.Fatalf("newWithDependencies: %v", err)
	}
	t.Cleanup(func() {
		unblockOnce.Do(func() { close(unblockFirst) })
		_ = s.Close()
	})
	activateServer(t, s)

	key := []byte("same-key")
	stream := newFakeLeaseStream(
		acquireRequest(1, key, 1, 1000),
		acquireRequest(2, key, 1, 1000),
	)
	errDone := make(chan error, 1)
	go func() { errDone <- s.LeaseStream(stream) }()

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first same-key request did not start")
	}
	shard := s.shards[s.shardIndex(string(key))]
	deadline := time.Now().Add(time.Second)
	for len(shard.jobs) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("second same-key request was not queued behind the first")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-secondStarted:
		t.Fatal("second same-key request started before the first completed")
	default:
	}
	select {
	case response := <-stream.sent:
		t.Fatalf("response %d arrived while the first request was blocked", response.GetRequestId())
	default:
	}

	unblockOnce.Do(func() { close(unblockFirst) })
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
	clock := &fakeClock{now: testEpoch}
	firstBlocked := make(chan struct{})
	unblockFirst := make(chan struct{})
	var blockOnce sync.Once
	var unblockOnce sync.Once
	s, err := newWithDependencies(Config{
		ConfiguredMaxTTL:     time.Second,
		ShardCount:           2,
		ShardQueueDepth:      8,
		MaxInFlightPerStream: 8,
	}, dependencies{
		now:             clock.Now,
		quarantineDelay: time.Hour,
		beforeApply: func(op operation) {
			if op.requestID == 1 {
				blockOnce.Do(func() { close(firstBlocked) })
				<-unblockFirst
			}
		},
	})
	if err != nil {
		t.Fatalf("newWithDependencies: %v", err)
	}
	t.Cleanup(func() {
		unblockOnce.Do(func() { close(unblockFirst) })
		_ = s.Close()
	})
	activateServer(t, s)

	firstKey, secondKey := keysForDifferentShards(t, s)
	stream := newFakeLeaseStream(
		acquireRequest(1, firstKey, 1, 1000),
		acquireRequest(2, secondKey, 2, 1000),
	)
	errDone := make(chan error, 1)
	go func() { errDone <- s.LeaseStream(stream) }()

	select {
	case <-firstBlocked:
	case <-time.After(time.Second):
		t.Fatal("first shard did not start processing")
	}
	select {
	case response := <-stream.sent:
		if response.GetRequestId() != 2 {
			t.Fatalf("first response request_id = %d, want 2", response.GetRequestId())
		}
	case <-time.After(time.Second):
		t.Fatal("second shard did not respond while first shard was blocked")
	}
	unblockOnce.Do(func() { close(unblockFirst) })
	select {
	case response := <-stream.sent:
		if response.GetRequestId() != 1 {
			t.Fatalf("second response request_id = %d, want 1", response.GetRequestId())
		}
	case <-time.After(time.Second):
		t.Fatal("first shard did not respond after clock was unblocked")
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
	s, _ := newTestServer(t, time.Second, 1)
	for _, request := range []*redleasev1.ClientRequest{nil, {}, {Operation: &redleasev1.ClientRequest_Acquire{}}} {
		_, _, err := s.decodeRequest(request)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("decodeRequest(%v) error = %v, want InvalidArgument", request, err)
		}
	}
}
