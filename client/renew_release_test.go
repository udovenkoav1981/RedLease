package client

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/boottime"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func TestLeaseRenewExtendsValidity(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, "renew", 1_000)
	previous := leaseValidUntil(lease)

	result := startLeaseRenew(lease, context.Background(), 3_000)
	requests := harness.receiveRenewRequests(t)
	for replica := range testQuorumSize {
		harness.respondRenew(replica, requests[replica], redleasev1.LeaseStatus_LEASE_STATUS_OK, 3_000)
	}

	if err := receiveRenewResult(t, result); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	want := leaseNow(lease) + 2_900
	if got := leaseValidUntil(lease); got != want {
		t.Fatalf("validUntil = %d, want %d", got, want)
	}
	if leaseValidUntil(lease) <= previous {
		t.Fatalf("Renew did not extend previous validity %d", previous)
	}
	if lease.requestedTTL != 1_000 {
		t.Fatalf("Renew changed healing requestedTTL to %d", lease.requestedTTL)
	}

	for replica := testQuorumSize; replica < testServerCount; replica++ {
		harness.respondRenew(replica, requests[replica], redleasev1.LeaseStatus_LEASE_STATUS_OK, 3_000)
	}
}

func TestLeaseNowIsAcquireStartAndChangesAtRenewStart(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, "operation-time", 2_000)
	storedAcquireStart := leaseNow(lease)
	time.Sleep(time.Millisecond)
	result := startLeaseRenew(lease, context.Background(), 2_000)
	requests := harness.receiveRenewRequests(t)

	storedRenewStart := leaseNow(lease)
	if storedRenewStart <= storedAcquireStart {
		t.Fatalf("lease now did not advance from Acquire %d at Renew start: %d", storedAcquireStart, storedRenewStart)
	}

	for replica, request := range requests {
		harness.respondRenew(replica, request, redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	}
	if err := receiveRenewResult(t, result); err != nil {
		t.Fatalf("Renew: %v", err)
	}
}

func TestLateAcquireResponseKeepsAcquireNowAfterRenewStarts(t *testing.T) {
	harness := newAcquireHarness(t)
	acquireResult := startClientAcquire(
		harness.client,
		context.Background(),
		[]byte("late-operation-time"),
		2_000,
	)
	acquireRequests := harness.receiveAcquireRequests(t)
	for replica := range testQuorumSize {
		harness.respondAcquire(
			replica,
			acquireRequests[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_OK,
			2_000,
		)
	}
	acquired := receiveAcquireCallResult(t, acquireResult)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	acquireStart := leaseNow(acquired.lease)

	time.Sleep(time.Millisecond)
	renewResult := startLeaseRenew(acquired.lease, context.Background(), 2_000)
	renewRequests := harness.receiveRenewRequests(t)
	for replica := range testQuorumSize {
		harness.respondRenew(
			replica,
			renewRequests[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_OK,
			2_000,
		)
	}
	if err := receiveRenewResult(t, renewResult); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	harness.respondAcquire(
		3,
		acquireRequests[3],
		redleasev1.LeaseStatus_LEASE_STATUS_OK,
		1_000,
	)
	harness.respondAcquire(
		4,
		acquireRequests[4],
		redleasev1.LeaseStatus_LEASE_STATUS_BUSY,
		0,
	)

	want := acquireStart + 900
	waitForReplicaConfirmedUntil(t, acquired.lease, 3, want)
}

func TestLeaseFailedRenewKeepsPreviousValidity(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, "failed-renew", 2_000)
	previous := leaseValidUntil(lease)

	result := startLeaseRenew(lease, context.Background(), 4_000)
	requests := harness.receiveRenewRequests(t)
	for replica, request := range requests {
		status := redleasev1.LeaseStatus_LEASE_STATUS_STALE
		if replica < 2 {
			status = redleasev1.LeaseStatus_LEASE_STATUS_OK
		}
		harness.respondRenew(replica, request, status, 4_000)
	}

	err := receiveRenewResult(t, result)
	if !errors.Is(err, ErrNotRenewed) {
		t.Fatalf("Renew error = %v, want ErrNotRenewed", err)
	}
	if got := leaseValidUntil(lease); got != previous {
		t.Fatalf("failed Renew changed validity from %d to %d", previous, got)
	}
	if !lease.Valid() {
		t.Fatal("failed Renew revoked the previous live quorum")
	}
	waitForConfirmedReplicas(t, lease, [testServerCount]bool{true, true, false, false, false})
}

func TestLeaseRenewCanUseQuorumAfterUnacceptedSubmitTimesOut(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, "renew-barrier-timeout", 2_000)
	harness.client.responseTimeout = 30 * time.Millisecond

	fifthGeneration := currentReplicaGeneration(t, harness.client.replicas[4])
	blocker, err := fifthGeneration.submit(context.Background(), acquireStreamRequest("renew-blocker"))
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	harness.streams[4].waitForSendAttempt(t)

	result := startLeaseRenew(lease, context.Background(), 3_000)
	var requests [testServerCount - 1]*redleasev1.ClientRequest
	for replica := range requests {
		request := receiveSentRequest(t, harness.streams[replica])
		if request.GetRenew() == nil {
			t.Fatalf("replica %d request is not Renew: %+v", replica, request)
		}
		requests[replica] = request
	}
	for replica := range testQuorumSize {
		harness.respondRenew(replica, requests[replica], redleasev1.LeaseStatus_LEASE_STATUS_OK, 3_000)
	}

	if err := receiveRenewResult(t, result); err != nil {
		t.Fatalf("Renew after unaccepted fifth submit timed out: %v", err)
	}

	blockerRequest := receiveSentRequest(t, harness.streams[4])
	harness.respondAcquire(4, blockerRequest, redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	if _, err := blocker.await(context.Background()); err != nil {
		t.Fatalf("await blocker: %v", err)
	}
}

func TestLeaseConcurrentRenewAndReleasePreservesWireOrderAndNoResurrection(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, "renew-release", 2_000)

	renewResult := startLeaseRenew(lease, context.Background(), 4_000)
	renewRequests := harness.receiveRenewRequests(t)

	lease.Release()
	if lease.Valid() {
		t.Fatal("Release did not invalidate lease immediately")
	}
	if remaining := lease.RemainingTTL(); remaining != 0 {
		t.Fatalf("Release left remaining TTL %d", remaining)
	}

	// These responses arrive after Release has transitioned the lease out of
	// ACTIVE. They must neither restore validity nor confirmations.
	for replica, request := range renewRequests {
		harness.respondRenew(replica, request, redleasev1.LeaseStatus_LEASE_STATUS_OK, 4_000)
	}
	err := receiveRenewResult(t, renewResult)
	if !errors.Is(err, ErrNotRenewed) || !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("Renew error = %v, want ErrNotRenewed and ErrLeaseReleased", err)
	}

	releases := harness.receiveReleaseRequests(t)
	for replica, release := range releases {
		if !sameProtobufLeaseID(
			release.GetRelease().GetLeaseId(),
			renewRequests[replica].GetRenew().GetLeaseId(),
		) {
			t.Fatalf("replica %d Release used a different lease ID", replica)
		}
		harness.respondRelease(replica, release)
	}
	waitForLeaseReleased(t, lease)

	if lease.Valid() || lease.RemainingTTL() != 0 {
		t.Fatal("late Renew resurrected released lease")
	}
	if got := lease.confirmedReplicas(); !slices.Equal(got, make([]bool, testServerCount)) {
		t.Fatalf("late Renew restored confirmations: %v", got)
	}
}

func TestLeaseReleaseIsImmediateIdempotentAndFansOut(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, "release", 2_000)

	lease.Release()
	lease.Release()
	if lease.Valid() {
		t.Fatal("Release did not invalidate lease immediately")
	}
	if remaining := lease.RemainingTTL(); remaining != 0 {
		t.Fatalf("Release left remaining TTL %d", remaining)
	}

	releases := harness.receiveReleaseRequests(t)
	for replica, release := range releases {
		harness.respondRelease(replica, release)
	}
	waitForLeaseReleased(t, lease)

	for replica, stream := range harness.streams {
		select {
		case duplicate := <-stream.sent:
			t.Fatalf("replica %d received duplicate Release: %+v", replica, duplicate)
		default:
		}
	}

	err := lease.Renew(context.Background(), 2_000)
	if !errors.Is(err, ErrNotRenewed) || !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("Renew after Release error = %v", err)
	}
}

func TestFailedAcquireCleanupRetriesAfterReplicaReconnect(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("retry-cleanup"), 2_000)
	requests := harness.receiveAcquireRequests(t)

	harness.respondAcquire(0, requests[0], redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	harness.respondAcquire(1, requests[1], redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	harness.respondAcquire(2, requests[2], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	harness.streams[4].receive <- fakeReceive{err: errors.New("disconnect before cleanup")}

	failed := receiveAcquireCallResult(t, result)
	assertNotAcquired(t, failed.err)

	for replica := range testServerCount - 1 {
		release := receiveReleaseRequest(t, harness.streams[replica])
		harness.respondRelease(replica, release)
	}

	reconnected := newReplicaFakeStream()
	harness.factories[4].results <- streamFactoryResult{stream: reconnected}
	waitForReplicaState(t, harness.client.replicas[4], true, false)
	harness.streams[4] = reconnected

	retriedRelease := receiveReleaseRequest(t, reconnected)
	if !sameProtobufLeaseID(
		retriedRelease.GetRelease().GetLeaseId(),
		requests[4].GetAcquire().GetLeaseId(),
	) {
		t.Fatal("retried cleanup used a different lease ID")
	}
	harness.respondRelease(4, retriedRelease)
	waitForNoPendingStreamCalls(t, harness.client)
}

func TestConfirmedReplicaExpiresIndependently(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("confirmed-expiry"), 2_000)
	requests := harness.receiveAcquireRequests(t)

	for replica := range testQuorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatus_LEASE_STATUS_OK, 500)
	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	waitForConfirmedReplicas(t, acquired.lease, [testServerCount]bool{true, true, true, true, true})

	acquired.lease.stateMu.Lock()
	acquired.lease.confirmedUntil[3] = boottime.Now()
	acquired.lease.stateMu.Unlock()
	want := [testServerCount]bool{true, true, true, false, true}
	if got := acquired.lease.confirmedReplicas(); !slices.Equal(got, want[:]) {
		t.Fatalf("confirmed replicas after expiry = %v, want %v", got, want)
	}
	if !acquired.lease.Valid() {
		t.Fatal("one expired replica invalidated the selected quorum")
	}
}

type renewCallResult struct {
	err error
}

func acquireFullyConfirmedLease(
	t *testing.T,
	harness *acquireHarness,
	key string,
	ttl Milliseconds,
) *Lease {
	t.Helper()
	result := startClientAcquire(harness.client, context.Background(), []byte(key), ttl)
	requests := harness.receiveAcquireRequests(t)
	for replica, request := range requests {
		harness.respondAcquire(replica, request, redleasev1.LeaseStatus_LEASE_STATUS_OK, uint64(ttl))
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	waitForConfirmedReplicas(t, acquired.lease, [testServerCount]bool{true, true, true, true, true})
	return acquired.lease
}

func startLeaseRenew(lease *Lease, ctx context.Context, ttl Milliseconds) <-chan renewCallResult {
	result := make(chan renewCallResult, 1)
	go func() { result <- renewCallResult{err: lease.Renew(ctx, ttl)} }()
	return result
}

func receiveRenewResult(t *testing.T, result <-chan renewCallResult) error {
	t.Helper()
	select {
	case renewed := <-result:
		return renewed.err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Renew result")
		return nil
	}
}

func (h *acquireHarness) receiveRenewRequests(t *testing.T) [testServerCount]*redleasev1.ClientRequest {
	t.Helper()
	var requests [testServerCount]*redleasev1.ClientRequest
	for replica, stream := range h.streams {
		request := receiveSentRequest(t, stream)
		if request.GetRenew() == nil {
			t.Fatalf("replica %d request is not Renew: %+v", replica, request)
		}
		requests[replica] = request
	}
	return requests
}

func (h *acquireHarness) respondRenew(
	replica int,
	request *redleasev1.ClientRequest,
	status redleasev1.LeaseStatus,
	ttl uint64,
) {
	h.streams[replica].receive <- fakeReceive{
		response: &redleasev1.ServerResponse{
			RequestId: request.GetRequestId(),
			Result: &redleasev1.ServerResponse_Renew{
				Renew: &redleasev1.RenewResponse{Status: status, TtlMs: ttl},
			},
		},
	}
}

func (h *acquireHarness) receiveReleaseRequests(t *testing.T) [testServerCount]*redleasev1.ClientRequest {
	t.Helper()
	var requests [testServerCount]*redleasev1.ClientRequest
	for replica, stream := range h.streams {
		requests[replica] = receiveReleaseRequest(t, stream)
	}
	return requests
}

func (h *acquireHarness) respondRelease(replica int, request *redleasev1.ClientRequest) {
	h.streams[replica].receive <- fakeReceive{
		response: &redleasev1.ServerResponse{
			RequestId: request.GetRequestId(),
			Result: &redleasev1.ServerResponse_Release{
				Release: &redleasev1.ReleaseResponse{Status: redleasev1.LeaseStatus_LEASE_STATUS_OK},
			},
		},
	}
}

func waitForLeaseReleased(t *testing.T, lease *Lease) {
	t.Helper()
	select {
	case <-lease.releaseDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for asynchronous Release")
	}
}

func waitForReplicaConfirmedUntil(t *testing.T, lease *Lease, replica int, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		lease.stateMu.RLock()
		confirmedUntil := lease.confirmedUntil[replica]
		lease.stateMu.RUnlock()
		if confirmedUntil == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica %d confirmed until = %d, want %d", replica, confirmedUntil, want)
		}
		time.Sleep(time.Millisecond)
	}
}
