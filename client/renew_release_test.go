package client

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

func TestRetryReleaseReplicaDistinguishesDeadlineFromCancellation(t *testing.T) {
	client := &Client{}

	deadlineContext, cancelDeadline := context.WithDeadline(context.Background(), time.Now())
	defer cancelDeadline()
	if exhausted := client.retryReleaseReplica(deadlineContext, 0, 0, 0, nil); !exhausted {
		t.Fatal("release retry deadline was not reported as exhausted")
	}

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if exhausted := client.retryReleaseReplica(canceledContext, 0, 0, 0, nil); exhausted {
		t.Fatal("release retry cancellation was reported as deadline exhaustion")
	}
}

func TestReplicaIndices(t *testing.T) {
	if got, want := replicaIndices(1<<1|1<<4, 5), []int{1, 4}; !slices.Equal(got, want) {
		t.Fatalf("replicaIndices = %v, want %v", got, want)
	}
}

func TestLeaseRenewExtendsValidity(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, 1, 1_000)
	previous := leaseValidUntil(lease)

	result := startLeaseRenew(lease, context.Background(), 3_000)
	requests := harness.receiveRenewRequests(t)
	for replica := range testQuorumSize {
		harness.respondRenew(replica, requests[replica], redleasev1.LeaseStatusOK, 3_000)
	}

	if err := receiveRenewResult(t, result); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	want := leaseNow(lease).Add(2_900 * time.Millisecond)
	if got := leaseValidUntil(lease); !got.Equal(want) {
		t.Fatalf("validUntil = %v, want %v", got, want)
	}
	if !leaseValidUntil(lease).After(previous) {
		t.Fatalf("Renew did not extend previous validity %v", previous)
	}
	if lease.requestedTTLMS != 1_000 {
		t.Fatalf("Renew changed healing requestedTTL to %d", lease.requestedTTLMS)
	}

	for replica := testQuorumSize; replica < testServerCount; replica++ {
		harness.respondRenew(replica, requests[replica], redleasev1.LeaseStatusOK, 3_000)
	}
}

func TestLeaseNowIsAcquireStartAndChangesAtRenewStart(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, 1, 2_000)
	storedAcquireStart := leaseNow(lease)
	time.Sleep(time.Millisecond)
	result := startLeaseRenew(lease, context.Background(), 2_000)
	requests := harness.receiveRenewRequests(t)

	storedRenewStart := leaseNow(lease)
	if !storedRenewStart.After(storedAcquireStart) {
		t.Fatalf("lease now did not advance from Acquire %v at Renew start: %v", storedAcquireStart, storedRenewStart)
	}

	for replica, request := range requests {
		harness.respondRenew(replica, request, redleasev1.LeaseStatusOK, 2_000)
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
		uint64(1),
		2_000,
	)
	acquireRequests := harness.receiveAcquireRequests(t)
	for replica := range testQuorumSize {
		harness.respondAcquire(
			replica,
			acquireRequests[replica],
			redleasev1.LeaseStatusOK,
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
			redleasev1.LeaseStatusOK,
			2_000,
		)
	}
	if err := receiveRenewResult(t, renewResult); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	harness.respondAcquire(
		3,
		acquireRequests[3],
		redleasev1.LeaseStatusOK,
		1_000,
	)
	harness.respondAcquire(
		4,
		acquireRequests[4],
		redleasev1.LeaseStatusBUSY,
		0,
	)

	want := acquireStart.Add(900 * time.Millisecond)
	waitForReplicaConfirmedUntil(t, acquired.lease, 3, want)
}

func TestLeaseFailedRenewKeepsPreviousValidity(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, 1, 2_000)
	previous := leaseValidUntil(lease)

	result := startLeaseRenew(lease, context.Background(), 4_000)
	requests := harness.receiveRenewRequests(t)
	for replica, request := range requests {
		status := redleasev1.LeaseStatusSTALE
		if replica < 2 {
			status = redleasev1.LeaseStatusOK
		}
		harness.respondRenew(replica, request, status, 4_000)
	}

	err := receiveRenewResult(t, result)
	if !errors.Is(err, ErrNotRenewed) {
		t.Fatalf("Renew error = %v, want ErrNotRenewed", err)
	}
	if got := leaseValidUntil(lease); !got.Equal(previous) {
		t.Fatalf("failed Renew changed validity from %v to %v", previous, got)
	}
	if lease.RemainingTTLms() == 0 {
		t.Fatal("failed Renew revoked the previous live quorum")
	}
	waitForConfirmedReplicas(t, lease, [testServerCount]bool{true, true, false, false, false})
}

func TestLeaseRenewCanUseQuorumAfterUnacceptedSubmitTimesOut(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, 1, 2_000)
	harness.client.responseTimeout = 30 * time.Millisecond

	fifthGeneration := currentReplicaGeneration(t, harness.client.replicas[4])
	blocker, err := fifthGeneration.submit(context.Background(), acquireStreamRequest(1))
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	harness.streams[4].waitForSendAttempt(t)

	result := startLeaseRenew(lease, context.Background(), 3_000)
	var requests [testServerCount - 1]observedRequest
	for replica := range requests {
		request := receiveSentRequest(t, harness.streams[replica])
		if request.Operation != protocol.OperationRenew {
			t.Fatalf("replica %d request is not Renew: %+v", replica, request)
		}
		requests[replica] = request
	}
	for replica := range testQuorumSize {
		harness.respondRenew(replica, requests[replica], redleasev1.LeaseStatusOK, 3_000)
	}

	if err := receiveRenewResult(t, result); err != nil {
		t.Fatalf("Renew after unaccepted fifth submit timed out: %v", err)
	}

	blockerRequest := receiveSentRequest(t, harness.streams[4])
	harness.respondAcquire(4, blockerRequest, redleasev1.LeaseStatusBUSY, 0)
	if _, err := blocker.await(context.Background()); err != nil {
		t.Fatalf("await blocker: %v", err)
	}
}

func TestLeaseConcurrentRenewAndReleasePreservesWireOrderAndNoResurrection(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, 1, 2_000)

	renewResult := startLeaseRenew(lease, context.Background(), 4_000)
	renewRequests := harness.receiveRenewRequests(t)

	lease.Release()
	if lease.RemainingTTLms() != 0 {
		t.Fatal("Release did not invalidate lease immediately")
	}

	// These responses arrive after Release has transitioned the lease out of
	// ACTIVE. They must neither restore validity nor confirmations.
	for replica, request := range renewRequests {
		harness.respondRenew(replica, request, redleasev1.LeaseStatusOK, 4_000)
	}
	err := receiveRenewResult(t, renewResult)
	if !errors.Is(err, ErrNotRenewed) || !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("Renew error = %v, want ErrNotRenewed and ErrLeaseReleased", err)
	}

	releases := harness.receiveReleaseRequests(t)
	for replica, release := range releases {
		if !sameLeaseID(release, renewRequests[replica]) {
			t.Fatalf("replica %d Release used a different lease ID", replica)
		}
		harness.respondRelease(replica, release)
	}
	waitForLeaseReleased(t, lease)

	if lease.RemainingTTLms() != 0 {
		t.Fatal("late Renew resurrected released lease")
	}
	if got := lease.confirmedReplicas(); !slices.Equal(got, make([]bool, testServerCount)) {
		t.Fatalf("late Renew restored confirmations: %v", got)
	}
}

func TestLeaseReleaseIsImmediateIdempotentAndFansOut(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, 1, 2_000)

	lease.Release()
	lease.Release()
	if lease.RemainingTTLms() != 0 {
		t.Fatal("Release did not invalidate lease immediately")
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
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	requests := harness.receiveAcquireRequests(t)

	harness.respondAcquire(0, requests[0], redleasev1.LeaseStatusOK, 2_000)
	harness.respondAcquire(1, requests[1], redleasev1.LeaseStatusOK, 2_000)
	harness.respondAcquire(2, requests[2], redleasev1.LeaseStatusBUSY, 0)
	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatusBUSY, 0)
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
	if !sameLeaseID(retriedRelease, requests[4]) {
		t.Fatal("retried cleanup used a different lease ID")
	}
	harness.respondRelease(4, retriedRelease)
	waitForNoPendingStreamCalls(t, harness.client)
}

func TestConfirmedReplicaExpiresIndependently(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	requests := harness.receiveAcquireRequests(t)

	for replica := range testQuorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatusOK, 2_000)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatusOK, 500)
	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatusOK, 2_000)
	waitForConfirmedReplicas(t, acquired.lease, [testServerCount]bool{true, true, true, true, true})

	acquired.lease.stateMu.Lock()
	acquired.lease.confirmedUntil[3] = time.Now()
	acquired.lease.stateMu.Unlock()
	want := [testServerCount]bool{true, true, true, false, true}
	if got := acquired.lease.confirmedReplicas(); !slices.Equal(got, want[:]) {
		t.Fatalf("confirmed replicas after expiry = %v, want %v", got, want)
	}
	if acquired.lease.RemainingTTLms() == 0 {
		t.Fatal("one expired replica invalidated the selected quorum")
	}
}

type renewCallResult struct {
	err error
}

func acquireFullyConfirmedLease(
	t *testing.T,
	harness *acquireHarness,
	key uint64,
	ttl uint64,
) *Lease {
	t.Helper()
	result := startClientAcquire(harness.client, context.Background(), key, ttl)
	requests := harness.receiveAcquireRequests(t)
	for replica, request := range requests {
		harness.respondAcquire(replica, request, redleasev1.LeaseStatusOK, ttl)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	waitForConfirmedReplicas(t, acquired.lease, [testServerCount]bool{true, true, true, true, true})
	return acquired.lease
}

func startLeaseRenew(lease *Lease, ctx context.Context, ttl uint64) <-chan renewCallResult {
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

func (h *acquireHarness) receiveRenewRequests(t *testing.T) [testServerCount]observedRequest {
	t.Helper()
	var requests [testServerCount]observedRequest
	for replica, stream := range h.streams {
		request := receiveSentRequest(t, stream)
		if request.Operation != protocol.OperationRenew {
			t.Fatalf("replica %d request is not Renew: %+v", replica, request)
		}
		requests[replica] = request
	}
	return requests
}

func (h *acquireHarness) respondRenew(
	replica int,
	request observedRequest,
	status redleasev1.LeaseStatus,
	ttl uint64,
) {
	h.streams[replica].receive <- fakeReceive{
		response: protocol.Response{
			RequestID: request.RequestID,
			Operation: protocol.OperationRenew,
			Status:    status,
			TTLMS:     ttl,
		},
	}
}

func (h *acquireHarness) receiveReleaseRequests(t *testing.T) [testServerCount]observedRequest {
	t.Helper()
	var requests [testServerCount]observedRequest
	for replica, stream := range h.streams {
		requests[replica] = receiveReleaseRequest(t, stream)
	}
	return requests
}

func (h *acquireHarness) respondRelease(replica int, request observedRequest) {
	h.streams[replica].receive <- fakeReceive{
		response: protocol.Response{
			RequestID: request.RequestID,
			Operation: protocol.OperationRelease,
			Status:    redleasev1.LeaseStatusOK,
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

func waitForReplicaConfirmedUntil(t *testing.T, lease *Lease, replica int, want time.Time) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		lease.stateMu.RLock()
		confirmedUntil := lease.confirmedUntil[replica]
		lease.stateMu.RUnlock()
		if confirmedUntil.Equal(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica %d confirmed until = %v, want %v", replica, confirmedUntil, want)
		}
		time.Sleep(time.Millisecond)
	}
}
