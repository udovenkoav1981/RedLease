package client

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func TestBackgroundHealingRetriesMissingReplicasToFiveOfFive(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("heal"), 2_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range quorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_OK,
			2_000,
		)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	originalValidUntil := acquired.lease.ValidUntil()

	for replica := quorumSize; replica < ServerCount; replica++ {
		harness.respondAcquire(
			replica,
			initial[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_BUSY,
			0,
		)
	}

	firstFourth := receiveAcquireRequest(t, harness.streams[3])
	firstFifth := receiveAcquireRequest(t, harness.streams[4])
	assertHealingAcquire(t, firstFourth, initial[3])
	assertHealingAcquire(t, firstFifth, initial[4])
	harness.respondAcquire(3, firstFourth, redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	harness.respondAcquire(4, firstFifth, redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)

	waitForConfirmedReplicas(
		t,
		acquired.lease,
		[ServerCount]bool{true, true, true, true, false},
	)

	secondFifth := receiveAcquireRequest(t, harness.streams[4])
	assertHealingAcquire(t, secondFifth, initial[4])
	harness.respondAcquire(4, secondFifth, redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	waitForConfirmedReplicas(
		t,
		acquired.lease,
		[ServerCount]bool{true, true, true, true, true},
	)

	if got := acquired.lease.ValidUntil(); !got.Equal(originalValidUntil) {
		t.Fatalf("healing changed validity from %v to %v", originalValidUntil, got)
	}
}

func TestBackgroundHealingReattachesReplicaAfterStaleRenew(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, "restart-heal", 2_000)

	renewResult := startLeaseRenew(lease, context.Background(), 3_000)
	renewRequests := harness.receiveRenewRequests(t)
	for replica := range ServerCount {
		status := redleasev1.LeaseStatus_LEASE_STATUS_OK
		if replica == 4 {
			status = redleasev1.LeaseStatus_LEASE_STATUS_STALE
		}
		harness.respondRenew(replica, renewRequests[replica], status, 3_000)
	}
	if err := receiveRenewResult(t, renewResult); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	renewedValidUntil := lease.ValidUntil()
	waitForConfirmedReplicas(
		t,
		lease,
		[ServerCount]bool{true, true, true, true, false},
	)

	healing := receiveAcquireRequest(t, harness.streams[4])
	if got := healing.GetAcquire().GetRequestedTtlMs(); got != 2_000 {
		t.Fatalf("healing requested TTL = %d, want original Acquire TTL 2000", got)
	}
	if !sameProtobufLeaseID(
		healing.GetAcquire().GetLeaseId(),
		renewRequests[4].GetRenew().GetLeaseId(),
	) {
		t.Fatal("healing after stale Renew used a different lease ID")
	}
	harness.respondAcquire(4, healing, redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	waitForConfirmedReplicas(t, lease, [ServerCount]bool{true, true, true, true, true})

	if got := lease.ValidUntil(); !got.Equal(renewedValidUntil) {
		t.Fatalf("healing changed renewed validity from %v to %v", renewedValidUntil, got)
	}
}

func TestBackgroundHealingContinuesAfterReplicaReconnect(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("reconnect-heal"), 5_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range quorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_OK,
			5_000,
		)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	harness.respondAcquire(3, initial[3], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	harness.streams[4].receive <- fakeReceive{err: errors.New("replica restart")}

	reconnected := newReplicaFakeStream()
	harness.factories[4].results <- streamFactoryResult{stream: reconnected}
	waitForReplicaState(t, harness.client.replicas[4], true, false)
	harness.streams[4] = reconnected

	fourth := receiveAcquireRequest(t, harness.streams[3])
	assertHealingAcquire(t, fourth, initial[3])
	harness.respondAcquire(3, fourth, redleasev1.LeaseStatus_LEASE_STATUS_OK, 5_000)

	fifth := receiveAcquireRequest(t, reconnected)
	assertHealingAcquire(t, fifth, initial[4])
	harness.respondAcquire(4, fifth, redleasev1.LeaseStatus_LEASE_STATUS_OK, 5_000)

	waitForConfirmedReplicas(
		t,
		acquired.lease,
		[ServerCount]bool{true, true, true, true, true},
	)
}

func TestBackgroundHealingStopsAfterLocalValidityExpires(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("expired-heal"), 1_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range quorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_OK,
			1_000,
		)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}

	acquired.lease.stateMu.Lock()
	acquired.lease.validUntil = time.Now().Round(0)
	acquired.lease.stateMu.Unlock()
	for replica := quorumSize; replica < ServerCount; replica++ {
		harness.respondAcquire(
			replica,
			initial[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_BUSY,
			0,
		)
	}

	for replica := quorumSize; replica < ServerCount; replica++ {
		select {
		case request := <-harness.streams[replica].sent:
			t.Fatalf("replica %d received healing after validity expired: %+v", replica, request)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestBackgroundHealingStopsBeforeReleaseSubmission(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("release-heal"), 2_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range quorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_OK,
			2_000,
		)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	for replica := quorumSize; replica < ServerCount; replica++ {
		harness.respondAcquire(
			replica,
			initial[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_BUSY,
			0,
		)
	}

	for replica := quorumSize; replica < ServerCount; replica++ {
		healing := receiveAcquireRequest(t, harness.streams[replica])
		assertHealingAcquire(t, healing, initial[replica])
	}

	acquired.lease.Release()
	releases := harness.receiveReleaseRequests(t)
	for replica, release := range releases {
		harness.respondRelease(replica, release)
	}
	waitForLeaseReleased(t, acquired.lease)

	for replica, stream := range harness.streams {
		select {
		case request := <-stream.sent:
			t.Fatalf("replica %d received request after Release: %+v", replica, request)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestBackgroundHealingDoesNotAcquireAfterReleaseAndReconnect(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("release-reconnect"), 2_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range quorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			redleasev1.LeaseStatus_LEASE_STATUS_OK,
			2_000,
		)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}

	harness.streams[4].receive <- fakeReceive{err: errors.New("disconnect before Release")}
	waitForReplicaState(t, harness.client.replicas[4], false, false)
	acquired.lease.Release()

	for replica := range ServerCount - 1 {
		release := receiveReleaseRequest(t, harness.streams[replica])
		harness.respondRelease(replica, release)
	}
	waitForLeaseReleased(t, acquired.lease)

	reconnected := newReplicaFakeStream()
	harness.factories[4].results <- streamFactoryResult{stream: reconnected}
	waitForReplicaState(t, harness.client.replicas[4], true, false)
	harness.streams[4] = reconnected

	request := receiveSentRequest(t, reconnected)
	if request.GetRelease() == nil {
		t.Fatalf("first request after Release and reconnect is not Release: %+v", request)
	}
	if !sameProtobufLeaseID(request.GetRelease().GetLeaseId(), initial[4].GetAcquire().GetLeaseId()) {
		t.Fatal("Release after reconnect used a different lease ID")
	}
	harness.respondRelease(4, request)
	waitForNoPendingStreamCalls(t, harness.client)

	select {
	case unexpected := <-reconnected.sent:
		t.Fatalf("request sent after reconnect cleanup: %+v", unexpected)
	case <-time.After(100 * time.Millisecond):
	}
}

func assertHealingAcquire(
	t *testing.T,
	healing *redleasev1.ClientRequest,
	initial *redleasev1.ClientRequest,
) {
	t.Helper()
	if !bytes.Equal(healing.GetAcquire().GetKey(), initial.GetAcquire().GetKey()) {
		t.Fatal("healing used a different key")
	}
	if !sameProtobufLeaseID(
		healing.GetAcquire().GetLeaseId(),
		initial.GetAcquire().GetLeaseId(),
	) {
		t.Fatal("healing used a different lease ID")
	}
	if healing.GetAcquire().GetRequestedTtlMs() != initial.GetAcquire().GetRequestedTtlMs() {
		t.Fatal("healing used a different requested TTL")
	}
}
