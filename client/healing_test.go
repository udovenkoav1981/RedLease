package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

func TestBackgroundHealingRetriesMissingReplicasToFiveOfFive(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range testQuorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			protocol.StatusOK,
			2_000,
		)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	originalValidUntil := leaseValidUntil(acquired.lease)

	for replica := testQuorumSize; replica < testServerCount; replica++ {
		harness.respondAcquire(
			replica,
			initial[replica],
			protocol.StatusBusy,
			0,
		)
	}

	firstFourth := receiveAcquireRequest(t, harness.streams[3])
	firstFifth := receiveAcquireRequest(t, harness.streams[4])
	assertHealingAcquire(t, firstFourth, initial[3])
	assertHealingAcquire(t, firstFifth, initial[4])
	harness.respondAcquire(3, firstFourth, protocol.StatusOK, 2_000)
	harness.respondAcquire(4, firstFifth, protocol.StatusBusy, 0)

	waitForConfirmedReplicas(
		t,
		acquired.lease,
		[testServerCount]bool{true, true, true, true, false},
	)

	secondFifth := receiveAcquireRequest(t, harness.streams[4])
	assertHealingAcquire(t, secondFifth, initial[4])
	harness.respondAcquire(4, secondFifth, protocol.StatusOK, 2_000)
	waitForConfirmedReplicas(
		t,
		acquired.lease,
		[testServerCount]bool{true, true, true, true, true},
	)

	if got := leaseValidUntil(acquired.lease); !got.Equal(originalValidUntil) {
		t.Fatalf("healing changed validity from %v to %v", originalValidUntil, got)
	}
}

func TestBackgroundHealingReattachesReplicaAfterStaleRenew(t *testing.T) {
	harness := newAcquireHarness(t)
	lease := acquireFullyConfirmedLease(t, harness, 2, 2_000)

	renewResult := startLeaseRenew(lease, context.Background(), 3_000)
	renewRequests := harness.receiveRenewRequests(t)
	for replica := range testServerCount {
		status := protocol.StatusOK
		if replica == 4 {
			status = protocol.StatusStale
		}
		harness.respondRenew(replica, renewRequests[replica], status, 3_000)
	}
	if err := receiveRenewResult(t, renewResult); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	renewedValidUntil := leaseValidUntil(lease)
	waitForConfirmedReplicas(
		t,
		lease,
		[testServerCount]bool{true, true, true, true, false},
	)

	healing := receiveAcquireRequest(t, harness.streams[4])
	if got := healing.RequestedTTLMS; got != 2_000 {
		t.Fatalf("healing requested TTL = %d, want original Acquire TTL 2000", got)
	}
	if !sameLeaseID(healing, renewRequests[4]) {
		t.Fatal("healing after stale Renew used a different lease ID")
	}
	harness.respondAcquire(4, healing, protocol.StatusOK, 2_000)
	waitForConfirmedReplicas(t, lease, [testServerCount]bool{true, true, true, true, true})

	if got := leaseValidUntil(lease); !got.Equal(renewedValidUntil) {
		t.Fatalf("healing changed renewed validity from %v to %v", renewedValidUntil, got)
	}
}

func TestBackgroundHealingContinuesAfterReplicaReconnect(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 5_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range testQuorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			protocol.StatusOK,
			5_000,
		)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	harness.respondAcquire(3, initial[3], protocol.StatusBusy, 0)
	harness.streams[4].receive <- fakeReceive{err: errors.New("replica restart")}

	reconnected := newReplicaFakeStream()
	harness.factories[4].results <- streamFactoryResult{stream: reconnected}
	waitForReplicaState(t, harness.client.replicas[4], true, false)
	harness.streams[4] = reconnected

	fourth := receiveAcquireRequest(t, harness.streams[3])
	assertHealingAcquire(t, fourth, initial[3])
	harness.respondAcquire(3, fourth, protocol.StatusOK, 5_000)

	fifth := receiveAcquireRequest(t, reconnected)
	assertHealingAcquire(t, fifth, initial[4])
	harness.respondAcquire(4, fifth, protocol.StatusOK, 5_000)

	waitForConfirmedReplicas(
		t,
		acquired.lease,
		[testServerCount]bool{true, true, true, true, true},
	)
}

func TestBackgroundHealingStopsAfterLocalValidityExpires(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 1_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range testQuorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			protocol.StatusOK,
			1_000,
		)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}

	acquired.lease.stateMu.Lock()
	acquired.lease.validUntil = time.Now()
	acquired.lease.stateMu.Unlock()
	for replica := testQuorumSize; replica < testServerCount; replica++ {
		harness.respondAcquire(
			replica,
			initial[replica],
			protocol.StatusBusy,
			0,
		)
	}

	for replica := testQuorumSize; replica < testServerCount; replica++ {
		select {
		case request := <-harness.streams[replica].sent:
			t.Fatalf("replica %d received healing after validity expired: %+v", replica, request)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestBackgroundHealingStopsBeforeReleaseSubmission(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range testQuorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			protocol.StatusOK,
			2_000,
		)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	for replica := testQuorumSize; replica < testServerCount; replica++ {
		harness.respondAcquire(
			replica,
			initial[replica],
			protocol.StatusBusy,
			0,
		)
	}

	for replica := testQuorumSize; replica < testServerCount; replica++ {
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
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	initial := harness.receiveAcquireRequests(t)

	for replica := range testQuorumSize {
		harness.respondAcquire(
			replica,
			initial[replica],
			protocol.StatusOK,
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

	for replica := range testServerCount - 1 {
		release := receiveReleaseRequest(t, harness.streams[replica])
		harness.respondRelease(replica, release)
	}
	waitForLeaseReleased(t, acquired.lease)

	reconnected := newReplicaFakeStream()
	harness.factories[4].results <- streamFactoryResult{stream: reconnected}
	waitForReplicaState(t, harness.client.replicas[4], true, false)
	harness.streams[4] = reconnected

	request := receiveSentRequest(t, reconnected)
	if request.Operation != protocol.OperationRelease {
		t.Fatalf("first request after Release and reconnect is not Release: %+v", request)
	}
	if !sameLeaseID(request, initial[4]) {
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
	healing protocol.Request,
	initial protocol.Request,
) {
	t.Helper()
	if healing.Key != initial.Key {
		t.Fatal("healing used a different key")
	}
	if !sameLeaseID(healing, initial) {
		t.Fatal("healing used a different lease ID")
	}
	if healing.RequestedTTLMS != initial.RequestedTTLMS {
		t.Fatal("healing used a different requested TTL")
	}
}
