package client

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

func TestClientAcquireThreeOKEstablishesValidity(t *testing.T) {
	harness := newAcquireHarness(t)
	key := uint64(42)
	result := startClientAcquire(harness.client, context.Background(), key, 2_000)
	requests := harness.receiveAcquireRequests(t)

	harness.respondAcquire(0, requests[0], redleasev1.LeaseStatusOK, 1_000)
	harness.respondAcquire(1, requests[1], redleasev1.LeaseStatusALREADY_OWNED, 1_500)
	harness.respondAcquire(2, requests[2], redleasev1.LeaseStatusOK, 2_000)

	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	wantValidUntil := leaseNow(acquired.lease).Add(900 * time.Millisecond)
	if got := leaseValidUntil(acquired.lease); !got.Equal(wantValidUntil) {
		t.Fatalf("validUntil = %v, want %v", got, wantValidUntil)
	}
	if acquired.lease.RemainingTTLms() == 0 {
		t.Fatal("newly acquired lease is not valid")
	}
	if request := requests[0]; request.ClientID != 19 || request.BootID != harness.client.bootID ||
		request.LeaseSequence != acquired.lease.sequence || acquired.lease.sequence != 1 {
		t.Fatalf("unexpected lease ID fields: %+v", request)
	}

	if got := acquired.lease.Key(); got != key {
		t.Fatalf("lease key = %d, want %d", got, key)
	}

	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatusBUSY, 0)
	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatusBUSY, 0)
}

func TestClientAcquireUsesEverySupportedQuorum(t *testing.T) {
	tests := []struct {
		name   string
		quorum Quorum
	}{
		{name: "1-of-1", quorum: Quorum1Of1},
		{name: "2-of-3", quorum: Quorum2Of3},
		{name: "3-of-5", quorum: Quorum3Of5},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			quorum := test.quorum
			client, factories := newClientForQuorum(quorum)
			client.responseTimeout = 500 * time.Millisecond
			client.clientID = 19
			client.bootID = 1
			t.Cleanup(func() { _ = client.Close() })

			serverCount, quorumSize, _ := quorum.parameters()
			streams := make([]*fakeLeaseClientStream, serverCount)
			for replica, factory := range factories {
				streams[replica] = newReplicaFakeStream()
				factory.results <- streamFactoryResult{stream: streams[replica]}
				waitForReplicaState(t, client.replicas[replica], true, false)
			}

			result := startClientAcquire(client, context.Background(), uint64(1), 2_000)
			requests := make([]observedRequest, serverCount)
			for replica, stream := range streams {
				requests[replica] = receiveAcquireRequest(t, stream)
			}
			for replica := range quorumSize - 1 {
				respondAcquireOnStream(
					streams[replica],
					requests[replica],
					redleasev1.LeaseStatusOK,
					2_000,
				)
			}
			select {
			case acquired := <-result:
				t.Fatalf("Acquire returned below quorum: %v", acquired.err)
			case <-time.After(20 * time.Millisecond):
			}

			respondAcquireOnStream(
				streams[quorumSize-1],
				requests[quorumSize-1],
				redleasev1.LeaseStatusOK,
				2_000,
			)
			acquired := receiveAcquireCallResult(t, result)
			if acquired.err != nil {
				t.Fatalf("Acquire at quorum: %v", acquired.err)
			}
			if acquired.lease.RemainingTTLms() == 0 {
				t.Fatal("Acquire at quorum returned an invalid lease")
			}

			for replica := quorumSize; replica < serverCount; replica++ {
				respondAcquireOnStream(
					streams[replica],
					requests[replica],
					redleasev1.LeaseStatusOK,
					2_000,
				)
			}
			waitForNoPendingStreamCalls(t, client)
		})
	}
}

func TestClientAcquireSelectsAnyValidThreeFromHeterogeneousResponses(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 4_000)
	requests := harness.receiveAcquireRequests(t)

	harness.respondAcquire(0, requests[0], redleasev1.LeaseStatusOK, 2_000)
	harness.respondAcquire(1, requests[1], redleasev1.LeaseStatusOK, 3_000)
	// This successful but already unusable replica must not poison a quorum
	// made from the other three successful replicas.
	harness.respondAcquire(2, requests[2], redleasev1.LeaseStatusOK, 50)
	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatusOK, 2_500)

	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	want := leaseNow(acquired.lease).Add(1_900 * time.Millisecond)
	if got := leaseValidUntil(acquired.lease); !got.Equal(want) {
		t.Fatalf("validUntil = %v, want best 3/5 quorum %v", got, want)
	}

	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatusBUSY, 0)
}

func TestClientAcquireZeroTTLDoesCleanupOnAllFive(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 0)
	requests := harness.receiveAcquireRequests(t)
	for replica, request := range requests {
		harness.respondAcquire(replica, request, redleasev1.LeaseStatusOK, 0)
	}

	failed := receiveAcquireCallResult(t, result)
	assertNotAcquired(t, failed.err)
	harness.receiveAndRespondToCleanup(t, &requests)
}

func TestClientAcquireReportsServerKeyLimit(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 1_000)
	requests := harness.receiveAcquireRequests(t)
	for replica, request := range requests {
		harness.respondAcquire(
			replica,
			request,
			redleasev1.LeaseStatusKEY_LIMIT_REACHED,
			0,
		)
	}

	failed := receiveAcquireCallResult(t, result)
	if !errors.Is(failed.err, ErrNotAcquired) || !errors.Is(failed.err, ErrKeyLimitReached) {
		t.Fatalf("limited Acquire error = %v, want ErrNotAcquired and ErrKeyLimitReached", failed.err)
	}
	harness.receiveAndRespondToCleanup(t, &requests)
}

func TestClientAcquireExpiredQuorumDoesCleanupOnAllFive(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 1_000)
	requests := harness.receiveAcquireRequests(t)
	for replica, request := range requests {
		// safetyMargin is 100ms, so this candidate is already at the strict
		// validity boundary even though every server returned OK.
		harness.respondAcquire(replica, request, redleasev1.LeaseStatusOK, 100)
	}

	failed := receiveAcquireCallResult(t, result)
	assertNotAcquired(t, failed.err)
	harness.receiveAndRespondToCleanup(t, &requests)
}

func TestClientAcquireTwoOKThreeBusyCleansSameLeaseIDOnAllFive(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	requests := harness.receiveAcquireRequests(t)

	for replica, request := range requests {
		status := redleasev1.LeaseStatusBUSY
		if replica < 2 {
			status = redleasev1.LeaseStatusOK
		}
		harness.respondAcquire(replica, request, status, 2_000)
	}

	failed := receiveAcquireCallResult(t, result)
	assertNotAcquired(t, failed.err)
	harness.receiveAndRespondToCleanup(t, &requests)
}

func TestClientAcquireCleansImmediatelyWhenQuorumBecomesImpossible(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	requests := harness.receiveAcquireRequests(t)

	for replica := range testQuorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatusBUSY, 0)
	}

	select {
	case failed := <-result:
		assertNotAcquired(t, failed.err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Acquire waited for responses after quorum became impossible")
	}
	harness.receiveAndRespondToCleanup(t, &requests)
}

func TestClientAcquireCallerCancellationStillSubmitsCleanup(t *testing.T) {
	harness := newAcquireHarness(t)
	callerContext, cancelCaller := context.WithCancel(context.Background())
	result := startClientAcquire(harness.client, callerContext, uint64(1), 2_000)
	requests := harness.receiveAcquireRequests(t)
	cancelCaller()

	failed := receiveAcquireCallResult(t, result)
	assertNotAcquired(t, failed.err)
	if !errors.Is(failed.err, context.Canceled) {
		t.Fatalf("Acquire error = %v, want caller cancellation", failed.err)
	}
	harness.receiveAndRespondToCleanup(t, &requests)
}

func TestClientAcquireWaitsForAllFiveSubmissionBarriers(t *testing.T) {
	harness := newAcquireHarness(t)

	fifthGeneration := currentReplicaGeneration(t, harness.client.replicas[4])
	blocker, err := fifthGeneration.submit(context.Background(), acquireStreamRequest(1))
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	harness.streams[4].waitForSendAttempt(t)

	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	var requests [testServerCount]observedRequest
	for replica := range testServerCount - 1 {
		requests[replica] = receiveAcquireRequest(t, harness.streams[replica])
	}
	for replica := range testQuorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatusOK, 2_000)
	}

	select {
	case early := <-result:
		t.Fatalf("Acquire returned before fifth submission barrier: %+v", early)
	case <-time.After(20 * time.Millisecond):
	}

	blockerRequest := receiveSentRequest(t, harness.streams[4])
	harness.respondAcquire(4, blockerRequest, redleasev1.LeaseStatusBUSY, 0)
	if _, err := blocker.await(context.Background()); err != nil {
		t.Fatalf("await blocker: %v", err)
	}

	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire after fifth barrier: %v", acquired.err)
	}

	requests[4] = receiveAcquireRequest(t, harness.streams[4])
	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatusBUSY, 0)
	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatusBUSY, 0)
}

func TestClientAcquireCanUseQuorumAfterUnacceptedSubmitTimesOut(t *testing.T) {
	harness := newAcquireHarness(t)
	harness.client.responseTimeout = 30 * time.Millisecond

	fifthGeneration := currentReplicaGeneration(t, harness.client.replicas[4])
	blocker, err := fifthGeneration.submit(context.Background(), acquireStreamRequest(1))
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	harness.streams[4].waitForSendAttempt(t)

	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	var requests [testServerCount - 1]observedRequest
	for replica := range requests {
		requests[replica] = receiveAcquireRequest(t, harness.streams[replica])
	}
	for replica := range testQuorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatusOK, 2_000)
	}

	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire after unaccepted fifth submit timed out: %v", acquired.err)
	}

	blockerRequest := receiveSentRequest(t, harness.streams[4])
	harness.respondAcquire(4, blockerRequest, redleasev1.LeaseStatusBUSY, 0)
	if _, err := blocker.await(context.Background()); err != nil {
		t.Fatalf("await blocker: %v", err)
	}
	for replica := testQuorumSize; replica < len(requests); replica++ {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatusBUSY, 0)
	}
}

func TestClientAcquireLateResponsesOnlyUpdateConfirmedReplicas(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), uint64(1), 2_000)
	requests := harness.receiveAcquireRequests(t)

	for replica := range testQuorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatusOK, 1_000)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	originalValidUntil := leaseValidUntil(acquired.lease)

	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatusALREADY_OWNED, 5_000)
	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatusOK, 5_000)
	waitForConfirmedReplicas(t, acquired.lease, [testServerCount]bool{true, true, true, true, true})

	if got := leaseValidUntil(acquired.lease); !got.Equal(originalValidUntil) {
		t.Fatalf("late responses changed validity from %v to %v", originalValidUntil, got)
	}
}

func TestClientAcquireConcurrentCalls(t *testing.T) {
	harness := newAcquireHarness(t)

	const calls = 20
	results := make([]<-chan acquireCallResult, calls)
	for call := range calls {
		results[call] = startClientAcquire(
			harness.client,
			context.Background(),
			uint64(call+1),
			2_000,
		)
	}

	var responders sync.WaitGroup
	responders.Add(testServerCount)
	for replica, stream := range harness.streams {
		go func() {
			defer responders.Done()
			for range calls {
				request := receiveAcquireRequest(t, stream)
				harness.respondAcquire(replica, request, redleasev1.LeaseStatusOK, 2_000)
			}
		}()
	}
	responders.Wait()

	seen := make(map[uint64]struct{}, calls)
	for _, result := range results {
		acquired := receiveAcquireCallResult(t, result)
		if acquired.err != nil {
			t.Fatalf("concurrent Acquire: %v", acquired.err)
		}
		sequence := acquired.lease.sequence
		if _, duplicate := seen[sequence]; duplicate {
			t.Fatalf("duplicate lease sequence %d", sequence)
		}
		seen[sequence] = struct{}{}
		waitForConfirmedReplicas(t, acquired.lease, [testServerCount]bool{true, true, true, true, true})
	}
}

type acquireHarness struct {
	client    *Client
	streams   [testServerCount]*fakeLeaseClientStream
	factories [testServerCount]*scriptedStreamFactory
}

type acquireCallResult struct {
	lease *Lease
	err   error
}

func newAcquireHarness(t *testing.T) *acquireHarness {
	t.Helper()
	client, factories := newClientWithScriptedReplicasWithoutCleanup()
	client.clientID = 19
	client.bootID = 1
	client.responseTimeout = 500 * time.Millisecond

	harness := &acquireHarness{client: client, factories: factories}
	for replica, factory := range factories {
		stream := newReplicaFakeStream()
		harness.streams[replica] = stream
		factory.results <- streamFactoryResult{stream: stream}
		waitForReplicaState(t, client.replicas[replica], true, false)
	}
	t.Cleanup(func() { _ = client.Close() })
	return harness
}

func (h *acquireHarness) receiveAcquireRequests(t *testing.T) [testServerCount]observedRequest {
	t.Helper()
	var requests [testServerCount]observedRequest
	for replica, stream := range h.streams {
		requests[replica] = receiveAcquireRequest(t, stream)
	}
	return requests
}

func (h *acquireHarness) respondAcquire(
	replica int,
	request observedRequest,
	status redleasev1.LeaseStatus,
	ttl uint64,
) {
	h.streams[replica].receive <- fakeReceive{
		response: protocol.Response{
			RequestID: request.RequestID,
			Operation: protocol.OperationAcquire,
			Status:    status,
			TTLMS:     ttl,
		},
	}
}

func (h *acquireHarness) receiveAndRespondToCleanup(
	t *testing.T,
	acquireRequests *[testServerCount]observedRequest,
) {
	t.Helper()
	for replica, stream := range h.streams {
		release := receiveReleaseRequest(t, stream)
		if release.Key != acquireRequests[replica].Key {
			t.Fatalf("replica %d cleanup key differs from Acquire key", replica)
		}
		if !sameLeaseID(release, acquireRequests[replica]) {
			t.Fatalf("replica %d cleanup lease ID differs from Acquire lease ID", replica)
		}
		stream.receive <- fakeReceive{
			response: protocol.Response{
				RequestID: release.RequestID,
				Operation: protocol.OperationRelease,
				Status:    redleasev1.LeaseStatusOK,
			},
		}
	}
	waitForNoPendingStreamCalls(t, h.client)
}

func startClientAcquire(
	client *Client,
	ctx context.Context,
	key uint64,
	ttl uint64,
) <-chan acquireCallResult {
	result := make(chan acquireCallResult, 1)
	go func() {
		lease, err := client.Acquire(ctx, key, ttl)
		result <- acquireCallResult{lease: lease, err: err}
	}()
	return result
}

func receiveAcquireCallResult(t *testing.T, result <-chan acquireCallResult) acquireCallResult {
	t.Helper()
	select {
	case acquired := <-result:
		return acquired
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Acquire result")
		return acquireCallResult{}
	}
}

func receiveAcquireRequest(t *testing.T, stream *fakeLeaseClientStream) observedRequest {
	t.Helper()
	request := receiveSentRequest(t, stream)
	if request.Operation != protocol.OperationAcquire {
		t.Fatalf("request is not Acquire: %+v", request)
	}
	return request
}

func respondAcquireOnStream(
	stream *fakeLeaseClientStream,
	request observedRequest,
	status redleasev1.LeaseStatus,
	ttl uint64,
) {
	stream.receive <- fakeReceive{
		response: protocol.Response{
			RequestID: request.RequestID,
			Operation: protocol.OperationAcquire,
			Status:    status,
			TTLMS:     ttl,
		},
	}
}

func receiveReleaseRequest(t *testing.T, stream *fakeLeaseClientStream) observedRequest {
	t.Helper()
	request := receiveSentRequest(t, stream)
	if request.Operation != protocol.OperationRelease {
		t.Fatalf("request is not Release: %+v", request)
	}
	return request
}

func currentReplicaGeneration(t *testing.T, replica *replicaConn) *connectionGeneration {
	t.Helper()
	replica.stateMu.Lock()
	defer replica.stateMu.Unlock()
	if replica.generation == nil {
		t.Fatal("replica has no current generation")
	}
	return replica.generation
}

func waitForConfirmedReplicas(t *testing.T, lease *Lease, want [testServerCount]bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if got := lease.confirmedReplicas(); slices.Equal(got, want[:]) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("confirmed replicas = %v, want %v", lease.confirmedReplicas(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func leaseNow(lease *Lease) time.Time {
	lease.stateMu.RLock()
	defer lease.stateMu.RUnlock()
	return lease.now
}

func leaseValidUntil(lease *Lease) time.Time {
	lease.stateMu.RLock()
	defer lease.stateMu.RUnlock()
	return lease.validUntil
}

func waitForNoPendingStreamCalls(t *testing.T, client *Client) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		pending := 0
		for _, replica := range client.replicas {
			generation := currentReplicaGeneration(t, replica)
			generation.pendingMu.Lock()
			pending += len(generation.pending)
			generation.pendingMu.Unlock()
		}
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d stream calls remain pending", pending)
		}
		time.Sleep(time.Millisecond)
	}
}

func sameLeaseID(first, second observedRequest) bool {
	return first.ClientID == second.ClientID &&
		first.BootID == second.BootID &&
		first.LeaseSequence == second.LeaseSequence
}

func assertNotAcquired(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("Acquire error = %v, want ErrNotAcquired", err)
	}
}
