package client

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func TestClientAcquireThreeOKEstablishesValidity(t *testing.T) {
	harness := newAcquireHarness(t)
	key := []byte("resource")
	result := startClientAcquire(harness.client, context.Background(), key, 2_000)
	requests := harness.receiveAcquireRequests(t)

	harness.respondAcquire(0, requests[0], redleasev1.LeaseStatus_LEASE_STATUS_OK, 1_000)
	harness.respondAcquire(1, requests[1], redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED, 1_500)
	harness.respondAcquire(2, requests[2], redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)

	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	wantValidUntil := harness.clock.now().Add(900 * time.Millisecond)
	if got := acquired.lease.ValidUntil(); !got.Equal(wantValidUntil) {
		t.Fatalf("ValidUntil = %v, want %v", got, wantValidUntil)
	}
	if !acquired.lease.Valid() {
		t.Fatal("newly acquired lease is not valid")
	}
	if id := acquired.lease.ID(); id.ClientID != 19 || id.BootID != 0x01020304 || id.LeaseSeq != 0 {
		t.Fatalf("unexpected lease ID: %+v", id)
	}

	key[0] = 'X'
	if got := acquired.lease.Key(); !bytes.Equal(got, []byte("resource")) {
		t.Fatalf("lease key changed through caller slice: %q", got)
	}
	returnedKey := acquired.lease.Key()
	returnedKey[0] = 'Y'
	if got := acquired.lease.Key(); !bytes.Equal(got, []byte("resource")) {
		t.Fatalf("lease key aliases getter result: %q", got)
	}

	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
}

func TestClientAcquireSelectsAnyValidThreeFromHeterogeneousResponses(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("key"), 4_000)
	requests := harness.receiveAcquireRequests(t)

	harness.respondAcquire(0, requests[0], redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	harness.respondAcquire(1, requests[1], redleasev1.LeaseStatus_LEASE_STATUS_OK, 3_000)
	// This successful but already unusable replica must not poison a quorum
	// made from the other three successful replicas.
	harness.respondAcquire(2, requests[2], redleasev1.LeaseStatus_LEASE_STATUS_OK, 50)
	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_500)

	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	want := harness.clock.now().Add(1_900 * time.Millisecond)
	if got := acquired.lease.ValidUntil(); !got.Equal(want) {
		t.Fatalf("ValidUntil = %v, want best 3/5 quorum %v", got, want)
	}

	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
}

func TestClientAcquireZeroTTLDoesCleanupOnAllFive(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("zero"), 0)
	requests := harness.receiveAcquireRequests(t)
	for replica, request := range requests {
		harness.respondAcquire(replica, request, redleasev1.LeaseStatus_LEASE_STATUS_OK, 0)
	}

	failed := receiveAcquireCallResult(t, result)
	assertNotAcquired(t, failed.err)
	harness.receiveAndRespondToCleanup(t, requests)
}

func TestClientAcquireExpiredQuorumDoesCleanupOnAllFive(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("expired"), 1_000)
	requests := harness.receiveAcquireRequests(t)
	for replica, request := range requests {
		// safetyMargin is 100ms, so this candidate is already at the strict
		// validity boundary even though every server returned OK.
		harness.respondAcquire(replica, request, redleasev1.LeaseStatus_LEASE_STATUS_OK, 100)
	}

	failed := receiveAcquireCallResult(t, result)
	assertNotAcquired(t, failed.err)
	harness.receiveAndRespondToCleanup(t, requests)
}

func TestClientAcquireTwoOKThreeBusyCleansSameLeaseIDOnAllFive(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("contended"), 2_000)
	requests := harness.receiveAcquireRequests(t)

	for replica, request := range requests {
		status := redleasev1.LeaseStatus_LEASE_STATUS_BUSY
		if replica < 2 {
			status = redleasev1.LeaseStatus_LEASE_STATUS_OK
		}
		harness.respondAcquire(replica, request, status, 2_000)
	}

	failed := receiveAcquireCallResult(t, result)
	assertNotAcquired(t, failed.err)
	harness.receiveAndRespondToCleanup(t, requests)
}

func TestClientAcquireCleansImmediatelyWhenQuorumBecomesImpossible(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("impossible"), 2_000)
	requests := harness.receiveAcquireRequests(t)

	for replica := range quorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	}

	select {
	case failed := <-result:
		assertNotAcquired(t, failed.err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Acquire waited for responses after quorum became impossible")
	}
	harness.receiveAndRespondToCleanup(t, requests)
}

func TestClientAcquireCallerCancellationStillSubmitsCleanup(t *testing.T) {
	harness := newAcquireHarness(t)
	callerContext, cancelCaller := context.WithCancel(context.Background())
	result := startClientAcquire(harness.client, callerContext, []byte("canceled"), 2_000)
	requests := harness.receiveAcquireRequests(t)
	cancelCaller()

	failed := receiveAcquireCallResult(t, result)
	assertNotAcquired(t, failed.err)
	if !errors.Is(failed.err, context.Canceled) {
		t.Fatalf("Acquire error = %v, want caller cancellation", failed.err)
	}
	harness.receiveAndRespondToCleanup(t, requests)
}

func TestClientAcquireWaitsForAllFiveSubmissionBarriers(t *testing.T) {
	harness := newAcquireHarness(t)

	fifthGeneration := currentReplicaGeneration(t, harness.client.replicas[4])
	blocker, err := fifthGeneration.submit(context.Background(), acquireStreamRequest("blocker"))
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	harness.streams[4].waitForSendAttempt(t)

	result := startClientAcquire(harness.client, context.Background(), []byte("barrier"), 2_000)
	var requests [ServerCount]*redleasev1.ClientRequest
	for replica := range ServerCount - 1 {
		requests[replica] = receiveAcquireRequest(t, harness.streams[replica])
	}
	for replica := range quorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	}

	select {
	case early := <-result:
		t.Fatalf("Acquire returned before fifth submission barrier: %+v", early)
	case <-time.After(20 * time.Millisecond):
	}

	blockerRequest := receiveSentRequest(t, harness.streams[4])
	harness.respondAcquire(4, blockerRequest, redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	if _, err := blocker.await(context.Background()); err != nil {
		t.Fatalf("await blocker: %v", err)
	}

	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire after fifth barrier: %v", acquired.err)
	}

	requests[4] = receiveAcquireRequest(t, harness.streams[4])
	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
}

func TestClientAcquireCanUseQuorumAfterUnacceptedSubmitTimesOut(t *testing.T) {
	harness := newAcquireHarness(t)
	harness.client.responseTimeout = 30 * time.Millisecond

	fifthGeneration := currentReplicaGeneration(t, harness.client.replicas[4])
	blocker, err := fifthGeneration.submit(context.Background(), acquireStreamRequest("blocker"))
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	harness.streams[4].waitForSendAttempt(t)

	result := startClientAcquire(harness.client, context.Background(), []byte("barrier-timeout"), 2_000)
	var requests [ServerCount - 1]*redleasev1.ClientRequest
	for replica := range requests {
		requests[replica] = receiveAcquireRequest(t, harness.streams[replica])
	}
	for replica := range quorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
	}

	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire after unaccepted fifth submit timed out: %v", acquired.err)
	}

	blockerRequest := receiveSentRequest(t, harness.streams[4])
	harness.respondAcquire(4, blockerRequest, redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	if _, err := blocker.await(context.Background()); err != nil {
		t.Fatalf("await blocker: %v", err)
	}
	for replica := quorumSize; replica < len(requests); replica++ {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatus_LEASE_STATUS_BUSY, 0)
	}
}

func TestClientAcquireLateResponsesOnlyUpdateConfirmedReplicas(t *testing.T) {
	harness := newAcquireHarness(t)
	result := startClientAcquire(harness.client, context.Background(), []byte("late"), 2_000)
	requests := harness.receiveAcquireRequests(t)

	for replica := range quorumSize {
		harness.respondAcquire(replica, requests[replica], redleasev1.LeaseStatus_LEASE_STATUS_OK, 1_000)
	}
	acquired := receiveAcquireCallResult(t, result)
	if acquired.err != nil {
		t.Fatalf("Acquire: %v", acquired.err)
	}
	originalValidUntil := acquired.lease.ValidUntil()

	harness.respondAcquire(3, requests[3], redleasev1.LeaseStatus_LEASE_STATUS_ALREADY_OWNED, 5_000)
	harness.respondAcquire(4, requests[4], redleasev1.LeaseStatus_LEASE_STATUS_OK, 5_000)
	waitForConfirmedReplicas(t, acquired.lease, [ServerCount]bool{true, true, true, true, true})

	if got := acquired.lease.ValidUntil(); !got.Equal(originalValidUntil) {
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
			[]byte{byte(call)},
			2_000,
		)
	}

	var responders sync.WaitGroup
	responders.Add(ServerCount)
	for replica, stream := range harness.streams {
		go func() {
			defer responders.Done()
			for range calls {
				request := receiveAcquireRequest(t, stream)
				harness.respondAcquire(replica, request, redleasev1.LeaseStatus_LEASE_STATUS_OK, 2_000)
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
		sequence := acquired.lease.ID().LeaseSeq
		if _, duplicate := seen[sequence]; duplicate {
			t.Fatalf("duplicate lease sequence %d", sequence)
		}
		seen[sequence] = struct{}{}
		waitForConfirmedReplicas(t, acquired.lease, [ServerCount]bool{true, true, true, true, true})
	}
}

type acquireHarness struct {
	client  *Client
	streams [ServerCount]*fakeLeaseClientStream
	clock   *fixedWallClock
}

type fixedWallClock struct {
	mu   sync.RWMutex
	time time.Time
}

func (c *fixedWallClock) now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.time
}

type acquireCallResult struct {
	lease *Lease
	err   error
}

func newAcquireHarness(t *testing.T) *acquireHarness {
	t.Helper()
	client, factories := newClientWithScriptedReplicasWithoutCleanup()
	clock := &fixedWallClock{
		time: time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC),
	}
	generator, err := newLeaseIDGeneratorFromReader(19, bytes.NewReader([]byte{1, 2, 3, 4}))
	if err != nil {
		t.Fatalf("new lease ID generator: %v", err)
	}
	client.idGenerator = generator
	client.responseTimeout = 500 * time.Millisecond
	client.wall = clock

	harness := &acquireHarness{client: client, clock: clock}
	for replica, factory := range factories {
		stream := newReplicaFakeStream()
		harness.streams[replica] = stream
		factory.results <- streamFactoryResult{stream: stream}
		waitForReplicaState(t, client.replicas[replica], true, false)
	}
	t.Cleanup(func() { _ = client.Close() })
	return harness
}

func (h *acquireHarness) receiveAcquireRequests(t *testing.T) [ServerCount]*redleasev1.ClientRequest {
	t.Helper()
	var requests [ServerCount]*redleasev1.ClientRequest
	for replica, stream := range h.streams {
		requests[replica] = receiveAcquireRequest(t, stream)
	}
	return requests
}

func (h *acquireHarness) respondAcquire(
	replica int,
	request *redleasev1.ClientRequest,
	status redleasev1.LeaseStatus,
	ttl uint64,
) {
	h.streams[replica].receive <- fakeReceive{
		response: &redleasev1.ServerResponse{
			RequestId: request.GetRequestId(),
			Result: &redleasev1.ServerResponse_Acquire{
				Acquire: &redleasev1.AcquireResponse{Status: status, TtlMs: ttl},
			},
		},
	}
}

func (h *acquireHarness) receiveAndRespondToCleanup(
	t *testing.T,
	acquireRequests [ServerCount]*redleasev1.ClientRequest,
) {
	t.Helper()
	for replica, stream := range h.streams {
		release := receiveReleaseRequest(t, stream)
		if !bytes.Equal(release.GetRelease().GetKey(), acquireRequests[replica].GetAcquire().GetKey()) {
			t.Fatalf("replica %d cleanup key differs from Acquire key", replica)
		}
		if !sameProtobufLeaseID(release.GetRelease().GetLeaseId(), acquireRequests[replica].GetAcquire().GetLeaseId()) {
			t.Fatalf("replica %d cleanup lease ID differs from Acquire lease ID", replica)
		}
		stream.receive <- fakeReceive{
			response: &redleasev1.ServerResponse{
				RequestId: release.GetRequestId(),
				Result: &redleasev1.ServerResponse_Release{
					Release: &redleasev1.ReleaseResponse{Status: redleasev1.LeaseStatus_LEASE_STATUS_OK},
				},
			},
		}
	}
	waitForNoPendingStreamCalls(t, h.client)
}

func startClientAcquire(
	client *Client,
	ctx context.Context,
	key []byte,
	ttl Milliseconds,
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

func receiveAcquireRequest(t *testing.T, stream *fakeLeaseClientStream) *redleasev1.ClientRequest {
	t.Helper()
	request := receiveSentRequest(t, stream)
	if request.GetAcquire() == nil {
		t.Fatalf("request is not Acquire: %+v", request)
	}
	return request
}

func receiveReleaseRequest(t *testing.T, stream *fakeLeaseClientStream) *redleasev1.ClientRequest {
	t.Helper()
	request := receiveSentRequest(t, stream)
	if request.GetRelease() == nil {
		t.Fatalf("request is not Release: %+v", request)
	}
	return request
}

func currentReplicaGeneration(t *testing.T, replica *replicaConn) *streamGeneration {
	t.Helper()
	replica.stateMu.Lock()
	defer replica.stateMu.Unlock()
	if replica.generation == nil {
		t.Fatal("replica has no current generation")
	}
	return replica.generation
}

func waitForConfirmedReplicas(t *testing.T, lease *Lease, want [ServerCount]bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if got := lease.confirmedReplicas(); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("confirmed replicas = %v, want %v", lease.confirmedReplicas(), want)
		}
		time.Sleep(time.Millisecond)
	}
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

func sameProtobufLeaseID(first, second *redleasev1.LeaseID) bool {
	return first.GetClientId() == second.GetClientId() &&
		first.GetBootId() == second.GetBootId() &&
		first.GetLeaseSeq() == second.GetLeaseSeq()
}

func assertNotAcquired(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("Acquire error = %v, want ErrNotAcquired", err)
	}
}
