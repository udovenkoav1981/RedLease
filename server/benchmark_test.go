package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func BenchmarkServerAcquireReleaseQueue(b *testing.B) {
	s, err := New(Config{ConfiguredMaxTTL: ProtocolMaxTTL})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	s.phase.Store(uint32(phaseActive))
	b.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	responses := make(chan *redleasev1.ServerResponse, 1)
	complete := func(response *redleasev1.ServerResponse) { responses <- response }
	b.ReportAllocs()
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		key := strconv.Itoa(iteration)
		id := leaseID{clientID: 1, bootID: 1, leaseSeq: uint64(iteration + 1)}
		if !s.dispatch(ctx.Done(), shardJob{
			operation: operation{
				kind:           operationAcquire,
				key:            key,
				leaseID:        id,
				requestedTTLMS: uint64(ProtocolMaxTTL / time.Millisecond),
			},
			complete: complete,
		}) {
			b.Fatal("dispatch Acquire")
		}
		if status := (<-responses).GetAcquire().GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
			b.Fatalf("Acquire status = %s", status)
		}

		if !s.dispatch(ctx.Done(), shardJob{
			operation: operation{kind: operationRelease, key: key, leaseID: id},
			complete:  complete,
		}) {
			b.Fatal("dispatch Release")
		}
		if status := (<-responses).GetRelease().GetStatus(); status != redleasev1.LeaseStatus_LEASE_STATUS_OK {
			b.Fatalf("Release status = %s", status)
		}
	}
}
