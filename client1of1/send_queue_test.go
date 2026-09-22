package client1of1

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestRequestRingFullAndWrap(t *testing.T) {
	queue := newRequestRing()
	requests := make([]outboundConnectionRequest, 2*sendQueueCapacity)
	for cycle := range 2 {
		start := cycle * sendQueueCapacity
		for index := range sendQueueCapacity {
			if !queue.tryEnqueue(&requests[start+index]) {
				t.Fatalf("enqueue cycle %d at %d reported full", cycle, index)
			}
		}
		if queue.tryEnqueue(&outboundConnectionRequest{}) {
			t.Fatal("enqueue beyond capacity succeeded")
		}
		if got := queue.len(); got != sendQueueCapacity {
			t.Fatalf("queue length = %d, want %d", got, sendQueueCapacity)
		}
		for index := range sendQueueCapacity {
			request, ok := queue.tryDequeue()
			if !ok || request != &requests[start+index] {
				t.Fatalf("dequeue cycle %d at %d = (%p, %t)", cycle, index, request, ok)
			}
		}
		if request, ok := queue.tryDequeue(); ok || request != nil {
			t.Fatalf("empty dequeue = (%p, %t)", request, ok)
		}
		if got := queue.len(); got != 0 {
			t.Fatalf("queue length after draining = %d, want 0", got)
		}
	}
}

func TestRequestRingConcurrentProducers(t *testing.T) {
	const producers = 16
	const requestsPerProducer = 2000
	const total = producers * requestsPerProducer
	queue := newRequestRing()
	requests := make([]outboundConnectionRequest, total)
	var producersDone sync.WaitGroup
	producersDone.Add(producers)
	for producer := range producers {
		go func() {
			defer producersDone.Done()
			start := producer * requestsPerProducer
			for index := range requestsPerProducer {
				for !queue.tryEnqueue(&requests[start+index]) {
					runtime.Gosched()
				}
			}
		}()
	}

	seen := make(map[*outboundConnectionRequest]bool, total)
	deadline := time.Now().Add(10 * time.Second)
	for len(seen) < total {
		request, ok := queue.tryDequeue()
		if !ok {
			if time.Now().After(deadline) {
				t.Fatalf("received %d of %d requests", len(seen), total)
			}
			runtime.Gosched()
			continue
		}
		if seen[request] {
			t.Fatalf("duplicate request %p", request)
		}
		seen[request] = true
	}
	producersDone.Wait()
	for index := range requests {
		if !seen[&requests[index]] {
			t.Fatalf("request %d was lost", index)
		}
	}
}
