package mpscring

import (
	"runtime"
	"sync"
	"testing"
)

func TestRingCapacityAndWrap(t *testing.T) {
	queue := newRing[*int]()
	values := make([]int, Capacity*2)
	for cycle := range 2 {
		for index := range Capacity {
			value := &values[cycle*Capacity+index]
			if !queue.tryEnqueue(value) {
				t.Fatalf("enqueue cycle %d at %d failed", cycle, index)
			}
		}
		if queue.tryEnqueue(new(int)) {
			t.Fatal("enqueue beyond capacity succeeded")
		}
		if got := queue.len(); got != Capacity {
			t.Fatalf("queue length = %d, want %d", got, Capacity)
		}
		for index := range Capacity {
			want := &values[cycle*Capacity+index]
			got, ok := queue.tryDequeue()
			if !ok || got != want {
				t.Fatalf("dequeue = %p, %t; want %p", got, ok, want)
			}
		}
	}
	if got, ok := queue.tryDequeue(); ok || got != nil {
		t.Fatalf("empty dequeue = %p, %t; want nil, false", got, ok)
	}
	if got := queue.len(); got != 0 {
		t.Fatalf("empty queue length = %d, want 0", got)
	}
}

func TestRingConcurrentProducers(t *testing.T) {
	const (
		producerCount = 16
		perProducer   = 1_000
	)
	queue := newRing[*int]()
	values := make([]int, producerCount*perProducer)
	var producers sync.WaitGroup
	for producer := range producerCount {
		producers.Go(func() {
			start := producer * perProducer
			for index := start; index < start+perProducer; index++ {
				for !queue.tryEnqueue(&values[index]) {
					runtime.Gosched()
				}
			}
		})
	}
	seen := make(map[*int]struct{}, len(values))
	for len(seen) < len(values) {
		value, ok := queue.tryDequeue()
		if !ok {
			runtime.Gosched()
			continue
		}
		if _, exists := seen[value]; exists {
			t.Fatal("value dequeued twice")
		}
		seen[value] = struct{}{}
	}
	producers.Wait()
	for index := range values {
		if _, ok := seen[&values[index]]; !ok {
			t.Fatalf("value %d was lost", index)
		}
	}
}

func TestNotifyingRingCoalescesWakeups(t *testing.T) {
	queue := NewNotifying[*int]()
	first := 1
	second := 2

	select {
	case <-queue.Ready():
		t.Fatal("empty queue signaled readiness")
	default:
	}
	if !queue.TryEnqueue(&first) || !queue.TryEnqueue(&second) {
		t.Fatal("enqueue failed")
	}
	select {
	case <-queue.Ready():
	default:
		t.Fatal("successful enqueue did not signal readiness")
	}
	select {
	case <-queue.Ready():
		t.Fatal("wakeups were not coalesced")
	default:
	}

	if got, ok := queue.TryDequeue(); !ok || got != &first {
		t.Fatalf("first dequeue = %p, %t; want %p, true", got, ok, &first)
	}
	if got, ok := queue.TryDequeue(); !ok || got != &second {
		t.Fatalf("second dequeue = %p, %t; want %p, true", got, ok, &second)
	}
}
