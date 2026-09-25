package mpscring

import (
	"runtime"
	"sync"
	"testing"
)

const testCapacity = 64

func TestRingCapacityAndWrap(t *testing.T) {
	queue := newRing[*int](testCapacity)
	values := make([]int, testCapacity*2)
	for cycle := range 2 {
		for index := range testCapacity {
			value := &values[cycle*testCapacity+index]
			if !queue.tryEnqueue(value) {
				t.Fatalf("enqueue cycle %d at %d failed", cycle, index)
			}
		}
		if queue.tryEnqueue(new(int)) {
			t.Fatal("enqueue beyond capacity succeeded")
		}
		if got := queue.len(); got != testCapacity {
			t.Fatalf("queue length = %d, want %d", got, testCapacity)
		}
		for index := range testCapacity {
			want := &values[cycle*testCapacity+index]
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
	queue := newRing[*int](testCapacity)
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
	queue := NewNotifying[*int](testCapacity)
	if got := queue.Capacity(); got != testCapacity {
		t.Fatalf("queue capacity = %d, want %d", got, testCapacity)
	}
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

func TestNewNotifyingRejectsInvalidCapacity(t *testing.T) {
	for _, capacity := range []int{-1, 0, 1, 3, 6} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("NewNotifying capacity %d did not panic", capacity)
				}
			}()
			NewNotifying[int](capacity)
		}()
	}
}
