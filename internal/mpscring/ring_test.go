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

func TestRingConcurrentInPlaceProducers(t *testing.T) {
	const (
		producerCount = 16
		perProducer   = 1_000
	)
	queue := NewNotifying[*int](testCapacity)
	values := make([]int, producerCount*perProducer)
	var producers sync.WaitGroup
	for producer := range producerCount {
		producers.Go(func() {
			start := producer * perProducer
			for index := start; index < start+perProducer; index++ {
				for {
					slot, ticket := queue.TryStartEnqueue()
					if slot == nil {
						runtime.Gosched()
						continue
					}
					*slot = &values[index]
					queue.FinishEnqueue(ticket)
					break
				}
			}
		})
	}
	seen := make(map[*int]struct{}, len(values))
	for len(seen) < len(values) {
		value, ok := queue.TryDequeue()
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

func TestNotifyingRingInPlaceEnqueueRequiresFinish(t *testing.T) {
	queue := NewNotifying[int](2)
	value, ticket := queue.TryStartEnqueue()
	if value == nil {
		t.Fatal("in-place enqueue failed")
	}
	*value = 10
	if got := queue.Len(); got != 1 {
		t.Fatalf("queue length with reserved slot = %d, want 1", got)
	}
	if got, dequeued := queue.TryDequeue(); dequeued || got != 0 {
		t.Fatalf("unpublished dequeue = %d, %t; want 0, false", got, dequeued)
	}

	queue.FinishEnqueue(ticket)
	select {
	case <-queue.Ready():
	default:
		t.Fatal("finishing enqueue did not signal readiness")
	}
	if got, dequeued := queue.TryDequeue(); !dequeued || got != 10 {
		t.Fatalf("published dequeue = %d, %t; want 10, true", got, dequeued)
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

func TestNotifyingRingInPlaceDequeue(t *testing.T) {
	queue := NewNotifying[int](2)
	if !queue.TryEnqueue(10) || !queue.TryEnqueue(20) {
		t.Fatal("enqueue failed")
	}

	value := queue.TryStartDequeue()
	if value == nil || *value != 10 {
		t.Fatalf("in-place dequeue = %v; want pointer to 10", value)
	}
	*value = 11
	if got := queue.Len(); got != 2 {
		t.Fatalf("queue length while slot is held = %d, want 2", got)
	}
	if queue.TryEnqueue(30) {
		t.Fatal("enqueue succeeded while the full queue's oldest slot was held")
	}

	queue.FinishDequeue()
	if got := queue.Len(); got != 1 {
		t.Fatalf("queue length after finishing dequeue = %d, want 1", got)
	}
	if got := queue.ring.slots[0].value; got != 0 {
		t.Fatalf("released slot retained value %d, want 0", got)
	}
	if !queue.TryEnqueue(30) {
		t.Fatal("enqueue after finishing dequeue failed")
	}

	if got, ok := queue.TryDequeue(); !ok || got != 20 {
		t.Fatalf("regular dequeue = %d, %t; want 20, true", got, ok)
	}
	value = queue.TryStartDequeue()
	if value == nil || *value != 30 {
		t.Fatalf("second in-place dequeue = %v; want pointer to 30", value)
	}
	queue.FinishDequeue()
	if value = queue.TryStartDequeue(); value != nil {
		t.Fatalf("empty in-place dequeue = %v; want nil", value)
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
