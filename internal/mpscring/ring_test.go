package mpscring

import (
	"runtime"
	"sync"
	"testing"
)

func TestRingCapacityAndWrap(t *testing.T) {
	queue := New[*int]()
	values := make([]int, Capacity*2)
	for cycle := range 2 {
		for index := range Capacity {
			value := &values[cycle*Capacity+index]
			if !queue.TryEnqueue(value) {
				t.Fatalf("enqueue cycle %d at %d failed", cycle, index)
			}
		}
		if queue.TryEnqueue(new(int)) {
			t.Fatal("enqueue beyond capacity succeeded")
		}
		if got := queue.Len(); got != Capacity {
			t.Fatalf("queue length = %d, want %d", got, Capacity)
		}
		for index := range Capacity {
			want := &values[cycle*Capacity+index]
			got, ok := queue.TryDequeue()
			if !ok || got != want {
				t.Fatalf("dequeue = %p, %t; want %p", got, ok, want)
			}
		}
	}
	if got, ok := queue.TryDequeue(); ok || got != nil {
		t.Fatalf("empty dequeue = %p, %t; want nil, false", got, ok)
	}
	if got := queue.Len(); got != 0 {
		t.Fatalf("empty queue length = %d, want 0", got)
	}
}

func TestRingConcurrentProducers(t *testing.T) {
	const (
		producerCount = 16
		perProducer   = 1_000
	)
	queue := New[*int]()
	values := make([]int, producerCount*perProducer)
	var producers sync.WaitGroup
	for producer := range producerCount {
		producers.Go(func() {
			start := producer * perProducer
			for index := start; index < start+perProducer; index++ {
				for !queue.TryEnqueue(&values[index]) {
					runtime.Gosched()
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
