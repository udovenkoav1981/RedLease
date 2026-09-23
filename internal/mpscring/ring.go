package mpscring

import "sync/atomic"

// Capacity is the fixed number of elements held by a ring.
const Capacity = 4096

// ring is a bounded multi-producer, single-consumer FIFO. A producer claims a
// position with tail, then publishes its value through the slot sequence. The
// consumer never reads a claimed but unpublished slot.
type ring[T any] struct {
	head  atomic.Uint64
	_     [56]byte // Keep the frequently written head and tail on separate cache lines.
	tail  atomic.Uint64
	_     [56]byte
	slots [Capacity]slot[T]
}

type slot[T any] struct {
	sequence atomic.Uint64
	value    T
}

// newRing creates an empty Ring.
func newRing[T any]() *ring[T] {
	queue := &ring[T]{}
	for index := range queue.slots {
		queue.slots[index].sequence.Store(uint64(index)) //nolint:gosec // Array index is nonnegative.
	}
	return queue
}

// tryEnqueue appends value and reports whether the queue had room for it.
func (q *ring[T]) tryEnqueue(value T) bool {
	for {
		position := q.tail.Load()
		slot := &q.slots[position&(Capacity-1)]
		sequence := slot.sequence.Load()
		if sequence == position {
			if q.tail.CompareAndSwap(position, position+1) {
				slot.value = value
				slot.sequence.Store(position + 1)
				return true
			}
			continue
		}
		if sequence < position {
			return false
		}
		// Another producer advanced tail since our load; retry its new position.
	}
}

// tryDequeue removes the oldest value and reports whether one was available.
// It must be called by exactly one consumer at a time.
func (q *ring[T]) tryDequeue() (T, bool) {
	position := q.head.Load()
	slot := &q.slots[position&(Capacity-1)]
	if slot.sequence.Load() != position+1 {
		var zero T
		return zero, false
	}
	value := slot.value
	var zero T
	slot.value = zero
	q.head.Store(position + 1)
	slot.sequence.Store(position + Capacity)
	return value, true
}

// len returns a momentary snapshot of the number of claimed queue positions.
func (q *ring[T]) len() uint64 {
	return q.tail.Load() - q.head.Load()
}

// NotifyingRing adds a coalescing wakeup signal to a ring. A successful
// enqueue makes Ready readable; consumers still drain the ring with
// TryDequeue because multiple enqueues may share one signal.
type NotifyingRing[T any] struct {
	ring  *ring[T]
	ready chan struct{}
}

// NewNotifying creates an empty ring with a buffered wakeup signal.
func NewNotifying[T any]() *NotifyingRing[T] {
	return &NotifyingRing[T]{
		ring:  newRing[T](),
		ready: make(chan struct{}, 1),
	}
}

// TryEnqueue appends value and signals the consumer when successful.
func (q *NotifyingRing[T]) TryEnqueue(value T) bool {
	if !q.ring.tryEnqueue(value) {
		return false
	}
	select {
	case q.ready <- struct{}{}:
	default:
	}
	return true
}

// TryDequeue removes the oldest value and reports whether one was available.
func (q *NotifyingRing[T]) TryDequeue() (T, bool) {
	return q.ring.tryDequeue()
}

// Ready becomes readable after at least one successful enqueue.
func (q *NotifyingRing[T]) Ready() <-chan struct{} {
	return q.ready
}

// Len returns a momentary snapshot of the number of claimed queue positions.
func (q *NotifyingRing[T]) Len() uint64 {
	return q.ring.len()
}
