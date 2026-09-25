package mpscring

import "sync/atomic"

const minimumCapacity = 2

// ring is a bounded multi-producer, single-consumer FIFO. A producer claims a
// position with tail, then publishes its value through the slot sequence. The
// consumer never reads a claimed but unpublished slot.
type ring[T any] struct {
	head     atomic.Uint64
	_        [56]byte // Keep the frequently written head and tail on separate cache lines.
	tail     atomic.Uint64
	_        [56]byte
	slots    []slot[T]
	mask     uint64
	capacity uint64
}

type slot[T any] struct {
	sequence atomic.Uint64
	value    T
}

// newRing creates an empty ring. Capacity must be a power of two greater than
// one so the sequence algorithm can distinguish full and empty slots.
func newRing[T any](capacity int) *ring[T] {
	if capacity < minimumCapacity || capacity&(capacity-1) != 0 {
		panic("mpscring: capacity must be a power of two greater than one")
	}
	queue := &ring[T]{
		slots:    make([]slot[T], capacity),
		mask:     uint64(capacity - 1),
		capacity: uint64(capacity),
	}
	for index := range queue.slots {
		queue.slots[index].sequence.Store(uint64(index)) //nolint:gosec // Array index is nonnegative.
	}
	return queue
}

// tryEnqueue appends value and reports whether the queue had room for it.
func (q *ring[T]) tryEnqueue(value T) bool {
	for {
		position := q.tail.Load()
		slot := &q.slots[position&q.mask]
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
	slot := &q.slots[position&q.mask]
	if slot.sequence.Load() != position+1 {
		var zero T
		return zero, false
	}
	value := slot.value
	var zero T
	slot.value = zero
	q.head.Store(position + 1)
	slot.sequence.Store(position + q.capacity)
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
// It panics if capacity is not a power of two greater than one.
func NewNotifying[T any](capacity int) *NotifyingRing[T] {
	return &NotifyingRing[T]{
		ring:  newRing[T](capacity),
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

// Capacity returns the maximum number of elements held by the ring.
func (q *NotifyingRing[T]) Capacity() int {
	return len(q.ring.slots)
}
