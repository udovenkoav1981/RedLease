package mpscring

import "sync/atomic"

// Capacity is the fixed number of elements held by a Ring.
const Capacity = 4096

// Ring is a bounded multi-producer, single-consumer FIFO. A producer claims a
// position with tail, then publishes its value through the slot sequence. The
// consumer never reads a claimed but unpublished slot.
type Ring[T any] struct {
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

// New creates an empty Ring.
func New[T any]() *Ring[T] {
	queue := &Ring[T]{}
	for index := range queue.slots {
		queue.slots[index].sequence.Store(uint64(index)) //nolint:gosec // Array index is nonnegative.
	}
	return queue
}

// TryEnqueue appends value and reports whether the queue had room for it.
func (q *Ring[T]) TryEnqueue(value T) bool {
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

// TryDequeue removes the oldest value and reports whether one was available.
// It must be called by exactly one consumer at a time.
func (q *Ring[T]) TryDequeue() (T, bool) {
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

// Len returns a momentary snapshot of the number of claimed queue positions.
func (q *Ring[T]) Len() uint64 {
	return q.tail.Load() - q.head.Load()
}
