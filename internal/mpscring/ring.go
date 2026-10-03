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

// EnqueueTicket identifies a slot reserved by TryStartEnqueue. Its contents
// are intentionally private: a ticket is only valid for one matching
// FinishEnqueue call on the same ring.
type EnqueueTicket struct {
	position uint64
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

// tryStartEnqueue reserves a slot and returns a pointer that the producer can
// fill in place. The slot remains invisible to the consumer until the matching
// finishEnqueue call publishes it. A nil pointer means the ring is full.
func (q *ring[T]) tryStartEnqueue() (*T, EnqueueTicket) {
	for {
		position := q.tail.Load()
		slot := &q.slots[position&q.mask]
		sequence := slot.sequence.Load()
		if sequence == position {
			if q.tail.CompareAndSwap(position, position+1) {
				return &slot.value, EnqueueTicket{position: position}
			}
			continue
		}
		if sequence < position {
			return nil, EnqueueTicket{}
		}
		// Another producer advanced tail since our load; retry its new position.
	}
}

// finishEnqueue publishes the slot identified by ticket to the consumer.
func (q *ring[T]) finishEnqueue(ticket EnqueueTicket) {
	slot := &q.slots[ticket.position&q.mask]
	slot.sequence.Store(ticket.position + 1)
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

// tryStartDequeue returns a pointer to the oldest value without releasing its
// slot. It must be paired with exactly one finishDequeue call after the value
// has been processed. A nil pointer means no value is currently available.
func (q *ring[T]) tryStartDequeue() *T {
	position := q.head.Load()
	slot := &q.slots[position&q.mask]
	if slot.sequence.Load() != position+1 {
		return nil
	}
	return &slot.value
}

// finishDequeue releases the slot returned by the preceding
// tryStartDequeue call.
func (q *ring[T]) finishDequeue() {
	position := q.head.Load()
	slot := &q.slots[position&q.mask]
	var zero T
	slot.value = zero
	q.head.Store(position + 1)
	slot.sequence.Store(position + q.capacity)
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

// TryStartEnqueue reserves a slot and returns a pointer that the producer can
// fill in place. Multiple producers may reserve and fill different slots
// concurrently. A nil pointer means the ring is full; the accompanying ticket
// is then invalid and must not be used.
//
// After a successful call, the producer must call FinishEnqueue exactly once
// with the returned ticket. The pointer must not be retained or accessed after
// that call. An unfinished earlier reservation blocks FIFO consumption of all
// later reservations.
func (q *NotifyingRing[T]) TryStartEnqueue() (*T, EnqueueTicket) {
	return q.ring.tryStartEnqueue()
}

// FinishEnqueue publishes an in-place value prepared after TryStartEnqueue and
// signals the consumer. The ticket must belong to this ring and must be used
// exactly once.
func (q *NotifyingRing[T]) FinishEnqueue(ticket EnqueueTicket) {
	q.ring.finishEnqueue(ticket)
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// TryDequeue removes the oldest value and reports whether one was available.
func (q *NotifyingRing[T]) TryDequeue() (T, bool) {
	return q.ring.tryDequeue()
}

// TryStartDequeue starts an in-place dequeue and returns a pointer to the
// oldest value. It returns nil when no value is currently available. The
// pointer remains valid until FinishDequeue is called.
//
// Exactly one consumer may operate on the ring. After a successful call, that
// consumer must call FinishDequeue exactly once and must not call TryDequeue or
// TryStartDequeue before doing so. The pointer must not be retained after
// FinishDequeue returns.
func (q *NotifyingRing[T]) TryStartDequeue() *T {
	return q.ring.tryStartDequeue()
}

// FinishDequeue completes the in-place dequeue started by TryStartDequeue and
// makes its slot available to producers again.
func (q *NotifyingRing[T]) FinishDequeue() {
	q.ring.finishDequeue()
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
