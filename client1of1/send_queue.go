package client1of1

import "sync/atomic"

const sendQueueCapacity = 16 * 1024

// requestRing is a bounded multi-producer, single-consumer FIFO. A producer
// claims a position with tail, then publishes its request through the slot's
// sequence. The consumer never reads a claimed but unpublished slot.
type requestRing struct {
	head  atomic.Uint64
	_     [56]byte // Keep the frequently written head and tail on separate cache lines.
	tail  atomic.Uint64
	_     [56]byte
	slots [sendQueueCapacity]requestRingSlot
}

type requestRingSlot struct {
	sequence atomic.Uint64
	request  *outboundConnectionRequest
}

func newRequestRing() *requestRing {
	queue := &requestRing{}
	for index := range &queue.slots {
		queue.slots[index].sequence.Store(uint64(index)) //nolint:gosec // index is in [0, sendQueueCapacity).
	}
	return queue
}

func (q *requestRing) tryEnqueue(request *outboundConnectionRequest) bool {
	for {
		position := q.tail.Load()
		slot := &q.slots[position&(sendQueueCapacity-1)]
		sequence := slot.sequence.Load()
		if sequence == position {
			if q.tail.CompareAndSwap(position, position+1) {
				slot.request = request
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

// tryDequeue must be called by exactly one consumer at a time.
func (q *requestRing) tryDequeue() (*outboundConnectionRequest, bool) {
	position := q.head.Load()
	slot := &q.slots[position&(sendQueueCapacity-1)]
	if slot.sequence.Load() != position+1 {
		return nil, false
	}
	request := slot.request
	slot.request = nil
	q.head.Store(position + 1)
	slot.sequence.Store(position + sendQueueCapacity)
	return request, true
}

func (q *requestRing) len() uint64 {
	return q.tail.Load() - q.head.Load()
}
