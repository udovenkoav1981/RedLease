package client

import (
	"time"
)

// Release immediately makes the local lease invalid and queues an idempotent
// best-effort Release to all configured replicas. Repeated calls do nothing.
func (l *Lease) Release() {
	l.stateMu.Lock()
	if l.lifecycle != leaseActive {
		l.stateMu.Unlock()
		return
	}
	l.lifecycle = leaseReleased
	l.validUntil = time.Time{}
	clear(l.confirmedUntil[:])
	l.stateMu.Unlock()

	l.healingBatches.Wait()
	l.client.releaseAll(l.key, l.sequence)
	close(l.releaseDone)
}

// releaseAll makes one best-effort enqueue attempt on every replica. Release
// responses are intentionally ignored; an undelivered lease remains TTL
// bounded on the server.
func (c *Client) releaseAll(key, sequence uint64) {
	for replica := range c.replicas {
		request := c.newReleaseRequest(key, sequence)
		_ = c.replicas[replica].submitNoResponse(request)
	}
}
