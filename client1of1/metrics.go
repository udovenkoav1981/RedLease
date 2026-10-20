package client1of1

import "github.com/udovenkoav1981/RedLease/clientmetrics"

// MetricsSnapshot returns a lock-free, point-in-time view of client metrics.
// Fields may have been observed at slightly different instants.
func (c *Client) MetricsSnapshot() clientmetrics.Snapshot {
	return c.metrics.Snapshot()
}
