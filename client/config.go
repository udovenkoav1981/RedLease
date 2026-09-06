package client

import (
	"fmt"

	"google.golang.org/grpc"
)

// ServerConfig identifies one independent lock-server.
//
// DialOptions must include the transport credentials appropriate for the
// deployment. Tests and trusted local deployments may explicitly use
// insecure credentials.
type ServerConfig struct {
	Target      string
	DialOptions []grpc.DialOption
}

// Config controls a RedLease client process. Servers must contain exactly the
// number of lock-servers selected by Quorum.
type Config struct {
	ClientID uint32
	Quorum   Quorum
	Servers  []ServerConfig

	// ResponseTimeout bounds each individual server response in milliseconds.
	// Zero selects the implementation default.
	ResponseTimeout uint32
}

// Validate checks local values needed to construct a usable client. Cluster
// membership and ID uniqueness remain deployment responsibilities.
func (c Config) Validate() error {
	serverCount, _, valid := c.Quorum.parameters()
	if !valid {
		return fmt.Errorf("unsupported quorum configuration %d", uint8(c.Quorum))
	}
	if len(c.Servers) != serverCount {
		return fmt.Errorf(
			"quorum configuration %d requires %d servers, got %d",
			uint8(c.Quorum),
			serverCount,
			len(c.Servers),
		)
	}
	for index, server := range c.Servers {
		if server.Target == "" {
			return fmt.Errorf("server %d target is empty", index)
		}
	}
	return nil
}
