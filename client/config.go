package client

import (
	"errors"
	"fmt"
	"log/slog"
)

// ServerConfig identifies one independent lock-server TCP endpoint.
type ServerConfig struct {
	Target string
}

// Config controls a RedLease client process. Servers must contain exactly the
// number of lock-servers selected by Quorum.
type Config struct {
	ClientID uint32
	Quorum   Quorum
	Servers  []ServerConfig

	// Logger receives structured background lifecycle events from the Client.
	// It is required. The caller retains ownership of the logger and its handler.
	Logger *slog.Logger

	// ResponseTimeout bounds each individual server response in milliseconds.
	// Zero selects the implementation default.
	ResponseTimeout uint32
}

// Validate checks local values needed to construct a usable client. Cluster
// membership and ID uniqueness remain deployment responsibilities.
func (c Config) Validate() error {
	if c.Logger == nil {
		return errors.New("logger must not be nil")
	}
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
