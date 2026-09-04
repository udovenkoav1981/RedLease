package client

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
)

const (
	// ServerCount is fixed by the RedLease 3/5 quorum architecture.
	ServerCount = 5

	defaultResponseTimeout = time.Second
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

// Config controls a RedLease client process. Servers is an array so an
// incorrectly sized cluster cannot be passed to the client API.
type Config struct {
	ClientID uint32
	Servers  [ServerCount]ServerConfig

	// ResponseTimeout bounds each individual server response. Zero selects the
	// implementation default.
	ResponseTimeout time.Duration
}

// Validate checks local values needed to construct a usable client. Cluster
// membership and ID uniqueness remain deployment responsibilities.
func (c Config) Validate() error {
	if c.ResponseTimeout < 0 {
		return errors.New("response timeout must not be negative")
	}
	for index, server := range c.Servers {
		if server.Target == "" {
			return fmt.Errorf("server %d target is empty", index)
		}
	}
	return nil
}
