package client1of1

import (
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc"
)

const defaultResponseTimeout = time.Second

// Milliseconds is the unit used for lease TTL values in the API and protocol.
type Milliseconds uint64

// Config controls a client connected to one lock-server.
type Config struct {
	// ClientID must be unique among simultaneously running client processes.
	ClientID uint32
	// Target is the address of the only lock-server.
	Target string

	// DialOptions must include transport credentials appropriate for the
	// deployment. Trusted local deployments may explicitly use insecure
	// credentials.
	DialOptions []grpc.DialOption

	// Logger receives structured background lifecycle events from the Client.
	// It is required. The caller retains ownership of the logger and its handler.
	Logger *slog.Logger

	// ResponseTimeout bounds an individual server operation in milliseconds.
	// Zero selects the implementation default.
	ResponseTimeout uint32
}

// Validate checks local values needed to construct a usable client.
func (c Config) Validate() error {
	switch {
	case c.Logger == nil:
		return errors.New("logger must not be nil")
	case c.Target == "":
		return errors.New("server target must not be empty")
	default:
		return nil
	}
}
