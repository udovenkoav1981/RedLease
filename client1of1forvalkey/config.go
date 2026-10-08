package client1of1forvalkey

import (
	"errors"
	"log/slog"
	"time"
)

const defaultResponseTimeout = time.Second

// Config controls a client connected to one Valkey server.
type Config struct {
	// ClientID must be unique among simultaneously running client processes.
	ClientID uint32
	// Target is the address of the Valkey server.
	Target string

	// Logger receives structured background lifecycle events from the Client.
	// It is required. The caller retains ownership of the logger and its handler.
	Logger *slog.Logger

	// ResponseTimeout bounds an individual Valkey operation in milliseconds.
	// Zero selects the implementation default.
	ResponseTimeout uint32
}

// Validate checks local values needed to construct a usable client.
func (c Config) Validate() error {
	switch {
	case c.Logger == nil:
		return errors.New("logger must not be nil")
	case c.Target == "":
		return errors.New("valkey target must not be empty")
	default:
		return nil
	}
}
