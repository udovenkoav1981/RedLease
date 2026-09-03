package client

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// ErrClientClosed is returned when an operation is attempted after Close.
var ErrClientClosed = errors.New("RedLease client closed")

// Client owns one persistent reconnecting stream to each of the five
// configured lock-servers.
type Client struct {
	replicas [ServerCount]*replicaConn

	idGenerator     *leaseIDGenerator
	responseTimeout time.Duration
	wall            wallClock

	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

// New creates a client and starts connecting to all five servers. It does not
// wait for a quorum; callers that need an explicit startup barrier can call
// WaitReady.
func New(config Config) (*Client, error) {
	resolved, err := resolveClientConfig(config)
	if err != nil {
		return nil, err
	}
	idGenerator, err := newLeaseIDGenerator(resolved.clientID)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		idGenerator:     idGenerator,
		responseTimeout: resolved.responseTimeout,
		wall:            systemWallClock{},
		ctx:             ctx,
		cancel:          cancel,
	}

	for index, server := range resolved.servers {
		connection, openErr := grpc.NewClient(server.Target, server.DialOptions...)
		if openErr != nil {
			cancel()
			for previous := range index {
				_ = client.replicas[previous].Close()
			}
			return nil, fmt.Errorf("create connection for server %d: %w", index, openErr)
		}
		client.replicas[index] = newReplicaConn(newGRPCStreamFactory(connection))
	}
	return client, nil
}

// WaitReady waits until at least three server streams are connected. It does
// not perform GetTTL and does not mutate server state.
func (c *Client) WaitReady(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.ctx.Err() != nil {
			return ErrClientClosed
		}

		ready := 0
		cases := make([]reflect.SelectCase, 0, ServerCount+2)
		cases = append(cases,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.ctx.Done())},
		)

		for _, replica := range c.replicas {
			replicaReady, _, changed := replica.readiness()
			if replicaReady {
				ready++
			}
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(changed),
			})
		}
		if ready >= quorumSize {
			return nil
		}

		chosen, _, _ := reflect.Select(cases)
		switch chosen {
		case 0:
			return ctx.Err()
		case 1:
			return ErrClientClosed
		}
	}
}

// Close stops reconnecting streams and releases all client-side resources.
// Server-side leases are not implicitly released and remain TTL bounded.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		var closeErrors []error
		for index, replica := range c.replicas {
			if replica == nil {
				continue
			}
			if err := replica.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close server %d: %w", index, err))
			}
		}
		c.closeErr = errors.Join(closeErrors...)
	})
	return c.closeErr
}
