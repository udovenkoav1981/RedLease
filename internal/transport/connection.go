// Package transport implements the persistent TCP connection used by
// RedLease clients. FlatBuffers framing and messages live in internal/protocol.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

// TCPWriteTimeout bounds one complete RedLease frame write. A timeout makes
// the connection unusable; its owner closes it and discards that connection
// generation's remaining send queue.
const TCPWriteTimeout = time.Second

// Connection permits one concurrent Send caller and one concurrent Recv
// caller. RedLease connection generations enforce that ownership model.
type Connection struct {
	conn    net.Conn
	builder *flatbuffers.Builder
	reader  protocol.FrameReader
}

func Dial(ctx context.Context, target string) (*Connection, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("enable TCP_NODELAY: %w", err)
		}
	}
	return NewConnection(conn), nil
}

func NewConnection(conn net.Conn) *Connection {
	return &Connection{conn: conn}
}

func (c *Connection) Send(request protocol.Request) error {
	if c.builder == nil {
		c.builder = flatbuffers.NewBuilder(protocol.NewBuilderSize)
	}
	frame, err := protocol.EncodeRequest(c.builder, request)
	if err != nil {
		return err
	}
	return WriteFrame(c.conn, frame)
}

// SendClientRequest writes an already encoded generated FlatBuffers request.
// The caller must not mutate or reuse its backing buffer until this method
// returns.
func (c *Connection) SendClientRequest(request *redleasev1.ClientRequest) error {
	if request == nil {
		return errors.New("FlatBuffers ClientRequest is nil")
	}
	return WriteFrame(c.conn, request.Table().Bytes)
}

// WriteFrame writes one complete frame with the fixed TCP write timeout.
func WriteFrame(conn net.Conn, frame []byte) error {
	return writeFrame(conn, frame, TCPWriteTimeout)
}

func writeFrame(conn net.Conn, frame []byte, timeout time.Duration) error {
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set TCP write deadline: %w", err)
	}
	return protocol.WriteFrame(conn, frame)
}

func (c *Connection) Recv() (protocol.Response, error) {
	frame, err := c.reader.ReadFrame(c.conn)
	if err != nil {
		return protocol.Response{}, err
	}
	return protocol.DecodeResponse(frame)
}

func (c *Connection) Close() error {
	return c.conn.Close()
}
