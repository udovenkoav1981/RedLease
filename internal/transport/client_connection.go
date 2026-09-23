package transport

import (
	"context"
	"fmt"
	"net"
)

// ClientConnection permits one buffered writer goroutine and one concurrent reader
// goroutine. RedLease clients enforce that ownership model.
type ClientConnection struct {
	Conn   net.Conn
	Writer *FrameWriter
	Reader *FrameReader
}

func Dial(ctx context.Context, target string) (*ClientConnection, error) {
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
	return NewClientConnection(conn), nil
}

func NewClientConnection(conn net.Conn) *ClientConnection {
	return &ClientConnection{
		Conn:   conn,
		Reader: NewFrameReader(conn),
		Writer: NewFrameWriter(conn),
	}
}
