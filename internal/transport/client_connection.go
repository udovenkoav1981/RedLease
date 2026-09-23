package transport

import (
	"context"
	"errors"
	"fmt"
	"net"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

// ClientConnection permits one buffered writer goroutine and one concurrent Recv
// goroutine. RedLease clients enforce that ownership model.
type ClientConnection struct {
	conn   net.Conn
	reader *FrameReader
	writer *FrameWriter
}

// LeaseConnection is the client-side transport boundary shared by RedLease
// client implementations. Exactly one goroutine owns buffered writes and one
// goroutine owns reads; Close must unblock both.
type LeaseConnection interface {
	BufferClientRequest(request *redleasev1.ClientRequest) error
	FlushClientRequests() error
	Recv() (protocol.Response, error)
	Close() error
}

var _ LeaseConnection = (*ClientConnection)(nil)

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
		conn:   conn,
		reader: NewFrameReader(conn),
		writer: NewFrameWriter(conn),
	}
}

// BufferClientRequest copies an already encoded generated FlatBuffers request
// into the connection's write buffer. The caller may reuse its backing buffer
// after this method returns. FlushClientRequests makes buffered requests
// visible to the TCP connection.
func (c *ClientConnection) BufferClientRequest(request *redleasev1.ClientRequest) error {
	if request == nil {
		return errors.New("FlatBuffers ClientRequest is nil")
	}
	return c.writer.BufferFrame(request.Table().Bytes)
}

// FlushClientRequests writes the current request batch to the TCP connection.
func (c *ClientConnection) FlushClientRequests() error {
	return c.writer.Flush()
}

func (c *ClientConnection) Recv() (protocol.Response, error) {
	frame, err := c.reader.ReadFrame()
	if err != nil {
		return protocol.Response{}, err
	}
	return protocol.DecodeResponse(frame)
}

func (c *ClientConnection) Close() error {
	return c.conn.Close()
}
