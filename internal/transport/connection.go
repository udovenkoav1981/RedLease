// Package transport implements the persistent TCP connection used by
// RedLease clients. FlatBuffers framing and messages live in internal/protocol.
package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
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

const (
	connectionReadBufferBytes  = 64 * 1024
	connectionWriteBufferBytes = 64 * 1024
)

// Connection permits one concurrent Send caller and one concurrent Recv
// caller. RedLease connection generations enforce that ownership model.
type Connection struct {
	conn    net.Conn
	builder *flatbuffers.Builder
	reader  *FrameReader
	writer  *FrameWriter
}

// FrameReader reads complete FlatBuffers frames through one connection-scoped
// buffer. It is owned by exactly one reader goroutine.
type FrameReader struct {
	frames protocol.FrameReader
	buffer *bufio.Reader
}

// FrameWriter accumulates complete FlatBuffers frames and flushes them as one
// TCP write batch. It is owned by exactly one writer goroutine.
type FrameWriter struct {
	conn   net.Conn
	buffer *bufio.Writer
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
	return &Connection{
		conn:   conn,
		reader: NewFrameReader(conn),
		writer: NewFrameWriter(conn),
	}
}

// NewFrameReader creates a buffered frame reader for one persistent
// connection.
func NewFrameReader(reader io.Reader) *FrameReader {
	return &FrameReader{
		buffer: bufio.NewReaderSize(reader, connectionReadBufferBytes),
	}
}

// NewFrameWriter creates a buffered writer for one persistent connection.
func NewFrameWriter(conn net.Conn) *FrameWriter {
	return &FrameWriter{
		conn:   conn,
		buffer: bufio.NewWriterSize(conn, connectionWriteBufferBytes),
	}
}

func (c *Connection) Send(request protocol.Request) error {
	if c.builder == nil {
		c.builder = flatbuffers.NewBuilder(protocol.NewBuilderSize)
	}
	frame, err := protocol.EncodeRequest(c.builder, request)
	if err != nil {
		return err
	}
	if err := c.writer.BufferFrame(frame); err != nil {
		return err
	}
	return c.FlushClientRequests()
}

// BufferClientRequest copies an already encoded generated FlatBuffers request
// into the connection's write buffer. The caller may reuse its backing buffer
// after this method returns. FlushClientRequests makes buffered requests
// visible to the TCP connection.
func (c *Connection) BufferClientRequest(request *redleasev1.ClientRequest) error {
	if request == nil {
		return errors.New("FlatBuffers ClientRequest is nil")
	}
	return c.writer.BufferFrame(request.Table().Bytes)
}

// BufferFrame copies one complete size-prefixed frame into the write buffer.
// The frame's backing storage may be reused after this method returns.
func (w *FrameWriter) BufferFrame(frame []byte) error {
	if err := w.conn.SetWriteDeadline(time.Now().Add(TCPWriteTimeout)); err != nil {
		return fmt.Errorf("set TCP write deadline: %w", err)
	}
	return protocol.WriteFrame(w.buffer, frame)
}

// FlushClientRequests writes the current request batch to the TCP connection.
func (c *Connection) FlushClientRequests() error {
	return c.writer.Flush()
}

// Flush writes all currently buffered frames to the TCP connection.
func (w *FrameWriter) Flush() error {
	return w.buffer.Flush()
}

// ReadFrame reads the next size-prefixed FlatBuffer. One underlying read may
// fill the buffer with multiple frames for subsequent calls.
func (r *FrameReader) ReadFrame() ([]byte, error) {
	return r.frames.ReadFrame(r.buffer)
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
	frame, err := c.reader.ReadFrame()
	if err != nil {
		return protocol.Response{}, err
	}
	return protocol.DecodeResponse(frame)
}

func (c *Connection) Close() error {
	return c.conn.Close()
}
