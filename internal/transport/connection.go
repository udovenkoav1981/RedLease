// Package transport implements buffered frame I/O and client TCP connections.
// Framing lives in internal/protocol; generated messages live in fbs/redlease/v1.
package transport

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

// TCPWriteTimeout bounds one buffered batch flush.
// A timeout makes the connection unusable; its owner must close it.
const TCPWriteTimeout = time.Second

const (
	connectionReadBufferBytes  = 64 * 1024
	connectionWriteBufferBytes = 64 * 1024
)

// FrameReader reads complete FlatBuffers frames through one connection-scoped
// buffer. It is owned by exactly one reader goroutine.
type FrameReader struct {
	buffer       *bufio.Reader
	pendingBytes int
}

// FrameWriter accumulates complete FlatBuffers frames and flushes them as one
// TCP write batch. It is owned by exactly one writer goroutine.
type FrameWriter struct {
	conn   net.Conn
	buffer *bufio.Writer
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

// BufferFrame copies one complete size-prefixed frame into the write buffer.
// If the frame does not fit, the previously buffered batch is flushed first.
// The frame's backing storage may be reused after this method returns.
func (w *FrameWriter) BufferFrame(frame []byte) error {
	if len(frame) > w.buffer.Size() {
		return fmt.Errorf(
			"frame size %d exceeds write buffer capacity %d",
			len(frame),
			w.buffer.Size(),
		)
	}
	if len(frame) > w.buffer.Available() {
		if err := w.Flush(); err != nil {
			return fmt.Errorf("flush full write buffer: %w", err)
		}
	}
	_, err := w.buffer.Write(frame)
	return err
}

// Flush writes all currently buffered frames to the TCP connection.
func (w *FrameWriter) Flush() error {
	if w.buffer.Buffered() == 0 {
		return nil
	}
	if err := w.conn.SetWriteDeadline(time.Now().Add(TCPWriteTimeout)); err != nil {
		return fmt.Errorf("set TCP write deadline: %w", err)
	}
	return w.buffer.Flush()
}

// ReadFrame reads the next size-prefixed FlatBuffer. One underlying read may
// fill the buffer with multiple frames for subsequent calls.
func (r *FrameReader) ReadFrame() ([]byte, error) {
	if r.pendingBytes != 0 {
		if _, err := r.buffer.Discard(r.pendingBytes); err != nil {
			return nil, err
		}
		r.pendingBytes = 0
	}

	prefix, err := r.buffer.Peek(flatbuffers.SizeUint32)
	if err != nil {
		return nil, frameReadError(prefix, err)
	}
	payloadSize := binary.LittleEndian.Uint32(prefix)
	if payloadSize < uint32(flatbuffers.SizeUOffsetT) ||
		payloadSize > uint32(protocol.MaxFrameBytes-flatbuffers.SizeUint32) {
		return nil, fmt.Errorf("%w: invalid payload size %d", protocol.ErrMalformedFrame, payloadSize)
	}
	frameSize := flatbuffers.SizeUint32 + int(payloadSize)
	frame, err := r.buffer.Peek(frameSize)
	if err != nil {
		return nil, frameReadError(frame, err)
	}
	r.pendingBytes = frameSize
	return frame, nil
}

func frameReadError(partial []byte, err error) error {
	if errors.Is(err, io.EOF) && len(partial) != 0 {
		return io.ErrUnexpectedEOF
	}
	return err
}
