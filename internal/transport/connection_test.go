package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
)

type recordingConn struct {
	bytes.Buffer

	reads          int
	writes         int
	writeDeadlines int
}

func (c *recordingConn) Read(value []byte) (int, error) {
	c.reads++
	return c.Buffer.Read(value)
}

func (c *recordingConn) Write(value []byte) (int, error) {
	c.writes++
	return c.Buffer.Write(value)
}

func (*recordingConn) Close() error                    { return nil }
func (*recordingConn) LocalAddr() net.Addr             { return nil }
func (*recordingConn) RemoteAddr() net.Addr            { return nil }
func (*recordingConn) SetDeadline(time.Time) error     { return nil }
func (*recordingConn) SetReadDeadline(time.Time) error { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error {
	c.writeDeadlines++
	return nil
}

func TestClientConnectionFlushesBufferedRequestsWithOneWrite(t *testing.T) {
	network := &recordingConn{}
	connection := NewClientConnection(network)
	builder := flatbuffers.NewBuilder(InitialBufferSize)

	for requestID := uint64(1); requestID <= 2; requestID++ {
		builder.Reset()
		redleasev1.ClientRequestStart(builder)
		redleasev1.ClientRequestAddRequestId(builder, requestID)
		redleasev1.ClientRequestAddOperation(builder, redleasev1.ClientOperationGET_TTL)
		root := redleasev1.ClientRequestEnd(builder)
		redleasev1.FinishSizePrefixedClientRequestBuffer(builder, root)
		request := redleasev1.GetSizePrefixedRootAsClientRequest(builder.FinishedBytes(), 0)
		if err := connection.BufferClientRequest(request); err != nil {
			t.Fatalf("buffer request %d: %v", requestID, err)
		}
	}
	if network.writes != 0 {
		t.Fatalf("writes before flush = %d, want 0", network.writes)
	}
	if network.writeDeadlines != 0 {
		t.Fatalf("write deadlines before flush = %d, want 0", network.writeDeadlines)
	}
	if err := connection.FlushClientRequests(); err != nil {
		t.Fatalf("flush requests: %v", err)
	}
	if network.writes != 1 {
		t.Fatalf("writes after flush = %d, want 1", network.writes)
	}
	if network.writeDeadlines != 1 {
		t.Fatalf("write deadlines after flush = %d, want 1", network.writeDeadlines)
	}

	reader := NewFrameReader(&network.Buffer)
	for requestID := uint64(1); requestID <= 2; requestID++ {
		frame, err := reader.ReadFrame()
		if err != nil {
			t.Fatalf("read request %d: %v", requestID, err)
		}
		request := redleasev1.GetSizePrefixedRootAsClientRequest(frame, 0)
		if request.RequestId() != requestID {
			t.Fatalf("request ID = %d, want %d", request.RequestId(), requestID)
		}
	}
}

func TestFrameWriterFlushesBeforeFrameExceedsAvailableBuffer(t *testing.T) {
	network := &recordingConn{}
	writer := NewFrameWriter(network)
	frame := make([]byte, connectionWriteBufferBytes/2+1)

	if err := writer.BufferFrame(frame); err != nil {
		t.Fatalf("buffer first frame: %v", err)
	}
	if err := writer.BufferFrame(frame); err != nil {
		t.Fatalf("buffer second frame: %v", err)
	}
	if network.writes != 1 || network.writeDeadlines != 1 {
		t.Fatalf(
			"intermediate flush = %d writes and %d deadlines, want 1 and 1",
			network.writes,
			network.writeDeadlines,
		)
	}

	if err := writer.Flush(); err != nil {
		t.Fatalf("flush second frame: %v", err)
	}
	if network.writes != 2 || network.writeDeadlines != 2 {
		t.Fatalf(
			"final flush = %d writes and %d deadlines, want 2 and 2",
			network.writes,
			network.writeDeadlines,
		)
	}
}

func TestFrameWriterEmptyFlushDoesNotSetDeadline(t *testing.T) {
	network := &recordingConn{}
	writer := NewFrameWriter(network)

	if err := writer.Flush(); err != nil {
		t.Fatalf("flush empty writer: %v", err)
	}
	if network.writes != 0 || network.writeDeadlines != 0 {
		t.Fatalf(
			"empty flush = %d writes and %d deadlines, want 0 and 0",
			network.writes,
			network.writeDeadlines,
		)
	}
}

func TestFrameReaderRejectsInvalidPayloadSize(t *testing.T) {
	t.Parallel()
	for _, payloadSize := range []uint32{0, flatbuffers.SizeUOffsetT - 1, MaxFrameBytes, ^uint32(0)} {
		var prefix [flatbuffers.SizeUint32]byte
		binary.LittleEndian.PutUint32(prefix[:], payloadSize)
		reader := NewFrameReader(bytes.NewReader(prefix[:]))
		if _, err := reader.ReadFrame(); !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("payload size %d error = %v, want ErrMalformedFrame", payloadSize, err)
		}
	}
}

func TestFrameReaderRejectsTruncatedPayload(t *testing.T) {
	t.Parallel()
	var frame [flatbuffers.SizeUint32 + 1]byte
	binary.LittleEndian.PutUint32(frame[:flatbuffers.SizeUint32], flatbuffers.SizeUOffsetT)
	reader := NewFrameReader(bytes.NewReader(frame[:]))
	if _, err := reader.ReadFrame(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated payload error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestFrameReaderReadsBufferedFramesWithOneRead(t *testing.T) {
	network := &recordingConn{}
	builder := flatbuffers.NewBuilder(InitialBufferSize)

	for requestID := uint64(1); requestID <= 2; requestID++ {
		builder.Reset()
		redleasev1.ServerResponseStart(builder)
		redleasev1.ServerResponseAddRequestId(builder, requestID)
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultRELEASE)
		redleasev1.ServerResponseAddRelease(builder, redleasev1.CreateReleaseResponse(
			builder, redleasev1.LeaseStatusOK,
		))
		root := redleasev1.ServerResponseEnd(builder)
		redleasev1.FinishSizePrefixedServerResponseBuffer(builder, root)
		frame := builder.FinishedBytes()
		if _, err := network.Buffer.Write(frame); err != nil {
			t.Fatalf("prepare response %d: %v", requestID, err)
		}
	}

	reader := NewFrameReader(network)
	for requestID := uint64(1); requestID <= 2; requestID++ {
		frame, err := reader.ReadFrame()
		if err != nil {
			t.Fatalf("read response %d: %v", requestID, err)
		}
		response, err := DecodeResponse(frame)
		if err != nil {
			t.Fatalf("decode response %d: %v", requestID, err)
		}
		if response.RequestID != requestID {
			t.Fatalf("response ID = %d, want %d", response.RequestID, requestID)
		}
	}
	if network.reads != 1 {
		t.Fatalf("TCP reads = %d, want 1", network.reads)
	}
}
