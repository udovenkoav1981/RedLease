package transport

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

type recordingConn struct {
	bytes.Buffer

	reads  int
	writes int
}

func (c *recordingConn) Read(value []byte) (int, error) {
	c.reads++
	return c.Buffer.Read(value)
}

func (c *recordingConn) Write(value []byte) (int, error) {
	c.writes++
	return c.Buffer.Write(value)
}

func (*recordingConn) Close() error                     { return nil }
func (*recordingConn) LocalAddr() net.Addr              { return nil }
func (*recordingConn) RemoteAddr() net.Addr             { return nil }
func (*recordingConn) SetDeadline(time.Time) error      { return nil }
func (*recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (*recordingConn) SetWriteDeadline(time.Time) error { return nil }

func TestConnectionFlushesBufferedClientRequestsWithOneWrite(t *testing.T) {
	network := &recordingConn{}
	connection := NewConnection(network)
	builder := flatbuffers.NewBuilder(protocol.NewBuilderSize)

	for requestID := uint64(1); requestID <= 2; requestID++ {
		frame, err := protocol.EncodeRequest(builder, protocol.Request{
			RequestID: requestID,
			Operation: protocol.OperationGetTTL,
		})
		if err != nil {
			t.Fatalf("encode request %d: %v", requestID, err)
		}
		request := redleasev1.GetSizePrefixedRootAsClientRequest(frame, 0)
		if err := connection.BufferClientRequest(request); err != nil {
			t.Fatalf("buffer request %d: %v", requestID, err)
		}
	}
	if network.writes != 0 {
		t.Fatalf("writes before flush = %d, want 0", network.writes)
	}
	if err := connection.FlushClientRequests(); err != nil {
		t.Fatalf("flush requests: %v", err)
	}
	if network.writes != 1 {
		t.Fatalf("writes after flush = %d, want 1", network.writes)
	}

	reader := NewFrameReader(&network.Buffer)
	for requestID := uint64(1); requestID <= 2; requestID++ {
		frame, err := reader.ReadFrame()
		if err != nil {
			t.Fatalf("read request %d: %v", requestID, err)
		}
		request, err := protocol.DecodeRequest(frame)
		if err != nil {
			t.Fatalf("decode request %d: %v", requestID, err)
		}
		if request.RequestID != requestID {
			t.Fatalf("request ID = %d, want %d", request.RequestID, requestID)
		}
	}
}

func TestFrameReaderReadsBufferedFramesWithOneRead(t *testing.T) {
	network := &recordingConn{}
	builder := flatbuffers.NewBuilder(protocol.NewBuilderSize)

	for requestID := uint64(1); requestID <= 2; requestID++ {
		frame, err := protocol.EncodeResponse(builder, protocol.Response{
			RequestID: requestID,
			Operation: protocol.OperationRelease,
			Status:    protocol.StatusOK,
		})
		if err != nil {
			t.Fatalf("encode response %d: %v", requestID, err)
		}
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
		response, err := protocol.DecodeResponse(frame)
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

func TestWriteFrameTimesOutBlockedTCPWrite(t *testing.T) {
	t.Parallel()
	server, client := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	err := writeFrame(client, []byte{1}, 10*time.Millisecond)
	if err == nil {
		t.Fatal("blocked frame write succeeded")
	}
	var networkError net.Error
	if !errors.As(err, &networkError) || !networkError.Timeout() {
		t.Fatalf("blocked frame write error = %v, want network timeout", err)
	}
}
