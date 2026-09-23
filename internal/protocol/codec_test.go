package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
)

func TestResponseRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []Response{
		{RequestID: 1, Operation: redleasev1.ClientOperationACQUIRE, Status: redleasev1.LeaseStatusALREADY_OWNED, TTLMS: 2},
		{RequestID: 3, Operation: redleasev1.ClientOperationRENEW, Status: redleasev1.LeaseStatusSTALE, TTLMS: 4},
		{RequestID: 5, Operation: redleasev1.ClientOperationRELEASE, Status: redleasev1.LeaseStatusOK},
		{RequestID: 6, Operation: redleasev1.ClientOperationGET_TTL, Status: redleasev1.LeaseStatusOK, TTLMS: 7},
	}
	for _, want := range tests {
		frame := testResponseFrame(want)
		got, err := DecodeResponse(frame)
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
	}
}

func testResponseFrame(response Response) []byte {
	builder := flatbuffers.NewBuilder(NewBuilderSize)
	redleasev1.ServerResponseStart(builder)
	redleasev1.ServerResponseAddRequestId(builder, response.RequestID)
	switch response.Operation {
	case redleasev1.ClientOperationACQUIRE:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultACQUIRE)
		redleasev1.ServerResponseAddAcquire(builder, redleasev1.CreateAcquireResponse(
			builder, response.Status, response.TTLMS,
		))
	case redleasev1.ClientOperationRENEW:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultRENEW)
		redleasev1.ServerResponseAddRenew(builder, redleasev1.CreateRenewResponse(
			builder, response.Status, response.TTLMS,
		))
	case redleasev1.ClientOperationRELEASE:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultRELEASE)
		redleasev1.ServerResponseAddRelease(builder, redleasev1.CreateReleaseResponse(
			builder, response.Status,
		))
	case redleasev1.ClientOperationGET_TTL:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultGET_TTL)
		redleasev1.ServerResponseAddGetTtl(builder, redleasev1.CreateGetTTLResponse(
			builder, response.TTLMS,
		))
	case redleasev1.ClientOperationNONE:
		return nil
	}
	root := redleasev1.ServerResponseEnd(builder)
	redleasev1.FinishSizePrefixedServerResponseBuffer(builder, root)
	return builder.FinishedBytes()
}

func TestFrameReader(t *testing.T) {
	t.Parallel()
	builder := flatbuffers.NewBuilder(NewBuilderSize)
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddOperation(builder, redleasev1.ClientOperationGET_TTL)
	root := redleasev1.ClientRequestEnd(builder)
	redleasev1.FinishSizePrefixedClientRequestBuffer(builder, root)
	want := builder.FinishedBytes()
	var reader FrameReader
	got, err := reader.ReadFrame(bufio.NewReader(bytes.NewReader(want)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("frame differs: %x != %x", got, want)
	}
	if err := ValidateFrame(got); err != nil {
		t.Fatalf("validate frame: %v", err)
	}
}

func TestFrameReaderRejectsInvalidPayloadSize(t *testing.T) {
	t.Parallel()
	for _, payloadSize := range []uint32{0, MaxFrameBytes, ^uint32(0)} {
		var prefix [sizePrefixBytes]byte
		binary.LittleEndian.PutUint32(prefix[:], payloadSize)
		var reader FrameReader
		if _, err := reader.ReadFrame(bufio.NewReader(bytes.NewReader(prefix[:]))); !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("payload size %d error = %v, want ErrMalformedFrame", payloadSize, err)
		}
	}
}

func TestFrameReaderRejectsTruncatedPayload(t *testing.T) {
	t.Parallel()
	var frame [sizePrefixBytes + 1]byte
	binary.LittleEndian.PutUint32(frame[:sizePrefixBytes], 2)
	var reader FrameReader
	if _, err := reader.ReadFrame(bufio.NewReader(bytes.NewReader(frame[:]))); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated payload error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestWriteFrameHandlesPartialWrites(t *testing.T) {
	t.Parallel()
	writer := &limitedWriter{maximum: 2}
	want := []byte{1, 2, 3, 4, 5}
	if err := WriteFrame(writer, want); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if !bytes.Equal(writer.buffer.Bytes(), want) {
		t.Fatalf("written frame = %v, want %v", writer.buffer.Bytes(), want)
	}
}

type limitedWriter struct {
	buffer  bytes.Buffer
	maximum int
}

func (w *limitedWriter) Write(value []byte) (int, error) {
	return w.buffer.Write(value[:min(len(value), w.maximum)])
}

func TestMalformedFrame(t *testing.T) {
	t.Parallel()
	for _, frame := range [][]byte{nil, {1, 0, 0, 0, 0}, {255, 0, 0, 0, 0, 0, 0, 0}} {
		if err := ValidateFrame(frame); !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("ValidateFrame(%x) error = %v", frame, err)
		}
	}
}
