package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
)

func TestRequestRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []Request{
		{RequestID: 1, Operation: OperationAcquire, Key: 2, ClientID: 3, BootID: 4, LeaseSequence: 5, RequestedTTLMS: 6},
		{RequestID: 7, Operation: OperationRenew, Key: 8, ClientID: 9, BootID: 10, LeaseSequence: 11, RequestedTTLMS: 12},
		{RequestID: 13, Operation: OperationRelease, Key: 14, ClientID: 15, BootID: 16, LeaseSequence: 17},
		{RequestID: 18, Operation: OperationGetTTL},
	}
	for _, want := range tests {
		frame, err := EncodeRequest(flatbuffers.NewBuilder(NewBuilderSize), want)
		if err != nil {
			t.Fatalf("encode %+v: %v", want, err)
		}
		got, err := DecodeRequest(frame)
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
	}
}

func TestResponseRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []Response{
		{RequestID: 1, Operation: OperationAcquire, Status: StatusAlreadyOwned, TTLMS: 2},
		{RequestID: 3, Operation: OperationRenew, Status: StatusStale, TTLMS: 4},
		{RequestID: 5, Operation: OperationRelease, Status: StatusOK},
		{RequestID: 6, Operation: OperationGetTTL, Status: StatusOK, TTLMS: 7},
	}
	for _, want := range tests {
		frame, err := EncodeResponse(flatbuffers.NewBuilder(NewBuilderSize), want)
		if err != nil {
			t.Fatalf("encode %+v: %v", want, err)
		}
		got, err := DecodeResponse(frame)
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
	}
}

func TestFrameReader(t *testing.T) {
	t.Parallel()
	want, err := EncodeRequest(flatbuffers.NewBuilder(NewBuilderSize), Request{Operation: OperationGetTTL})
	if err != nil {
		t.Fatal(err)
	}
	var reader FrameReader
	got, err := reader.ReadFrame(bytes.NewReader(want))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("frame differs: %x != %x", got, want)
	}
}

func TestFrameReaderRejectsInvalidPayloadSize(t *testing.T) {
	t.Parallel()
	for _, payloadSize := range []uint32{0, MaxFrameBytes, ^uint32(0)} {
		var prefix [sizePrefixBytes]byte
		binary.LittleEndian.PutUint32(prefix[:], payloadSize)
		var reader FrameReader
		if _, err := reader.ReadFrame(bytes.NewReader(prefix[:])); !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("payload size %d error = %v, want ErrMalformedFrame", payloadSize, err)
		}
	}
}

func TestFrameReaderRejectsTruncatedPayload(t *testing.T) {
	t.Parallel()
	var frame [sizePrefixBytes + 1]byte
	binary.LittleEndian.PutUint32(frame[:sizePrefixBytes], 2)
	var reader FrameReader
	if _, err := reader.ReadFrame(bytes.NewReader(frame[:])); !errors.Is(err, io.ErrUnexpectedEOF) {
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
		if _, err := DecodeRequest(frame); !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("DecodeRequest(%x) error = %v", frame, err)
		}
	}
}
