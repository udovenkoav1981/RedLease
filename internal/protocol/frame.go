package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// MaxFrameBytes bounds one size-prefixed FlatBuffer, including its prefix.
	MaxFrameBytes     = 1024
	initialBufferSize = 128
)

// NewBuilderSize is the initial capacity used for the small RedLease frames.
const NewBuilderSize = initialBufferSize

// FrameReader owns reusable storage for one size-prefixed FlatBuffer.
type FrameReader struct {
	buffer [MaxFrameBytes]byte
}

func (r *FrameReader) ReadFrame(reader io.Reader) ([]byte, error) {
	prefix := r.buffer[:sizePrefixBytes]
	if _, err := io.ReadFull(reader, prefix); err != nil {
		return nil, err
	}
	payloadSize := binary.LittleEndian.Uint32(prefix)
	if payloadSize == 0 || payloadSize > uint32(len(r.buffer)-sizePrefixBytes) {
		return nil, fmt.Errorf("%w: payload size %d exceeds limit", ErrMalformedFrame, payloadSize)
	}
	frameSize := sizePrefixBytes + int(payloadSize)
	frame := r.buffer[:frameSize]
	if _, err := io.ReadFull(reader, frame[sizePrefixBytes:]); err != nil {
		return nil, err
	}
	return frame, nil
}

func WriteFrame(writer io.Writer, frame []byte) error {
	for len(frame) != 0 {
		written, err := writer.Write(frame)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrNoProgress
		}
		if written < 0 || written > len(frame) {
			return errors.New("invalid frame write count")
		}
		frame = frame[written:]
	}
	return nil
}
