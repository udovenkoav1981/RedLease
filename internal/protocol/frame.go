package protocol

import (
	"bufio"
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

// FrameReader returns size-prefixed FlatBuffers directly from a bufio.Reader.
// A returned frame remains valid only until the next ReadFrame call.
type FrameReader struct {
	pendingBytes int
}

func (r *FrameReader) ReadFrame(reader *bufio.Reader) ([]byte, error) {
	if r.pendingBytes != 0 {
		if _, err := reader.Discard(r.pendingBytes); err != nil {
			return nil, err
		}
		r.pendingBytes = 0
	}

	prefix, err := reader.Peek(sizePrefixBytes)
	if err != nil {
		return nil, frameReadError(prefix, err)
	}
	payloadSize := binary.LittleEndian.Uint32(prefix)
	if payloadSize == 0 || payloadSize > uint32(MaxFrameBytes-sizePrefixBytes) {
		return nil, fmt.Errorf("%w: payload size %d exceeds limit", ErrMalformedFrame, payloadSize)
	}
	frameSize := sizePrefixBytes + int(payloadSize)
	frame, err := reader.Peek(frameSize)
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
