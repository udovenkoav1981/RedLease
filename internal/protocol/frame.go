package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
)

var ErrMalformedFrame = errors.New("malformed RedLease FlatBuffer frame")

const (
	sizePrefixBytes = 4
	// MaxFrameBytes bounds one size-prefixed FlatBuffer, including its prefix.
	MaxFrameBytes     = 1024
	initialBufferSize = 128
)

// NewBuilderSize is the initial capacity used for the small RedLease frames.
const NewBuilderSize = initialBufferSize

// ValidateFrame checks the size prefix and minimum FlatBuffers root size.
func ValidateFrame(frame []byte) error {
	if len(frame) < sizePrefixBytes+flatbuffers.SizeUOffsetT {
		return fmt.Errorf("%w: frame is too short", ErrMalformedFrame)
	}
	payloadSize := binary.LittleEndian.Uint32(frame[:sizePrefixBytes])
	if uint64(payloadSize)+sizePrefixBytes != uint64(len(frame)) {
		return fmt.Errorf("%w: size prefix %d does not match frame size %d", ErrMalformedFrame, payloadSize, len(frame))
	}
	return nil
}
