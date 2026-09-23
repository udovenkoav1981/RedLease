package protocol

import "errors"

var ErrMalformedFrame = errors.New("malformed RedLease FlatBuffer frame")

const (
	// MaxFrameBytes bounds one size-prefixed FlatBuffer, including its prefix.
	MaxFrameBytes     = 1024
	initialBufferSize = 128
)

// NewBuilderSize is the initial capacity used for the small RedLease frames.
const NewBuilderSize = initialBufferSize
