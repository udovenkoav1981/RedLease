// Package vtcodec provides a gRPC protobuf codec that uses vtprotobuf helpers
// when available and the standard protobuf codec for other message types.
package vtcodec

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

type vtMessage interface {
	proto.Message
	MarshalVT() ([]byte, error)
	UnmarshalVT(data []byte) error
}

// Codec retains the standard protobuf wire format and content subtype.
// It is installed explicitly, without replacing the process-wide gRPC codec.
type Codec struct{}

var _ encoding.CodecV2 = Codec{}

func (Codec) Name() string {
	return "proto"
}

func (Codec) Marshal(value any) (mem.BufferSlice, error) {
	message, ok := value.(vtMessage)
	if !ok {
		return marshalStandard(value)
	}

	wire, err := message.MarshalVT()
	if err != nil {
		return nil, err
	}
	return mem.BufferSlice{mem.SliceBuffer(wire)}, nil
}

func (Codec) Unmarshal(data mem.BufferSlice, value any) error {
	message, ok := value.(vtMessage)
	if !ok {
		return unmarshalStandard(data, value)
	}

	buffer := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer buffer.Free()
	// UnmarshalVT merges into the receiver; the standard gRPC protobuf codec
	// resets it before decoding. Keep that behavior for reused messages.
	proto.Reset(message)
	return message.UnmarshalVT(buffer.ReadOnlyData())
}

func marshalStandard(value any) (mem.BufferSlice, error) {
	message := standardMessage(value)
	if message == nil {
		return nil, fmt.Errorf("vtcodec: cannot marshal %T: want protobuf message", value)
	}
	wire, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	return mem.BufferSlice{mem.SliceBuffer(wire)}, nil
}

func unmarshalStandard(data mem.BufferSlice, value any) error {
	message := standardMessage(value)
	if message == nil {
		return fmt.Errorf("vtcodec: cannot unmarshal into %T: want protobuf message", value)
	}
	buffer := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer buffer.Free()
	return proto.Unmarshal(buffer.ReadOnlyData(), message)
}

func standardMessage(value any) proto.Message {
	switch message := value.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(message)
	case protoadapt.MessageV2:
		return message
	default:
		return nil
	}
}
