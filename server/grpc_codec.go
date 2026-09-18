package server

import (
	"google.golang.org/grpc"

	"github.com/udovenkoav1981/RedLease/internal/vtcodec"
)

// VTProtoServerOption enables vtprotobuf serialization for RedLease messages.
// Pass it to grpc.NewServer before registering the RedLease service. It also
// applies to other services on that gRPC server; their messages fall back to
// the standard protobuf codec if they do not have vtprotobuf helpers.
func VTProtoServerOption() grpc.ServerOption {
	return grpc.ForceServerCodecV2(vtcodec.Codec{})
}
