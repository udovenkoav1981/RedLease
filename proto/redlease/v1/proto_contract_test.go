package redleasev1

import (
	"math"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestLeaseStatusNumericContract(t *testing.T) {
	tests := []struct {
		name   string
		status LeaseStatus
		want   int32
	}{
		{name: "OK", status: LeaseStatus_LEASE_STATUS_OK, want: 0},
		{name: "ALREADY_OWNED", status: LeaseStatus_LEASE_STATUS_ALREADY_OWNED, want: 1},
		{name: "BUSY", status: LeaseStatus_LEASE_STATUS_BUSY, want: 2},
		{name: "STALE", status: LeaseStatus_LEASE_STATUS_STALE, want: 3},
		{name: "NOT_READY", status: LeaseStatus_LEASE_STATUS_NOT_READY, want: 4},
		{name: "KEY_LIMIT_REACHED", status: LeaseStatus_LEASE_STATUS_KEY_LIMIT_REACHED, want: 5},
		{name: "KEY_TOO_LARGE", status: LeaseStatus_LEASE_STATUS_KEY_TOO_LARGE, want: 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := int32(tt.status); got != tt.want {
				t.Fatalf("numeric value = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestClientRequestBinaryRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   *ClientRequest
	}{
		{
			name: "acquire",
			in: &ClientRequest{
				RequestId: 1,
				Operation: &ClientRequest_Acquire{Acquire: &AcquireRequest{
					Key:            []byte{0x00, 0x7f, 0xff},
					LeaseId:        &LeaseID{ClientId: 11, BootId: 12, LeaseSeq: 13},
					RequestedTtlMs: 0,
				}},
			},
		},
		{
			name: "renew",
			in: &ClientRequest{
				RequestId: math.MaxUint64,
				Operation: &ClientRequest_Renew{Renew: &RenewRequest{
					Key:            []byte("lease-key"),
					LeaseId:        &LeaseID{ClientId: math.MaxUint32, BootId: math.MaxUint32, LeaseSeq: math.MaxUint64},
					RequestedTtlMs: math.MaxUint64 - 1,
				}},
			},
		},
		{
			name: "release",
			in: &ClientRequest{
				RequestId: 3,
				Operation: &ClientRequest_Release{Release: &ReleaseRequest{
					Key:     []byte("lease-key"),
					LeaseId: &LeaseID{ClientId: 21, BootId: 22, LeaseSeq: 23},
				}},
			},
		},
		{
			name: "get_ttl",
			in: &ClientRequest{
				RequestId: 4,
				Operation: &ClientRequest_GetTtl{GetTtl: &GetTTLRequest{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := &ClientRequest{}
			binaryRoundTrip(t, tt.in, got)
		})
	}
}

func TestServerResponseBinaryRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   *ServerResponse
	}{
		{
			name: "acquire",
			in: &ServerResponse{
				RequestId: 1,
				Result: &ServerResponse_Acquire{Acquire: &AcquireResponse{
					Status: LeaseStatus_LEASE_STATUS_ALREADY_OWNED,
					TtlMs:  0,
				}},
			},
		},
		{
			name: "renew",
			in: &ServerResponse{
				RequestId: math.MaxUint64,
				Result: &ServerResponse_Renew{Renew: &RenewResponse{
					Status: LeaseStatus_LEASE_STATUS_OK,
					TtlMs:  math.MaxUint64 - 1,
				}},
			},
		},
		{
			name: "release",
			in: &ServerResponse{
				RequestId: 3,
				Result: &ServerResponse_Release{Release: &ReleaseResponse{
					Status: LeaseStatus_LEASE_STATUS_STALE,
				}},
			},
		},
		{
			name: "get_ttl",
			in: &ServerResponse{
				RequestId: 4,
				Result: &ServerResponse_GetTtl{GetTtl: &GetTTLResponse{
					ConfiguredMaxTtlMs: math.MaxUint64,
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := &ServerResponse{}
			binaryRoundTrip(t, tt.in, got)
		})
	}
}

func TestMaxUint64TTLSurvivesBinarySerialization(t *testing.T) {
	tests := []struct {
		name string
		in   proto.Message
		out  proto.Message
	}{
		{
			name: "acquire requested ttl",
			in:   &AcquireRequest{RequestedTtlMs: math.MaxUint64},
			out:  &AcquireRequest{},
		},
		{
			name: "renew requested ttl",
			in:   &RenewRequest{RequestedTtlMs: math.MaxUint64},
			out:  &RenewRequest{},
		},
		{
			name: "acquire response ttl",
			in:   &AcquireResponse{TtlMs: math.MaxUint64},
			out:  &AcquireResponse{},
		},
		{
			name: "renew response ttl",
			in:   &RenewResponse{TtlMs: math.MaxUint64},
			out:  &RenewResponse{},
		},
		{
			name: "configured max ttl",
			in:   &GetTTLResponse{ConfiguredMaxTtlMs: math.MaxUint64},
			out:  &GetTTLResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binaryRoundTrip(t, tt.in, tt.out)
		})
	}
}

func TestRedLeaseServiceDescriptorIsBidirectionalStream(t *testing.T) {
	if got, want := RedLease_ServiceDesc.ServiceName, "redlease.v1.RedLease"; got != want {
		t.Fatalf("service name = %q, want %q", got, want)
	}
	if len(RedLease_ServiceDesc.Methods) != 0 {
		t.Fatalf("unary method count = %d, want 0", len(RedLease_ServiceDesc.Methods))
	}
	if len(RedLease_ServiceDesc.Streams) != 1 {
		t.Fatalf("stream count = %d, want 1", len(RedLease_ServiceDesc.Streams))
	}

	stream := RedLease_ServiceDesc.Streams[0]
	if got, want := stream.StreamName, "LeaseStream"; got != want {
		t.Errorf("stream name = %q, want %q", got, want)
	}
	if !stream.ClientStreams {
		t.Error("LeaseStream is not client-streaming")
	}
	if !stream.ServerStreams {
		t.Error("LeaseStream is not server-streaming")
	}

	service := File_redlease_v1_redlease_proto.Services().ByName(protoreflect.Name("RedLease"))
	if service == nil {
		t.Fatal("protobuf service descriptor RedLease not found")
	}
	method := service.Methods().ByName(protoreflect.Name("LeaseStream"))
	if method == nil {
		t.Fatal("protobuf method descriptor LeaseStream not found")
	}
	if !method.IsStreamingClient() || !method.IsStreamingServer() {
		t.Fatalf(
			"LeaseStream streaming shape = (client=%t, server=%t), want (true, true)",
			method.IsStreamingClient(),
			method.IsStreamingServer(),
		)
	}
	if got, want := method.Input().FullName(), protoreflect.FullName("redlease.v1.ClientRequest"); got != want {
		t.Errorf("input type = %q, want %q", got, want)
	}
	if got, want := method.Output().FullName(), protoreflect.FullName("redlease.v1.ServerResponse"); got != want {
		t.Errorf("output type = %q, want %q", got, want)
	}
}

func binaryRoundTrip(t *testing.T, in, out proto.Message) {
	t.Helper()

	wire, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := proto.Unmarshal(wire, out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round trip mismatch:\ninput:  %v\noutput: %v", in, out)
	}
}
