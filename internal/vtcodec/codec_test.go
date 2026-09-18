package vtcodec_test

import (
	"testing"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/udovenkoav1981/RedLease/internal/vtcodec"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

func TestCodecInteroperatesWithStandardProto(t *testing.T) {
	tests := []struct {
		name string
		in   proto.Message
		out  proto.Message
	}{
		{
			name: "acquire request",
			in: &redleasev1.ClientRequest{
				RequestId: 17,
				Operation: &redleasev1.ClientRequest_Acquire{Acquire: &redleasev1.AcquireRequest{
					Key: []byte("resource/17"),
					LeaseId: &redleasev1.LeaseID{
						ClientId: 1,
						BootId:   2,
						LeaseSeq: 3,
					},
					RequestedTtlMs: 1000,
				}},
			},
			out: &redleasev1.ClientRequest{},
		},
		{
			name: "renew response",
			in: &redleasev1.ServerResponse{
				RequestId: 18,
				Result: &redleasev1.ServerResponse_Renew{Renew: &redleasev1.RenewResponse{
					Status: redleasev1.LeaseStatus_LEASE_STATUS_OK,
					TtlMs:  900,
				}},
			},
			out: &redleasev1.ServerResponse{},
		},
	}

	codec := vtcodec.Codec{}
	if codec.Name() != "proto" {
		t.Fatalf("codec name = %q, want proto", codec.Name())
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// An unknown varint field must survive communication with newer peers.
			tt.in.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			wire, err := codec.Marshal(tt.in)
			if err != nil {
				t.Fatalf("vt marshal: %v", err)
			}
			defer wire.Free()
			if err := proto.Unmarshal(wire.Materialize(), tt.out); err != nil {
				t.Fatalf("standard unmarshal: %v", err)
			}
			if !proto.Equal(tt.in, tt.out) {
				t.Fatalf("vt marshal changed message: %v != %v", tt.in, tt.out)
			}

			standardWire, err := proto.Marshal(tt.in)
			if err != nil {
				t.Fatalf("standard marshal: %v", err)
			}
			if err := codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(standardWire)}, tt.out); err != nil {
				t.Fatalf("vt unmarshal: %v", err)
			}
			if !proto.Equal(tt.in, tt.out) {
				t.Fatalf("vt unmarshal changed message: %v != %v", tt.in, tt.out)
			}
		})
	}
}

func TestCodecUnmarshalResetsReceiver(t *testing.T) {
	codec := vtcodec.Codec{}
	out := &redleasev1.ClientRequest{
		RequestId: 99,
		Operation: &redleasev1.ClientRequest_Release{Release: &redleasev1.ReleaseRequest{
			Key: []byte("old"),
		}},
	}
	in := &redleasev1.ClientRequest{
		RequestId: 1,
		Operation: &redleasev1.ClientRequest_GetTtl{GetTtl: &redleasev1.GetTTLRequest{}},
	}
	wire, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if err := codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(wire)}, out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("decoded message = %v, want %v", out, in)
	}
}

func TestCodecFallsBackForOtherProtoMessages(t *testing.T) {
	codec := vtcodec.Codec{}
	wire, err := codec.Marshal(&emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	defer wire.Free()
	if err := codec.Unmarshal(wire, &emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkCodecMarshal(b *testing.B) {
	message := &redleasev1.ClientRequest{
		RequestId: 42,
		Operation: &redleasev1.ClientRequest_Acquire{Acquire: &redleasev1.AcquireRequest{
			Key:            []byte("resource/42"),
			LeaseId:        &redleasev1.LeaseID{ClientId: 1, BootId: 2, LeaseSeq: 3},
			RequestedTtlMs: 1000,
		}},
	}
	for _, tt := range []struct {
		name  string
		codec encoding.CodecV2
	}{
		{name: "standard", codec: encoding.GetCodecV2("proto")},
		{name: "vtprotobuf", codec: vtcodec.Codec{}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				wire, err := tt.codec.Marshal(message)
				if err != nil {
					b.Fatal(err)
				}
				wire.Free()
			}
		})
	}
}

func BenchmarkCodecUnmarshal(b *testing.B) {
	message := &redleasev1.ServerResponse{
		RequestId: 42,
		Result: &redleasev1.ServerResponse_Acquire{Acquire: &redleasev1.AcquireResponse{
			Status: redleasev1.LeaseStatus_LEASE_STATUS_OK,
			TtlMs:  1000,
		}},
	}
	bytes, err := proto.Marshal(message)
	if err != nil {
		b.Fatal(err)
	}
	wire := mem.BufferSlice{mem.SliceBuffer(bytes)}
	for _, tt := range []struct {
		name  string
		codec encoding.CodecV2
	}{
		{name: "standard", codec: encoding.GetCodecV2("proto")},
		{name: "vtprotobuf", codec: vtcodec.Codec{}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			out := new(redleasev1.ServerResponse)
			b.ReportAllocs()
			for b.Loop() {
				if err := tt.codec.Unmarshal(wire, out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
