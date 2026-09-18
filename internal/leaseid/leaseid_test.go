package leaseid

import (
	"testing"
)

func TestNewBootIDAndProtobufConversion(t *testing.T) {
	bootID, err := NewBootID()
	if err != nil {
		t.Fatalf("new boot ID: %v", err)
	}

	id := LeaseID{ClientID: 42, BootID: bootID, Sequence: 1}
	protobuf := id.Protobuf()
	if protobuf.GetClientId() != id.ClientID ||
		protobuf.GetBootId() != id.BootID ||
		protobuf.GetLeaseSeq() != id.Sequence {
		t.Fatalf("unexpected protobuf lease ID: %+v", protobuf)
	}
}
