package main

import (
	"errors"
	"io"
	"net"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

func TestStatelessPeerRespondsToAllOperations(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	done := make(chan error, 1)
	go func() {
		defer func() { _ = serverSide.Close() }()
		done <- serveConnection(serverSide, 1000)
	}()
	connection := transport.NewClientConnection(clientSide)
	defer func() {
		_ = connection.Close()
		if err := <-done; err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
			t.Errorf("server connection: %v", err)
		}
	}()

	requests := []struct {
		operation redleasev1.ClientOperation
		requested uint64
		wantTTL   uint64
	}{
		{redleasev1.ClientOperationACQUIRE, 500, 500},
		{redleasev1.ClientOperationRENEW, 2000, 1000},
		{redleasev1.ClientOperationRELEASE, 0, 0},
		{redleasev1.ClientOperationGET_TTL, 0, 1000},
		{redleasev1.ClientOperationACQUIRE, 0, 0},
	}
	builder := flatbuffers.NewBuilder(transport.InitialBufferSize)
	for index, item := range requests {
		builder.Reset()
		redleasev1.ClientRequestStart(builder)
		redleasev1.ClientRequestAddRequestId(builder, uint64(index+1))
		redleasev1.ClientRequestAddOperation(builder, item.operation)
		switch item.operation {
		case redleasev1.ClientOperationACQUIRE:
			redleasev1.ClientRequestAddAcquire(builder, redleasev1.CreateAcquireRequest(builder, 42, 1, 1, uint64(index+1), item.requested))
		case redleasev1.ClientOperationRENEW:
			redleasev1.ClientRequestAddRenew(builder, redleasev1.CreateRenewRequest(builder, 42, 1, 1, uint64(index+1), item.requested))
		case redleasev1.ClientOperationRELEASE:
			redleasev1.ClientRequestAddRelease(builder, redleasev1.CreateReleaseRequest(builder, 42, 1, 1, uint64(index+1)))
		case redleasev1.ClientOperationGET_TTL:
		default:
			t.Fatalf("unsupported test request operation %d", item.operation)
		}
		root := redleasev1.ClientRequestEnd(builder)
		redleasev1.FinishSizePrefixedClientRequestBuffer(builder, root)
		request := redleasev1.GetSizePrefixedRootAsClientRequest(builder.FinishedBytes(), 0)
		if err := connection.BufferClientRequest(request); err != nil {
			t.Fatal(err)
		}
	}
	if err := connection.FlushClientRequests(); err != nil {
		t.Fatal(err)
	}
	for index, item := range requests {
		response, err := connection.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if response.RequestID != uint64(index+1) || response.Status != redleasev1.LeaseStatusOK || response.TTLMS != item.wantTTL {
			t.Errorf("response %d = %+v; want ID=%d, status OK, TTL=%d", index, response, index+1, item.wantTTL)
		}
	}
}
