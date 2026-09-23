package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

type observedRequest struct {
	id        uint64
	operation redleasev1.ClientOperation
	key       uint64
	sequence  uint64
}

func TestProduceRequestsPartitionsKeysByConnection(t *testing.T) {
	config := &options{keyCount: 2}
	for _, test := range []struct {
		offset uint64
		want   []uint64
	}{
		{offset: 0, want: []uint64{1, 2, 1}},
		{offset: 2, want: []uint64{3, 4, 3}},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		queue := make(chan requestPair, 3)
		done := make(chan error, 1)
		go func() { done <- produceRequests(ctx, queue, config, test.offset) }()
		for _, want := range test.want {
			select {
			case pair := <-queue:
				if pair.key != want {
					t.Errorf("offset %d: key = %d, want %d", test.offset, pair.key, want)
				}
			case <-time.After(time.Second):
				t.Fatal("producer did not send")
			}
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("producer error = %v", err)
		}
	}
}

func TestConnectionsOptions(t *testing.T) {
	for _, test := range []struct {
		args    []string
		wantErr bool
	}{
		{args: []string{"-connections=1"}},
		{args: []string{"-connections=4", "-keys=10"}},
		{args: []string{"-connections=0"}, wantErr: true},
		{args: []string{"-connections=1025"}, wantErr: true},
		{args: []string{"-connections=2", "-keys=" + strconv.FormatUint(math.MaxUint64, 10)}, wantErr: true},
	} {
		_, err := parseOptions(test.args, io.Discard)
		if (err != nil) != test.wantErr {
			t.Errorf("parseOptions(%v) error = %v, wantErr=%v", test.args, err, test.wantErr)
		}
	}
}

type batchTestConnection struct {
	requests []observedRequest
	flushes  int
	flushed  chan struct{}
}

func (c *batchTestConnection) Write(batch []byte) (int, error) {
	for offset := 0; offset < len(batch); {
		if len(batch)-offset < flatbuffers.SizeUint32 {
			return 0, io.ErrUnexpectedEOF
		}
		frameSize := flatbuffers.SizeUint32 + int(binary.LittleEndian.Uint32(batch[offset:]))
		if frameSize > len(batch)-offset {
			return 0, io.ErrUnexpectedEOF
		}
		request := redleasev1.GetSizePrefixedRootAsClientRequest(batch[offset:offset+frameSize], 0)
		observed := observedRequest{id: request.RequestId(), operation: request.Operation()}
		switch request.Operation() {
		case redleasev1.ClientOperationACQUIRE:
			var acquire redleasev1.AcquireRequest
			if request.Acquire(&acquire) == nil {
				return 0, errors.New("Acquire payload missing")
			}
			observed.key, observed.sequence = acquire.Key(), acquire.LeaseSeq()
		case redleasev1.ClientOperationRELEASE:
			var release redleasev1.ReleaseRequest
			if request.Release(&release) == nil {
				return 0, errors.New("Release payload missing")
			}
			observed.key, observed.sequence = release.Key(), release.LeaseSeq()
		default:
			return 0, errors.New("unexpected request operation")
		}
		c.requests = append(c.requests, observed)
		offset += frameSize
	}
	c.flushes++
	select {
	case c.flushed <- struct{}{}:
	default:
	}
	return len(batch), nil
}

func (*batchTestConnection) Read([]byte) (int, error) {
	return 0, errors.New("Read not used by writer test")
}

func (*batchTestConnection) Close() error                     { return nil }
func (*batchTestConnection) LocalAddr() net.Addr              { return batchTestAddr("local") }
func (*batchTestConnection) RemoteAddr() net.Addr             { return batchTestAddr("remote") }
func (*batchTestConnection) SetDeadline(time.Time) error      { return nil }
func (*batchTestConnection) SetReadDeadline(time.Time) error  { return nil }
func (*batchTestConnection) SetWriteDeadline(time.Time) error { return nil }

type batchTestAddr string

func (a batchTestAddr) Network() string { return "test" }
func (a batchTestAddr) String() string  { return string(a) }

func TestSendRequestsFlushesAvailablePairsAsOneBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := &batchTestConnection{flushed: make(chan struct{}, 1)}
	sendQueue := make(chan requestPair, 2)
	sendQueue <- requestPair{key: 11, sequence: 1}
	sendQueue <- requestPair{key: 22, sequence: 2}
	done := make(chan error, 1)
	go func() { done <- sendRequests(ctx, transport.NewClientConnection(connection), sendQueue, 42, 1000) }()

	select {
	case <-connection.flushed:
	case <-time.After(time.Second):
		t.Fatal("writer did not flush queued requests")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("writer error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer did not stop after cancellation")
	}
	if connection.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", connection.flushes)
	}
	want := []observedRequest{
		{id: 1, operation: redleasev1.ClientOperationACQUIRE, key: 11, sequence: 1},
		{id: 2, operation: redleasev1.ClientOperationRELEASE, key: 11, sequence: 1},
		{id: 3, operation: redleasev1.ClientOperationACQUIRE, key: 22, sequence: 2},
		{id: 4, operation: redleasev1.ClientOperationRELEASE, key: 22, sequence: 2},
	}
	if !slices.Equal(connection.requests, want) {
		t.Fatalf("requests = %+v, want %+v", connection.requests, want)
	}
}
