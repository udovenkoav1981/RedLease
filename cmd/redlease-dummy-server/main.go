// Command redlease-dummy-server is a stateless TCP peer for benchmarking clients.
// It is not a lock server and must never protect real resources.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/udovenkoav1981/RedLease/internal/protocol"
	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

const (
	bufferBytes     = 64 * 1024
	defaultMaxTTLMS = uint64(5000)
)

type responseTemplate struct {
	frame []byte
	root  redleasev1.ServerResponse
}

type templates struct {
	acquire responseTemplate
	renew   responseTemplate
	release responseTemplate
	getTTL  responseTemplate
}

func main() {
	flags := flag.NewFlagSet("redlease-dummy-server", flag.ExitOnError)
	listen := flags.String("listen", "127.0.0.1:50052", "TCP listen address")
	maxTTL := flags.Uint64("max-ttl-ms", defaultMaxTTLMS, "advertised maximum lease TTL in milliseconds")
	_ = flags.Parse(os.Args[1:])
	if *maxTTL == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "max-ttl-ms must be positive")
		os.Exit(2)
	}
	if err := run(*listen, *maxTTL); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(listen string, maxTTL uint64) error {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", listen)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	_, _ = fmt.Fprintf(os.Stderr, "stateless benchmark peer listening on %s; NOT a lock server\n", listener.Addr())
	for {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return acceptErr
		}
		go func() {
			defer func() { _ = connection.Close() }()
			if tcp, ok := connection.(*net.TCPConn); ok {
				_ = tcp.SetNoDelay(true)
			}
			_ = serveConnection(connection, maxTTL)
		}()
	}
}

func serveConnection(connection net.Conn, maxTTL uint64) (result error) {
	defer func() {
		if recover() != nil {
			result = protocol.ErrMalformedFrame
		}
	}()
	reader := bufio.NewReaderSize(connection, bufferBytes)
	writer := bufio.NewWriterSize(connection, bufferBytes)
	responses := newTemplates(maxTTL)
	for {
		frame, err := readFrame(reader, false)
		if err != nil {
			return err
		}
		for {
			if err := processFrame(frame, &responses, writer, maxTTL); err != nil {
				return err
			}
			if _, err := reader.Discard(len(frame)); err != nil {
				return err
			}
			frame, err = readFrame(reader, true)
			if err != nil {
				return err
			}
			if frame == nil {
				break
			}
		}
		if err := connection.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
}

// bufferedOnly prevents waiting for a partial next frame before flushing the
// responses already generated from this read batch.
func readFrame(reader *bufio.Reader, bufferedOnly bool) ([]byte, error) {
	if bufferedOnly && reader.Buffered() < 4 {
		return nil, nil
	}
	prefix, err := reader.Peek(4)
	if err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(prefix)
	if size < 4 || size > protocol.MaxFrameBytes-4 {
		return nil, protocol.ErrMalformedFrame
	}
	if bufferedOnly && reader.Buffered() < int(size)+4 {
		return nil, nil
	}
	return reader.Peek(int(size) + 4)
}

func processFrame(
	frame []byte,
	responses *templates,
	writer *bufio.Writer,
	maxTTL uint64,
) error {
	var request redleasev1.ClientRequest
	request.Init(frame, flatbuffers.GetUOffsetT(frame[4:])+4)
	var response *responseTemplate
	switch request.Operation() {
	case redleasev1.ClientOperationACQUIRE:
		var acquire redleasev1.AcquireRequest
		if request.Acquire(&acquire) == nil {
			return protocol.ErrMalformedFrame
		}
		response = &responses.acquire
		var acquireResponse redleasev1.AcquireResponse
		response.root.Acquire(&acquireResponse)
		acquireResponse.MutateTtlMs(min(acquire.RequestedTtlMs(), maxTTL))
	case redleasev1.ClientOperationRENEW:
		var renew redleasev1.RenewRequest
		if request.Renew(&renew) == nil {
			return protocol.ErrMalformedFrame
		}
		response = &responses.renew
		var renewResponse redleasev1.RenewResponse
		response.root.Renew(&renewResponse)
		renewResponse.MutateTtlMs(min(renew.RequestedTtlMs(), maxTTL))
	case redleasev1.ClientOperationRELEASE:
		var release redleasev1.ReleaseRequest
		if request.Release(&release) == nil {
			return protocol.ErrMalformedFrame
		}
		response = &responses.release
	case redleasev1.ClientOperationGET_TTL:
		response = &responses.getTTL
	default:
		return protocol.ErrMalformedFrame
	}
	if !response.root.MutateRequestId(request.RequestId()) {
		return errors.New("response template is missing request ID")
	}
	_, err := writer.Write(response.frame)
	return err
}

func newTemplates(maxTTL uint64) templates {
	return templates{
		acquire: buildTemplate(redleasev1.ServerResultACQUIRE, maxTTL),
		renew:   buildTemplate(redleasev1.ServerResultRENEW, maxTTL),
		release: buildTemplate(redleasev1.ServerResultRELEASE, maxTTL),
		getTTL:  buildTemplate(redleasev1.ServerResultGET_TTL, maxTTL),
	}
}

func buildTemplate(result redleasev1.ServerResult, maxTTL uint64) responseTemplate {
	builder := flatbuffers.NewBuilder(protocol.NewBuilderSize)
	redleasev1.ServerResponseStart(builder)
	redleasev1.ServerResponseAddRequestId(builder, 1)
	redleasev1.ServerResponseAddResult(builder, result)
	switch result {
	case redleasev1.ServerResultACQUIRE:
		redleasev1.ServerResponseAddAcquire(builder,
			redleasev1.CreateAcquireResponse(builder, redleasev1.LeaseStatusOK, maxTTL))
	case redleasev1.ServerResultRENEW:
		redleasev1.ServerResponseAddRenew(builder, redleasev1.CreateRenewResponse(builder, redleasev1.LeaseStatusOK, maxTTL))
	case redleasev1.ServerResultRELEASE:
		redleasev1.ServerResponseAddRelease(builder, redleasev1.CreateReleaseResponse(builder, redleasev1.LeaseStatusOK))
	case redleasev1.ServerResultGET_TTL:
		redleasev1.ServerResponseAddGetTtl(builder, redleasev1.CreateGetTTLResponse(builder, maxTTL))
	default:
		panic("unsupported response type")
	}
	root := redleasev1.ServerResponseEnd(builder)
	redleasev1.FinishSizePrefixedServerResponseBuffer(builder, root)
	frame := builder.FinishedBytes()
	template := responseTemplate{frame: frame}
	template.root.Init(frame, flatbuffers.GetUOffsetT(frame[4:])+4)
	return template
}
