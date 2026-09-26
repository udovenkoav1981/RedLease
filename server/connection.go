package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

var errServerClosed = errors.New("server is closed")

const (
	initialAcceptRetryDelay = 5 * time.Millisecond
	maximumAcceptRetryDelay = time.Second
	responseQueueCapacity   = 4096
)

type connectionSession struct {
	server *Server
	conn   net.Conn
	ctx    context.Context //nolint:containedctx // Session owns this connection-scoped context.

	respQueue     *mpscring.NotifyingRing[*outboundResponse]
	responsesDone chan struct{}
	requestsDone  chan struct{}
	recvDone      chan error
	pending       sync.WaitGroup
}

func (s *Server) runListener(listener net.Listener) {
	defer s.wg.Done()

	var retryDelay time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			temporary, ok := err.(interface{ Temporary() bool })
			if ok && temporary.Temporary() {
				if retryDelay == 0 {
					retryDelay = initialAcceptRetryDelay
				} else {
					retryDelay = min(retryDelay*2, maximumAcceptRetryDelay)
				}
				s.logger.Warn(
					"temporary listener accept error",
					slog.Any("error", err),
					slog.Duration("retry_delay", retryDelay),
				)
				time.Sleep(retryDelay)
				continue
			}
			if s.unavailableError() == nil {
				s.fail(fmt.Errorf("accept RedLease connection: %w", err))
			}
			return
		}
		retryDelay = 0

		// register connection
		s.connectionMu.Lock()
		if s.unavailableError() != nil {
			s.connectionMu.Unlock()
			_ = conn.Close()
			return
		}
		s.connections[conn] = struct{}{}
		s.connectionWG.Add(1)
		s.connectionMu.Unlock()

		go s.runConnection(conn)
	}
}

func (s *Server) runConnection(conn net.Conn) {
	defer s.connectionWG.Done()
	defer func() {
		_ = conn.Close()
		s.connectionMu.Lock()
		delete(s.connections, conn)
		s.connectionMu.Unlock()
	}()

	if s.unavailableError() != nil {
		return
	}
	s.activeConnections.Add(1)

	ctx, cancel := context.WithCancel(s.ctx)
	session := &connectionSession{
		server:        s,
		conn:          conn,
		ctx:           ctx,
		respQueue:     mpscring.NewNotifying[*outboundResponse](responseQueueCapacity),
		responsesDone: make(chan struct{}),
		requestsDone:  make(chan struct{}),
		recvDone:      make(chan error, 1),
	}
	go session.receiveRequests()

	writer := transport.NewFrameWriter(conn)
	err := session.sendResponses(writer)
	cancel()
	_ = conn.Close()
	<-session.requestsDone
	go session.discardResponses()
	s.activeConnections.Add(-1)
	if err != nil && s.ctx.Err() == nil {
		s.logger.Debug("connection closed", "remote_address", conn.RemoteAddr(), "error", err)
	}
}

func (s *Server) closeConnections() {
	s.connectionMu.Lock()
	defer s.connectionMu.Unlock()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for conn := range s.connections {
		_ = conn.Close()
	}
}

func (s *connectionSession) sendResponses(writer *transport.FrameWriter) error {
	recvDone := s.recvDone

	for {
		response, ok := s.respQueue.TryDequeue()
		if !ok {
			select {
			case <-s.respQueue.Ready():
				continue
			case <-s.responsesDone:
				response, ok = s.respQueue.TryDequeue()
				if !ok {
					return s.server.unavailableErrorUnlessClosed()
				}
			case err := <-recvDone:
				if err != nil && !errors.Is(err, io.EOF) {
					return err
				}
				recvDone = nil
				continue
			case <-s.ctx.Done():
				return s.server.unavailableErrorUnlessClosed()
			}
		}

		for {
			if err := s.bufferResponse(writer, response); err != nil {
				return err
			}
			response, ok = s.respQueue.TryDequeue()
			if ok {
				continue
			}
			if err := writer.Flush(); err != nil {
				if s.server.ctx.Err() == nil {
					return fmt.Errorf("flush response batch: %w", err)
				}
				return s.server.unavailableErrorUnlessClosed()
			}
			break
		}
	}
}

func (s *connectionSession) bufferResponse(
	writer *transport.FrameWriter,
	response *outboundResponse,
) error {
	defer s.server.recycleOutboundResponse(response)
	if err := s.server.unavailableError(); err != nil {
		return err
	}
	if err := writer.BufferFrame(response.message.Table().Bytes); err != nil {
		return fmt.Errorf("buffer response: %w", err)
	}
	return nil
}

func (s *connectionSession) discardResponses() {
	<-s.responsesDone
	for {
		response, ok := s.respQueue.TryDequeue()
		if !ok {
			return
		}
		s.server.recycleOutboundResponse(response)
	}
}

func (s *connectionSession) receiveRequests() {
	defer close(s.requestsDone)
	defer func() {
		go func() {
			s.pending.Wait()
			close(s.responsesDone)
		}()
	}()

	reader := transport.NewFrameReader(s.conn)
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			s.recvDone <- err
			return
		}
		if err := s.server.unavailableError(); err != nil {
			s.recvDone <- err
			return
		}

		if err := s.processRequest(frame); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			s.recvDone <- err
			return
		}
	}
}

func (s *connectionSession) enqueueResponse(response protocol.Response) bool {
	outbound, err := s.server.newOutboundResponse(response)
	if err != nil {
		s.server.fail(fmt.Errorf("encode response: %w", err))
		return false
	}
	for !s.respQueue.TryEnqueue(outbound) {
		select {
		case <-s.ctx.Done():
			s.server.recycleOutboundResponse(outbound)
			return false
		default:
			runtime.Gosched()
		}
	}
	return true
}

func (s *Server) unavailableError() error {
	switch serverPhase(s.phase.Load()) {
	case phaseFailed:
		return ErrServerFailed
	case phaseClosed:
		return errServerClosed
	default:
		return nil
	}
}

func (s *Server) unavailableErrorUnlessClosed() error {
	if serverPhase(s.phase.Load()) == phaseClosed {
		return nil
	}
	return s.unavailableError()
}

func (s *connectionSession) processRequest(frame []byte) (err error) {
	// десериализатор FlatBuffers не валидирует offset и падает в панику если в пакете мусор
	// поэтому тут recover
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", transport.ErrMalformedFrame, recovered)
		}
	}()
	request := redleasev1.GetSizePrefixedRootAsClientRequest(frame, 0)
	var decoded operation
	switch request.Operation() {
	case redleasev1.ClientOperationACQUIRE:
		var acquire redleasev1.AcquireRequest
		if request.Acquire(&acquire) == nil {
			return fmt.Errorf("%w: Acquire payload is missing", transport.ErrMalformedFrame)
		}
		decoded = operation{
			requestID: request.RequestId(),
			kind:      operationAcquire,
			key:       acquire.Key(),
			leaseID: leaseID{
				clientID: acquire.ClientId(),
				bootID:   acquire.BootId(),
				leaseSeq: acquire.LeaseSeq(),
			},
			requestedTTLMS: acquire.RequestedTtlMs(),
		}

	case redleasev1.ClientOperationRENEW:
		var renew redleasev1.RenewRequest
		if request.Renew(&renew) == nil {
			return fmt.Errorf("%w: Renew payload is missing", transport.ErrMalformedFrame)
		}
		decoded = operation{
			requestID: request.RequestId(),
			kind:      operationRenew,
			key:       renew.Key(),
			leaseID: leaseID{
				clientID: renew.ClientId(),
				bootID:   renew.BootId(),
				leaseSeq: renew.LeaseSeq(),
			},
			requestedTTLMS: renew.RequestedTtlMs(),
		}

	case redleasev1.ClientOperationRELEASE:
		var release redleasev1.ReleaseRequest
		if request.Release(&release) == nil {
			return fmt.Errorf("%w: Release payload is missing", transport.ErrMalformedFrame)
		}
		decoded = operation{
			requestID: request.RequestId(),
			kind:      operationRelease,
			key:       release.Key(),
			leaseID: leaseID{
				clientID: release.ClientId(),
				bootID:   release.BootId(),
				leaseSeq: release.LeaseSeq(),
			},
		}

	case redleasev1.ClientOperationGET_TTL:
		if !s.enqueueResponse(protocol.Response{
			RequestID: request.RequestId(),
			Operation: redleasev1.ClientOperationGET_TTL,
			Status:    redleasev1.LeaseStatusOK,
			TTLMS:     s.server.config.MaxTTL,
		}) {
			return context.Canceled
		}
		return nil

	default:
		return fmt.Errorf("%w: unsupported request operation %d", transport.ErrMalformedFrame,
			request.Operation(),
		)
	}

	if serverPhase(s.server.phase.Load()) == phaseQuarantine {
		if !s.enqueueResponse(statusResponse(decoded, redleasev1.LeaseStatusNOT_READY)) {
			return context.Canceled
		}
		return nil
	}

	decoded.session = s
	s.pending.Add(1)
	if !s.server.dispatch(s.ctx.Done(), decoded) {
		s.pending.Done()
		return context.Canceled
	}
	return nil
}
