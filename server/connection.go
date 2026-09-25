package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

var (
	errServerClosed       = errors.New("server is closed")
	errServerNotAccepting = errors.New("server is not accepting work")
)

const (
	initialAcceptRetryDelay  = 5 * time.Millisecond
	maximumAcceptRetryDelay  = time.Second
	maxInFlightPerConnection = 4096
)

type connectionSession struct {
	server *Server
	conn   net.Conn
	ctx    context.Context //nolint:containedctx // Session owns this connection-scoped context.

	respQueue     *mpscring.NotifyingRing[*outboundResponse]
	responsesDone chan struct{}
	slots         chan struct{}
	recvDone      chan error
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
		respQueue:     mpscring.NewNotifying[*outboundResponse](maxInFlightPerConnection),
		responsesDone: make(chan struct{}),
		slots:         make(chan struct{}, maxInFlightPerConnection),
		recvDone:      make(chan error, 1),
	}
	go session.receiveRequests()

	writer := transport.NewFrameWriter(conn)
	err := session.sendResponses(writer)
	cancel()
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
	defer s.releaseSlot()
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
		s.releaseSlot()
	}
}

func (s *connectionSession) receiveRequests() {
	var pending pendingJobs
	defer func() {
		go func() {
			pending.Wait()
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
		decoded, directResponse, direct, err := s.server.decodeRequest(frame)
		if err != nil {
			s.recvDone <- err
			return
		}
		phaseAtReceive := serverPhase(s.server.phase.Load())
		if err := s.reserveSlot(); err != nil {
			s.recvDone <- err
			return
		}
		if err := s.server.unavailableError(); err != nil {
			s.releaseSlot()
			s.recvDone <- err
			return
		}

		pending.Add(1)
		complete := func(response protocol.Response) {
			defer pending.Done()
			outbound, err := s.server.newOutboundResponse(response)
			if err != nil {
				s.releaseSlot()
				s.server.fail(fmt.Errorf("encode response: %w", err))
				return
			}
			if !s.respQueue.TryEnqueue(outbound) {
				s.server.recycleOutboundResponse(outbound)
				s.releaseSlot()
				s.server.fail(errors.New("connection response queue full despite reserved slot"))
				return
			}
		}
		if direct {
			complete(directResponse)
			continue
		}
		if phaseAtReceive == phaseQuarantine {
			complete(notReadyResponse(decoded))
			continue
		}
		if !s.server.dispatch(s.ctx.Done(), shardJob{operation: decoded, complete: complete}) {
			pending.Done()
			s.releaseSlot()
			if err := s.server.unavailableError(); err != nil {
				s.recvDone <- err
			} else {
				s.recvDone <- errServerNotAccepting
			}
			return
		}
	}
}

type pendingJobs struct {
	waitGroup sync.WaitGroup
}

func (p *pendingJobs) Add(delta int) { p.waitGroup.Add(delta) }
func (p *pendingJobs) Done()         { p.waitGroup.Done() }
func (p *pendingJobs) Wait()         { p.waitGroup.Wait() }

func (s *connectionSession) reserveSlot() error {
	select {
	case s.slots <- struct{}{}:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *connectionSession) releaseSlot() {
	select {
	case <-s.slots:
	default:
		s.server.fail(errors.New("released an unreserved connection slot"))
	}
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

func (s *Server) decodeRequest(
	frame []byte,
) (decoded operation, response protocol.Response, direct bool, err error) {

	// десериализатор FlatBuffers не валидирует offset и падает в панику если в пакете мусор
	// поэтому тут recover
	defer func() {
		if recovered := recover(); recovered != nil {
			decoded = operation{}
			response = protocol.Response{}
			direct = false
			err = fmt.Errorf("%w: %v", transport.ErrMalformedFrame, recovered)
		}
	}()
	request := redleasev1.GetSizePrefixedRootAsClientRequest(frame, 0)
	switch request.Operation() {
	case redleasev1.ClientOperationACQUIRE:
		var acquire redleasev1.AcquireRequest
		if request.Acquire(&acquire) == nil {
			return operation{}, protocol.Response{}, false, fmt.Errorf(
				"%w: Acquire payload is missing",
				transport.ErrMalformedFrame,
			)
		}
		return operation{
			requestID: request.RequestId(),
			kind:      operationAcquire,
			key:       acquire.Key(),
			leaseID: leaseID{
				clientID: acquire.ClientId(),
				bootID:   acquire.BootId(),
				leaseSeq: acquire.LeaseSeq(),
			},
			requestedTTLMS: acquire.RequestedTtlMs(),
		}, protocol.Response{}, false, nil

	case redleasev1.ClientOperationRENEW:
		var renew redleasev1.RenewRequest
		if request.Renew(&renew) == nil {
			return operation{}, protocol.Response{}, false, fmt.Errorf(
				"%w: Renew payload is missing",
				transport.ErrMalformedFrame,
			)
		}
		return operation{
			requestID: request.RequestId(),
			kind:      operationRenew,
			key:       renew.Key(),
			leaseID: leaseID{
				clientID: renew.ClientId(),
				bootID:   renew.BootId(),
				leaseSeq: renew.LeaseSeq(),
			},
			requestedTTLMS: renew.RequestedTtlMs(),
		}, protocol.Response{}, false, nil

	case redleasev1.ClientOperationRELEASE:
		var release redleasev1.ReleaseRequest
		if request.Release(&release) == nil {
			return operation{}, protocol.Response{}, false, fmt.Errorf(
				"%w: Release payload is missing",
				transport.ErrMalformedFrame,
			)
		}
		return operation{
			requestID: request.RequestId(),
			kind:      operationRelease,
			key:       release.Key(),
			leaseID: leaseID{
				clientID: release.ClientId(),
				bootID:   release.BootId(),
				leaseSeq: release.LeaseSeq(),
			},
		}, protocol.Response{}, false, nil

	case redleasev1.ClientOperationGET_TTL:
		return operation{}, protocol.Response{
			RequestID: request.RequestId(),
			Operation: redleasev1.ClientOperationGET_TTL,
			Status:    redleasev1.LeaseStatusOK,
			TTLMS:     s.config.MaxTTL,
		}, true, nil

	default:
		return operation{}, protocol.Response{}, false, fmt.Errorf(
			"%w: unsupported request operation %d",
			transport.ErrMalformedFrame,
			request.Operation(),
		)
	}
}
