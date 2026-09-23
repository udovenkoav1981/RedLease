package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/mpscring"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
	"github.com/udovenkoav1981/RedLease/internal/transport"
)

var (
	errServerClosed       = errors.New("server is closed")
	errServerNotAccepting = errors.New("server is not accepting work")
)

type connectionSession struct {
	server *Server
	conn   net.Conn
	ctx    context.Context //nolint:containedctx // Session owns this connection-scoped context.

	responses      *mpscring.Ring[*outboundResponse]
	responsesReady chan struct{}
	responsesDone  chan struct{}
	slots          chan struct{}
	recvDone       chan error
}

// Serve accepts persistent RedLease TCP connections on listener. Server owns
// listener after a successful call and closes it during Close or fatal failure.
// Serve may be called only once.
func (s *Server) Serve(listener net.Listener) error {
	if listener == nil {
		return errors.New("listener must not be nil")
	}
	if err := s.unavailableError(); err != nil {
		return err
	}

	s.transportMu.Lock()
	if s.serveStarted {
		s.transportMu.Unlock()
		return errors.New("server Serve called more than once")
	}
	s.serveStarted = true
	s.listener = listener
	s.transportMu.Unlock()

	defer func() {
		s.transportMu.Lock()
		if s.listener == listener {
			s.listener = nil
		}
		s.transportMu.Unlock()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return s.unavailableErrorUnlessClosed()
			}
			return fmt.Errorf("accept RedLease connection: %w", err)
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetNoDelay(true); err != nil {
				_ = conn.Close()
				continue
			}
		}
		if !s.registerConnection(conn) {
			_ = conn.Close()
			return s.unavailableErrorUnlessClosed()
		}
		go s.runConnection(conn)
	}
}

func (s *Server) registerConnection(conn net.Conn) bool {
	s.transportMu.Lock()
	defer s.transportMu.Unlock()
	if s.ctx.Err() != nil {
		return false
	}
	s.connections[conn] = struct{}{}
	s.connectionWG.Add(1)
	return true
}

func (s *Server) runConnection(conn net.Conn) {
	defer s.connectionWG.Done()
	defer func() {
		_ = conn.Close()
		s.transportMu.Lock()
		delete(s.connections, conn)
		s.transportMu.Unlock()
	}()

	if err := s.serveConnection(conn); err != nil && s.ctx.Err() == nil {
		s.logger.Debug("connection closed", "remote_address", conn.RemoteAddr(), "error", err)
	}
}

func (s *Server) closeTransport() {
	s.transportMu.Lock()
	listener := s.listener
	connections := make([]net.Conn, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
	}
	s.transportMu.Unlock()

	if listener != nil {
		_ = listener.Close()
	}
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func (s *Server) serveConnection(conn net.Conn) (result error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.failRecoveredPanic("serving connection", recovered)
			result = s.unavailableError()
		}
	}()
	if err := s.unavailableError(); err != nil {
		return err
	}
	s.activeConnections.Add(1)
	defer s.activeConnections.Add(-1)

	ctx, cancel := context.WithCancel(s.ctx)
	session := &connectionSession{
		server:         s,
		conn:           conn,
		ctx:            ctx,
		responses:      mpscring.New[*outboundResponse](),
		responsesReady: make(chan struct{}, 1),
		responsesDone:  make(chan struct{}),
		slots:          make(chan struct{}, mpscring.Capacity),
		recvDone:       make(chan error, 1),
	}
	defer func() {
		cancel()
		go session.discardResponses()
	}()
	go session.receiveSafely()

	writer := transport.NewFrameWriter(conn)
	return session.writeResponses(writer)
}

func (s *connectionSession) writeResponses(writer *transport.FrameWriter) error {
	recvDone := s.recvDone

	for {
		response, ok := s.responses.TryDequeue()
		if !ok {
			select {
			case <-s.responsesReady:
				continue
			case <-s.responsesDone:
				response, ok = s.responses.TryDequeue()
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
			response, ok = s.responses.TryDequeue()
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
		response, ok := s.responses.TryDequeue()
		if !ok {
			return
		}
		s.server.recycleOutboundResponse(response)
		s.releaseSlot()
	}
}

func (s *connectionSession) receiveSafely() {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.server.failRecoveredPanic("receiving connection request", recovered)
			select {
			case s.recvDone <- s.server.unavailableError():
			default:
			}
		}
	}()
	s.receive()
}

func (s *connectionSession) receive() {
	var pending pendingJobs
	defer func() {
		go func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					s.server.failRecoveredPanic("finishing connection responses", recovered)
				}
			}()
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
			if !s.responses.TryEnqueue(outbound) {
				s.server.recycleOutboundResponse(outbound)
				s.releaseSlot()
				s.server.fail(errors.New("connection response queue full despite reserved slot"))
				return
			}
			select {
			case s.responsesReady <- struct{}{}:
			default:
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
	// FlatBuffers getters view the receive buffer. Copy scalars into operation
	// before the reader advances and reuses that buffer for another frame.
	defer func() {
		if recovered := recover(); recovered != nil {
			decoded = operation{}
			response = protocol.Response{}
			direct = false
			err = fmt.Errorf("%w: %v", protocol.ErrMalformedFrame, recovered)
		}
	}()
	if err := protocol.ValidateFrame(frame); err != nil {
		return operation{}, protocol.Response{}, false, err
	}
	request := redleasev1.GetSizePrefixedRootAsClientRequest(frame, 0)
	switch request.Operation() {
	case redleasev1.ClientOperationACQUIRE:
		var acquire redleasev1.AcquireRequest
		if request.Acquire(&acquire) == nil {
			return operation{}, protocol.Response{}, false, fmt.Errorf(
				"%w: Acquire payload is missing",
				protocol.ErrMalformedFrame,
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
				protocol.ErrMalformedFrame,
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
				protocol.ErrMalformedFrame,
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
			protocol.ErrMalformedFrame,
			request.Operation(),
		)
	}
}
