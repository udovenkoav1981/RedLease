package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"

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

	responses chan protocol.Response
	slots     chan struct{}
	recvDone  chan error
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
	defer cancel()
	session := &connectionSession{
		server:    s,
		conn:      conn,
		ctx:       ctx,
		responses: make(chan protocol.Response, s.config.MaxInFlightPerConnection),
		slots:     make(chan struct{}, s.config.MaxInFlightPerConnection),
		recvDone:  make(chan error, 1),
	}
	go session.receiveSafely()

	builder := flatbuffers.NewBuilder(protocol.NewBuilderSize)
	writer := transport.NewFrameWriter(conn)
	return session.writeResponses(writer, builder)
}

func (s *connectionSession) writeResponses(
	writer *transport.FrameWriter,
	builder *flatbuffers.Builder,
) error {
	recvDone := s.recvDone

connectionLoop:
	for {
		select {
		case response, ok := <-s.responses:
			if !ok {
				return s.server.unavailableErrorUnlessClosed()
			}

			for {
				if err := s.bufferResponse(writer, builder, response); err != nil {
					return err
				}
				select {
				case response, ok = <-s.responses:
					if ok {
						continue
					}
					if err := writer.Flush(); err != nil && s.server.ctx.Err() == nil {
						return fmt.Errorf("flush response batch: %w", err)
					}
					return s.server.unavailableErrorUnlessClosed()
				default:
					if err := writer.Flush(); err != nil {
						return fmt.Errorf("flush response batch: %w", err)
					}
					continue connectionLoop
				}
			}

		case err := <-recvDone:
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			recvDone = nil

		case <-s.ctx.Done():
			return s.server.unavailableErrorUnlessClosed()
		}
	}
}

func (s *connectionSession) bufferResponse(
	writer *transport.FrameWriter,
	builder *flatbuffers.Builder,
	response protocol.Response,
) error {
	if err := s.server.unavailableError(); err != nil {
		s.releaseSlot()
		return err
	}
	frame, err := protocol.EncodeResponse(builder, response)
	if err != nil {
		s.releaseSlot()
		s.server.fail(fmt.Errorf("encode response: %w", err))
		return s.server.unavailableError()
	}
	if err := writer.BufferFrame(frame); err != nil {
		s.releaseSlot()
		return fmt.Errorf("buffer response: %w", err)
	}
	s.releaseSlot()
	return nil
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
					s.server.failRecoveredPanic("closing connection responses", recovered)
				}
			}()
			pending.Wait()
			close(s.responses)
		}()
	}()

	reader := transport.NewFrameReader(s.conn)
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			s.recvDone <- err
			return
		}
		request, err := protocol.DecodeRequest(frame)
		if err != nil {
			s.recvDone <- err
			return
		}
		phaseAtReceive := serverPhase(s.server.phase.Load())
		if err := s.reserveSlot(); err != nil {
			s.recvDone <- err
			return
		}

		decoded, directResponse, direct, err := s.server.decodeRequest(request)
		if err != nil {
			s.releaseSlot()
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
			select {
			case s.responses <- response:
			case <-s.ctx.Done():
				s.releaseSlot()
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

func (s *Server) decodeRequest(request protocol.Request) (operation, protocol.Response, bool, error) {
	switch request.Operation {
	case protocol.OperationAcquire:
		return operation{
			requestID: request.RequestID,
			kind:      operationAcquire,
			key:       request.Key,
			leaseID: leaseID{
				clientID: request.ClientID,
				bootID:   request.BootID,
				leaseSeq: request.LeaseSequence,
			},
			requestedTTLMS: request.RequestedTTLMS,
		}, protocol.Response{}, false, nil

	case protocol.OperationRenew:
		return operation{
			requestID: request.RequestID,
			kind:      operationRenew,
			key:       request.Key,
			leaseID: leaseID{
				clientID: request.ClientID,
				bootID:   request.BootID,
				leaseSeq: request.LeaseSequence,
			},
			requestedTTLMS: request.RequestedTTLMS,
		}, protocol.Response{}, false, nil

	case protocol.OperationRelease:
		return operation{
			requestID: request.RequestID,
			kind:      operationRelease,
			key:       request.Key,
			leaseID: leaseID{
				clientID: request.ClientID,
				bootID:   request.BootID,
				leaseSeq: request.LeaseSequence,
			},
		}, protocol.Response{}, false, nil

	case protocol.OperationGetTTL:
		return operation{}, protocol.Response{
			RequestID: request.RequestID,
			Operation: protocol.OperationGetTTL,
			Status:    protocol.StatusOK,
			TTLMS:     s.config.MaxTTL,
		}, true, nil

	default:
		return operation{}, protocol.Response{}, false, fmt.Errorf(
			"unsupported request operation %d",
			request.Operation,
		)
	}
}
