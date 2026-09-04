package server

import (
	"context"
	"io"
	"sync"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type streamSession struct {
	server *Server
	ctx    context.Context

	responses chan *redleasev1.ServerResponse
	slots     chan struct{}
	recvDone  chan error
}

// LeaseStream multiplexes requests through the global shard workers. Recv is
// performed by one goroutine and Send only by the handler goroutine.
func (s *Server) LeaseStream(stream grpc.BidiStreamingServer[redleasev1.ClientRequest, redleasev1.ServerResponse]) error {
	if s.closed.Load() {
		return status.Error(codes.Unavailable, "server is closed")
	}

	ctx, cancel := context.WithCancel(stream.Context())
	stopServerCancel := context.AfterFunc(s.ctx, cancel)
	defer stopServerCancel()
	defer cancel()

	session := &streamSession{
		server:    s,
		ctx:       ctx,
		responses: make(chan *redleasev1.ServerResponse, s.config.maxInFlight),
		slots:     make(chan struct{}, s.config.maxInFlight),
		recvDone:  make(chan error, 1),
	}
	go session.receive(stream)

	recvDone := session.recvDone
	for {
		select {
		case response, ok := <-session.responses:
			if !ok {
				return nil
			}
			if err := stream.Send(response); err != nil {
				return err
			}
			session.releaseSlot()

		case err := <-recvDone:
			if err != nil && err != io.EOF {
				return err
			}
			// No more jobs can be added after receive returns. Its waiter closes
			// responses after every accepted job has published its response.
			recvDone = nil

		case <-ctx.Done():
			if s.closed.Load() {
				return status.Error(codes.Unavailable, "server is closed")
			}
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}
}

func (s *streamSession) receive(stream grpc.BidiStreamingServer[redleasev1.ClientRequest, redleasev1.ServerResponse]) {
	var pending pendingJobs
	defer func() {
		go func() {
			pending.Wait()
			close(s.responses)
		}()
	}()

	for {
		request, err := stream.Recv()
		if err != nil {
			s.recvDone <- err
			return
		}
		phaseAtReceive := serverPhase(s.server.phase.Load())
		if err := s.reserveSlot(); err != nil {
			s.recvDone <- err
			return
		}

		decoded, directResponse, err := s.server.decodeRequest(request)
		if err != nil {
			s.releaseSlot()
			s.recvDone <- err
			return
		}

		pending.Add(1)
		complete := func(response *redleasev1.ServerResponse) {
			defer pending.Done()
			select {
			case s.responses <- response:
			case <-s.ctx.Done():
				s.releaseSlot()
			}
		}
		if directResponse != nil {
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
			s.recvDone <- status.Error(codes.Unavailable, "server is closed")
			return
		}
	}
}

// pendingJobs wraps sync.WaitGroup to keep the stream lifecycle details local
// to this file.
type pendingJobs struct {
	waitGroup sync.WaitGroup
}

func (p *pendingJobs) Add(delta int) { p.waitGroup.Add(delta) }
func (p *pendingJobs) Done()         { p.waitGroup.Done() }
func (p *pendingJobs) Wait()         { p.waitGroup.Wait() }

func (s *streamSession) reserveSlot() error {
	select {
	case s.slots <- struct{}{}:
		return nil
	case <-s.ctx.Done():
		return status.Error(codes.Unavailable, "stream is closed")
	}
}

func (s *streamSession) releaseSlot() {
	select {
	case <-s.slots:
	default:
		panic("server: released an unreserved stream slot")
	}
}

func (s *Server) decodeRequest(request *redleasev1.ClientRequest) (operation, *redleasev1.ServerResponse, error) {
	if request == nil {
		return operation{}, nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	switch value := request.Operation.(type) {
	case *redleasev1.ClientRequest_Acquire:
		if value.Acquire == nil {
			return operation{}, nil, status.Error(codes.InvalidArgument, "acquire request is nil")
		}
		return operation{
			requestID:      request.RequestId,
			kind:           operationAcquire,
			key:            string(value.Acquire.Key),
			leaseID:        makeLeaseID(value.Acquire.LeaseId),
			requestedTTLMS: value.Acquire.RequestedTtlMs,
		}, nil, nil

	case *redleasev1.ClientRequest_Renew:
		if value.Renew == nil {
			return operation{}, nil, status.Error(codes.InvalidArgument, "renew request is nil")
		}
		return operation{
			requestID:      request.RequestId,
			kind:           operationRenew,
			key:            string(value.Renew.Key),
			leaseID:        makeLeaseID(value.Renew.LeaseId),
			requestedTTLMS: value.Renew.RequestedTtlMs,
		}, nil, nil

	case *redleasev1.ClientRequest_Release:
		if value.Release == nil {
			return operation{}, nil, status.Error(codes.InvalidArgument, "release request is nil")
		}
		return operation{
			requestID: request.RequestId,
			kind:      operationRelease,
			key:       string(value.Release.Key),
			leaseID:   makeLeaseID(value.Release.LeaseId),
		}, nil, nil

	case *redleasev1.ClientRequest_GetTtl:
		if value.GetTtl == nil {
			return operation{}, nil, status.Error(codes.InvalidArgument, "get TTL request is nil")
		}
		return operation{}, &redleasev1.ServerResponse{
			RequestId: request.RequestId,
			Result: &redleasev1.ServerResponse_GetTtl{GetTtl: &redleasev1.GetTTLResponse{
				ConfiguredMaxTtlMs: s.config.configuredMaxTTLMS,
			}},
		}, nil

	default:
		return operation{}, nil, status.Error(codes.InvalidArgument, "request operation is missing")
	}
}
