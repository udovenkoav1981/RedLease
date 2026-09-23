package server

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

// outboundResponse owns its FlatBuffers backing bytes until the connection
// writer has copied the complete frame into its write buffer.
type outboundResponse struct {
	message redleasev1.ServerResponse
	builder *flatbuffers.Builder
}

func (s *Server) newOutboundResponse(response protocol.Response) (*outboundResponse, error) {
	if response.Status > redleasev1.LeaseStatusKEY_LIMIT_REACHED {
		return nil, fmt.Errorf("unsupported lease status %d", response.Status)
	}
	if response.Operation < redleasev1.ClientOperationACQUIRE || response.Operation > redleasev1.ClientOperationGET_TTL {
		return nil, fmt.Errorf("unsupported response operation %d", response.Operation)
	}

	var outbound *outboundResponse
	if pooled := s.responsePool.Get(); pooled != nil {
		if reusable, ok := pooled.(*outboundResponse); ok {
			outbound = reusable
			outbound.builder.Reset()
		}
	}
	if outbound == nil {
		outbound = &outboundResponse{builder: flatbuffers.NewBuilder(protocol.NewBuilderSize)}
	}
	builder := outbound.builder
	redleasev1.ServerResponseStart(builder)
	redleasev1.ServerResponseAddRequestId(builder, response.RequestID)
	switch response.Operation {
	case redleasev1.ClientOperationACQUIRE:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultACQUIRE)
		redleasev1.ServerResponseAddAcquire(builder, redleasev1.CreateAcquireResponse(
			builder, response.Status, response.TTLMS,
		))
	case redleasev1.ClientOperationRENEW:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultRENEW)
		redleasev1.ServerResponseAddRenew(builder, redleasev1.CreateRenewResponse(
			builder, response.Status, response.TTLMS,
		))
	case redleasev1.ClientOperationRELEASE:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultRELEASE)
		redleasev1.ServerResponseAddRelease(builder, redleasev1.CreateReleaseResponse(
			builder, response.Status,
		))
	case redleasev1.ClientOperationGET_TTL:
		redleasev1.ServerResponseAddResult(builder, redleasev1.ServerResultGET_TTL)
		redleasev1.ServerResponseAddGetTtl(builder, redleasev1.CreateGetTTLResponse(
			builder, response.TTLMS,
		))
	case redleasev1.ClientOperationNONE:
		s.recycleOutboundResponse(outbound)
		return nil, fmt.Errorf("unsupported response operation %d", response.Operation)
	}
	root := redleasev1.ServerResponseEnd(builder)
	redleasev1.FinishSizePrefixedServerResponseBuffer(builder, root)
	frame := builder.FinishedBytes()
	rootOffset := flatbuffers.GetUOffsetT(frame[flatbuffers.SizeUint32:]) +
		flatbuffers.UOffsetT(flatbuffers.SizeUint32)
	outbound.message.Init(frame, rootOffset)
	return outbound, nil
}

func (s *Server) recycleOutboundResponse(outbound *outboundResponse) {
	outbound.message = redleasev1.ServerResponse{}
	s.responsePool.Put(outbound)
}
