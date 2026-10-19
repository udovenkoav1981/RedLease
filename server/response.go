package server

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
	"github.com/udovenkoav1981/RedLease/internal/protocol"
)

// encodeResponse serializes one owned response into the connection writer's
// reusable builder. The returned frame remains valid until the next Reset.
func encodeResponse(builder *flatbuffers.Builder, response protocol.Response) ([]byte, error) {
	if response.Status > redleasev1.LeaseStatusKEY_LIMIT_REACHED {
		return nil, fmt.Errorf("unsupported lease status %d", response.Status)
	}
	if response.Operation < redleasev1.ClientOperationACQUIRE || response.Operation > redleasev1.ClientOperationGET_TTL {
		return nil, fmt.Errorf("unsupported response operation %d", response.Operation)
	}

	builder.Reset()
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
		return nil, fmt.Errorf("unsupported response operation %d", response.Operation)
	}
	root := redleasev1.ServerResponseEnd(builder)
	redleasev1.FinishSizePrefixedServerResponseBuffer(builder, root)
	return builder.FinishedBytes(), nil
}
