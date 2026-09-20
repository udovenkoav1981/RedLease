package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/proto/redlease/v1"
)

const sizePrefixBytes = 4

var ErrMalformedFrame = errors.New("malformed RedLease FlatBuffer frame")

// EncodeRequest resets builder and returns a size-prefixed FlatBuffer. The
// returned bytes remain valid until builder is modified or reset.
func EncodeRequest(builder *flatbuffers.Builder, request Request) ([]byte, error) {
	if builder == nil {
		return nil, errors.New("FlatBuffers builder is nil")
	}
	if !validOperation(request.Operation) {
		return nil, fmt.Errorf("unsupported request operation %d", request.Operation)
	}

	builder.Reset()
	redleasev1.ClientRequestStart(builder)
	redleasev1.ClientRequestAddRequestId(builder, request.RequestID)
	redleasev1.ClientRequestAddOperation(builder, request.Operation)
	switch request.Operation {
	case OperationAcquire:
		redleasev1.ClientRequestAddAcquire(builder, redleasev1.CreateAcquireRequest(
			builder,
			request.Key,
			request.ClientID,
			request.BootID,
			request.LeaseSequence,
			request.RequestedTTLMS,
		))
	case OperationRenew:
		redleasev1.ClientRequestAddRenew(builder, redleasev1.CreateRenewRequest(
			builder,
			request.Key,
			request.ClientID,
			request.BootID,
			request.LeaseSequence,
			request.RequestedTTLMS,
		))
	case OperationRelease:
		redleasev1.ClientRequestAddRelease(builder, redleasev1.CreateReleaseRequest(
			builder,
			request.Key,
			request.ClientID,
			request.BootID,
			request.LeaseSequence,
		))
	case OperationGetTTL:
	}
	root := redleasev1.ClientRequestEnd(builder)
	redleasev1.FinishSizePrefixedClientRequestBuffer(builder, root)
	return builder.FinishedBytes(), nil
}

// DecodeRequest copies a size-prefixed FlatBuffer into an owned Request.
func DecodeRequest(frame []byte) (request Request, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", ErrMalformedFrame, recovered)
		}
	}()
	if err := validateFrame(frame); err != nil {
		return Request{}, err
	}

	root := redleasev1.GetSizePrefixedRootAsClientRequest(frame, 0)
	request.RequestID = root.RequestId()
	request.Operation = root.Operation()
	switch request.Operation {
	case OperationAcquire:
		var acquire redleasev1.AcquireRequest
		if root.Acquire(&acquire) == nil {
			return Request{}, fmt.Errorf("%w: Acquire payload is missing", ErrMalformedFrame)
		}
		request.Key = acquire.Key()
		request.ClientID = acquire.ClientId()
		request.BootID = acquire.BootId()
		request.LeaseSequence = acquire.LeaseSeq()
		request.RequestedTTLMS = acquire.RequestedTtlMs()
	case OperationRenew:
		var renew redleasev1.RenewRequest
		if root.Renew(&renew) == nil {
			return Request{}, fmt.Errorf("%w: Renew payload is missing", ErrMalformedFrame)
		}
		request.Key = renew.Key()
		request.ClientID = renew.ClientId()
		request.BootID = renew.BootId()
		request.LeaseSequence = renew.LeaseSeq()
		request.RequestedTTLMS = renew.RequestedTtlMs()
	case OperationRelease:
		var release redleasev1.ReleaseRequest
		if root.Release(&release) == nil {
			return Request{}, fmt.Errorf("%w: Release payload is missing", ErrMalformedFrame)
		}
		request.Key = release.Key()
		request.ClientID = release.ClientId()
		request.BootID = release.BootId()
		request.LeaseSequence = release.LeaseSeq()
	case OperationGetTTL:
	default:
		return Request{}, fmt.Errorf("%w: unsupported request operation %d", ErrMalformedFrame, request.Operation)
	}
	return request, nil
}

// EncodeResponse resets builder and returns a size-prefixed FlatBuffer. The
// returned bytes remain valid until builder is modified or reset.
func EncodeResponse(builder *flatbuffers.Builder, response Response) ([]byte, error) {
	if builder == nil {
		return nil, errors.New("FlatBuffers builder is nil")
	}
	if !validOperation(response.Operation) {
		return nil, fmt.Errorf("unsupported response operation %d", response.Operation)
	}
	if !validStatus(response.Status) {
		return nil, fmt.Errorf("unsupported lease status %d", response.Status)
	}

	builder.Reset()
	redleasev1.ServerResponseStart(builder)
	redleasev1.ServerResponseAddRequestId(builder, response.RequestID)
	redleasev1.ServerResponseAddResult(builder, serverResult(response.Operation))
	switch response.Operation {
	case OperationAcquire:
		redleasev1.ServerResponseAddAcquire(builder, redleasev1.CreateAcquireResponse(
			builder,
			response.Status,
			response.TTLMS,
		))
	case OperationRenew:
		redleasev1.ServerResponseAddRenew(builder, redleasev1.CreateRenewResponse(
			builder,
			response.Status,
			response.TTLMS,
		))
	case OperationRelease:
		redleasev1.ServerResponseAddRelease(builder, redleasev1.CreateReleaseResponse(
			builder,
			response.Status,
		))
	case OperationGetTTL:
		redleasev1.ServerResponseAddGetTtl(builder, redleasev1.CreateGetTTLResponse(
			builder,
			response.TTLMS,
		))
	}
	root := redleasev1.ServerResponseEnd(builder)
	redleasev1.FinishSizePrefixedServerResponseBuffer(builder, root)
	return builder.FinishedBytes(), nil
}

// DecodeResponse copies a size-prefixed FlatBuffer into an owned Response.
func DecodeResponse(frame []byte) (response Response, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", ErrMalformedFrame, recovered)
		}
	}()
	if err := validateFrame(frame); err != nil {
		return Response{}, err
	}

	root := redleasev1.GetSizePrefixedRootAsServerResponse(frame, 0)
	response.RequestID = root.RequestId()
	switch root.Result() {
	case redleasev1.ServerResultACQUIRE:
		response.Operation = OperationAcquire
		var acquire redleasev1.AcquireResponse
		if root.Acquire(&acquire) == nil {
			return Response{}, fmt.Errorf("%w: Acquire response is missing", ErrMalformedFrame)
		}
		response.Status = acquire.Status()
		response.TTLMS = acquire.TtlMs()
	case redleasev1.ServerResultRENEW:
		response.Operation = OperationRenew
		var renew redleasev1.RenewResponse
		if root.Renew(&renew) == nil {
			return Response{}, fmt.Errorf("%w: Renew response is missing", ErrMalformedFrame)
		}
		response.Status = renew.Status()
		response.TTLMS = renew.TtlMs()
	case redleasev1.ServerResultRELEASE:
		response.Operation = OperationRelease
		var release redleasev1.ReleaseResponse
		if root.Release(&release) == nil {
			return Response{}, fmt.Errorf("%w: Release response is missing", ErrMalformedFrame)
		}
		response.Status = release.Status()
	case redleasev1.ServerResultGET_TTL:
		response.Operation = OperationGetTTL
		var getTTL redleasev1.GetTTLResponse
		if root.GetTtl(&getTTL) == nil {
			return Response{}, fmt.Errorf("%w: GetTTL response is missing", ErrMalformedFrame)
		}
		response.Status = StatusOK
		response.TTLMS = getTTL.ConfiguredMaxTtlMs()
	default:
		return Response{}, fmt.Errorf("%w: unsupported response result %d", ErrMalformedFrame, root.Result())
	}
	if !validStatus(response.Status) {
		return Response{}, fmt.Errorf("%w: unsupported lease status %d", ErrMalformedFrame, response.Status)
	}
	return response, nil
}

func validateFrame(frame []byte) error {
	if len(frame) < sizePrefixBytes+flatbuffers.SizeUOffsetT {
		return fmt.Errorf("%w: frame is too short", ErrMalformedFrame)
	}
	payloadSize := binary.LittleEndian.Uint32(frame[:sizePrefixBytes])
	if uint64(payloadSize)+sizePrefixBytes != uint64(len(frame)) {
		return fmt.Errorf("%w: size prefix %d does not match frame size %d", ErrMalformedFrame, payloadSize, len(frame))
	}
	return nil
}

func validOperation(operation Operation) bool {
	return operation >= OperationAcquire && operation <= OperationGetTTL
}

func validStatus(status Status) bool {
	return status >= StatusOK && status <= StatusKeyLimitReached
}

func serverResult(operation Operation) redleasev1.ServerResult {
	switch operation {
	case OperationAcquire:
		return redleasev1.ServerResultACQUIRE
	case OperationRenew:
		return redleasev1.ServerResultRENEW
	case OperationRelease:
		return redleasev1.ServerResultRELEASE
	case OperationGetTTL:
		return redleasev1.ServerResultGET_TTL
	default:
		return redleasev1.ServerResultNONE
	}
}
