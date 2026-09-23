package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
)

const sizePrefixBytes = 4

var ErrMalformedFrame = errors.New("malformed RedLease FlatBuffer frame")

// DecodeResponse copies a size-prefixed FlatBuffer into an owned Response.
func DecodeResponse(frame []byte) (response Response, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", ErrMalformedFrame, recovered)
		}
	}()
	if err := ValidateFrame(frame); err != nil {
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
		response.Status = redleasev1.LeaseStatusOK
		response.TTLMS = getTTL.ConfiguredMaxTtlMs()
	default:
		return Response{}, fmt.Errorf("%w: unsupported response result %d", ErrMalformedFrame, root.Result())
	}
	if !validStatus(response.Status) {
		return Response{}, fmt.Errorf("%w: unsupported lease status %d", ErrMalformedFrame, response.Status)
	}
	return response, nil
}

// ValidateFrame checks the size prefix and minimum FlatBuffers root size.
func ValidateFrame(frame []byte) error {
	if len(frame) < sizePrefixBytes+flatbuffers.SizeUOffsetT {
		return fmt.Errorf("%w: frame is too short", ErrMalformedFrame)
	}
	payloadSize := binary.LittleEndian.Uint32(frame[:sizePrefixBytes])
	if uint64(payloadSize)+sizePrefixBytes != uint64(len(frame)) {
		return fmt.Errorf("%w: size prefix %d does not match frame size %d", ErrMalformedFrame, payloadSize, len(frame))
	}
	return nil
}

func validStatus(status redleasev1.LeaseStatus) bool {
	return status >= redleasev1.LeaseStatusOK && status <= redleasev1.LeaseStatusKEY_LIMIT_REACHED
}
