package protocol

import (
	"fmt"

	redleasev1 "github.com/udovenkoav1981/RedLease/fbs/redlease/v1"
)

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
		response.Operation = redleasev1.ClientOperationACQUIRE
		var acquire redleasev1.AcquireResponse
		if root.Acquire(&acquire) == nil {
			return Response{}, fmt.Errorf("%w: Acquire response is missing", ErrMalformedFrame)
		}
		response.Status = acquire.Status()
		response.TTLMS = acquire.TtlMs()
	case redleasev1.ServerResultRENEW:
		response.Operation = redleasev1.ClientOperationRENEW
		var renew redleasev1.RenewResponse
		if root.Renew(&renew) == nil {
			return Response{}, fmt.Errorf("%w: Renew response is missing", ErrMalformedFrame)
		}
		response.Status = renew.Status()
		response.TTLMS = renew.TtlMs()
	case redleasev1.ServerResultRELEASE:
		response.Operation = redleasev1.ClientOperationRELEASE
		var release redleasev1.ReleaseResponse
		if root.Release(&release) == nil {
			return Response{}, fmt.Errorf("%w: Release response is missing", ErrMalformedFrame)
		}
		response.Status = release.Status()
	case redleasev1.ServerResultGET_TTL:
		response.Operation = redleasev1.ClientOperationGET_TTL
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

func validStatus(status redleasev1.LeaseStatus) bool {
	return status >= redleasev1.LeaseStatusOK && status <= redleasev1.LeaseStatusKEY_LIMIT_REACHED
}
