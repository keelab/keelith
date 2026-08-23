// Package grpc provides Keelith's grpc-go transport.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	kerrors "github.com/keelab/keelith/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	errorDomain   = "keelith.anniext.cn"
	errorCodeKey  = "keelith_code"
	defaultReason = "GRPC_ERROR"
)

var (
	// ErrInvalidOption means a transport option is unsafe or incomplete.
	ErrInvalidOption = errors.New("grpc transport: invalid option")
	// ErrAlreadyStarted means Server.Start was called more than once.
	ErrAlreadyStarted = errors.New("grpc transport: server already started")
	// ErrNilContext means a public operation received a nil context.
	ErrNilContext = errors.New("grpc transport: nil context")
)

// ErrorCodec maps immutable Keelith Errors to gRPC status details.
type ErrorCodec struct {
	allowed []string
}

// NewErrorCodec constructs an ErrorCodec with a wire metadata allowlist.
func NewErrorCodec(allowedMetadata ...string) (*ErrorCodec, error) {
	allowed := make([]string, 0, len(allowedMetadata))
	seen := make(map[string]struct{}, len(allowedMetadata))
	for _, key := range allowedMetadata {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if normalized == "" || normalized == errorCodeKey {
			return nil, fmt.Errorf("%w: error metadata key %q", ErrInvalidOption, key)
		}
		if _, duplicate := seen[normalized]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate error metadata key %q",
				ErrInvalidOption,
				normalized,
			)
		}
		seen[normalized] = struct{}{}
		allowed = append(allowed, normalized)
	}
	sort.Strings(allowed)
	return &ErrorCodec{allowed: allowed}, nil
}

// Encode converts err to a gRPC-compatible status error.
func (codec *ErrorCodec) Encode(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}

	var frameworkErr *kerrors.Error
	if !errors.As(err, &frameworkErr) {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, "internal server error")
	}
	detailMetadata := map[string]string{
		errorCodeKey: strconv.FormatInt(int64(frameworkErr.Code()), 10),
	}
	allowed := make(map[string]struct{}, len(codec.allowedKeys()))
	for _, key := range codec.allowedKeys() {
		allowed[key] = struct{}{}
	}
	for key, value := range frameworkErr.Metadata() {
		normalized := strings.ToLower(key)
		if _, ok := allowed[normalized]; ok {
			detailMetadata[normalized] = value
		}
	}
	detail := &errdetails.ErrorInfo{
		Reason:   normalizeReason(frameworkErr.Reason()),
		Domain:   errorDomain,
		Metadata: detailMetadata,
	}
	target := status.New(codeToGRPC(frameworkErr.Code()), frameworkErr.Message())
	withDetails, detailErr := target.WithDetails(detail)
	if detailErr != nil {
		return target.Err()
	}
	return withDetails.Err()
}

// Decode restores a Keelith Error when status details were produced by
// ErrorCodec.
func (codec *ErrorCodec) Decode(err error) error {
	if err == nil {
		return nil
	}
	target, ok := status.FromError(err)
	if !ok {
		return err
	}
	for _, detail := range target.Details() {
		info, detailOK := detail.(*errdetails.ErrorInfo)
		if !detailOK || info.GetDomain() != errorDomain {
			continue
		}
		code, parseErr := strconv.ParseInt(info.GetMetadata()[errorCodeKey], 10, 32)
		if parseErr != nil {
			continue
		}
		metadata := make(map[string]string)
		for _, key := range codec.allowedKeys() {
			if value, exists := info.GetMetadata()[key]; exists {
				metadata[key] = value
			}
		}
		return kerrors.New(
			int32(code),
			info.GetReason(),
			target.Message(),
			kerrors.WithMetadata(metadata),
		)
	}
	return err
}

func (codec *ErrorCodec) allowedKeys() []string {
	if codec == nil {
		return nil
	}
	return codec.allowed
}

func normalizeReason(reason string) string {
	if reason == "" {
		return defaultReason
	}
	return reason
}

func codeToGRPC(code int32) codes.Code {
	switch code {
	case 400:
		return codes.InvalidArgument
	case 401:
		return codes.Unauthenticated
	case 403:
		return codes.PermissionDenied
	case 404:
		return codes.NotFound
	case 409:
		return codes.Aborted
	case 412:
		return codes.FailedPrecondition
	case 413, 429:
		return codes.ResourceExhausted
	case 499:
		return codes.Canceled
	case 501:
		return codes.Unimplemented
	case 503:
		return codes.Unavailable
	case 504:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}
