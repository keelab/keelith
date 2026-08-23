package kitex

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	kitexerrors "github.com/cloudwego/kitex/pkg/kerrors"
	kerrors "github.com/keelab/keelith/errors"
)

const (
	errorMarkerKey   = "keelith.error"
	errorMarkerValue = "v1"
	errorReasonKey   = "keelith.reason"
	errorMetadataKey = "keelith.metadata."
)

// ErrorCodec maps immutable Keelith Errors to Kitex business status errors.
//
// Private causes never cross the wire. Only explicitly allowlisted metadata
// is stored in Kitex BizExtra.
type ErrorCodec struct {
	allowed []string
}

// NewErrorCodec constructs an ErrorCodec with a wire metadata allowlist.
func NewErrorCodec(allowedMetadata ...string) (*ErrorCodec, error) {
	allowed := make([]string, 0, len(allowedMetadata))
	seen := make(map[string]struct{}, len(allowedMetadata))
	for _, key := range allowedMetadata {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if normalized == "" ||
			strings.HasPrefix(normalized, "keelith.") {
			return nil, fmt.Errorf(
				"%w: error metadata key %q",
				ErrInvalidOption,
				key,
			)
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

// Encode converts an error to a Kitex business status error.
func (codec *ErrorCodec) Encode(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return newKitexError(499, "CANCELED", "request canceled", nil)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return newKitexError(
			504,
			"DEADLINE_EXCEEDED",
			"request deadline exceeded",
			nil,
		)
	}
	var frameworkErr *kerrors.Error
	if !errors.As(err, &frameworkErr) {
		return newKitexError(
			500,
			"INTERNAL",
			"internal server error",
			nil,
		)
	}
	metadata := make(map[string]string)
	source := frameworkErr.Metadata()
	normalizedSource := make(map[string]string, len(source))
	for key, value := range source {
		normalizedSource[strings.ToLower(strings.TrimSpace(key))] = value
	}
	for _, key := range codec.allowedKeys() {
		if value, ok := normalizedSource[key]; ok {
			metadata[key] = value
		}
	}
	return newKitexError(
		frameworkErr.Code(),
		normalizeKitexReason(frameworkErr.Reason()),
		frameworkErr.Message(),
		metadata,
	)
}

// Decode restores a Keelith Error only for statuses emitted by this codec.
func (codec *ErrorCodec) Decode(err error) error {
	if err == nil {
		return nil
	}
	business, ok := kitexerrors.FromBizStatusError(err)
	if !ok {
		return err
	}
	extra := business.BizExtra()
	if extra[errorMarkerKey] != errorMarkerValue {
		return err
	}
	metadata := make(map[string]string)
	for _, key := range codec.allowedKeys() {
		if value, exists := extra[errorMetadataKey+key]; exists {
			metadata[key] = value
		}
	}
	return kerrors.New(
		business.BizStatusCode(),
		normalizeKitexReason(extra[errorReasonKey]),
		business.BizMessage(),
		kerrors.WithMetadata(metadata),
	)
}

func newKitexError(
	code int32,
	reason string,
	message string,
	metadata map[string]string,
) error {
	extra := map[string]string{
		errorMarkerKey: errorMarkerValue,
		errorReasonKey: normalizeKitexReason(reason),
	}
	for key, value := range metadata {
		extra[errorMetadataKey+key] = value
	}
	return kitexerrors.NewBizStatusErrorWithExtra(code, message, extra)
}

func (codec *ErrorCodec) allowedKeys() []string {
	if codec == nil {
		return nil
	}
	return codec.allowed
}

func normalizeKitexReason(reason string) string {
	normalized := strings.TrimSpace(reason)
	if normalized == "" {
		return "KITEX_ERROR"
	}
	return normalized
}
