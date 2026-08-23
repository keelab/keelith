package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"sort"
	"strings"

	kerrors "github.com/keelab/keelith/errors"
)

var (
	// ErrInvalidOption means a transport option is unsafe or incomplete.
	ErrInvalidOption = errors.New("http transport: invalid option")
	// ErrInvalidRoute means a route identity, codec, or handler is invalid.
	ErrInvalidRoute = errors.New("http transport: invalid route")
	// ErrAlreadyStarted means Server.Start was called more than once.
	ErrAlreadyStarted = errors.New("http transport: server already started")
	// ErrNilContext means a public operation received a nil context.
	ErrNilContext = errors.New("http transport: nil context")
	// ErrRequestTooLarge means an HTTP request body exceeded its budget.
	ErrRequestTooLarge = errors.New("http transport: request body too large")
	// ErrResponseTooLarge means an HTTP response body exceeded its budget.
	ErrResponseTooLarge = errors.New("http transport: response body too large")
	// ErrHeaderTooLarge means HTTP headers exceeded their budget.
	ErrHeaderTooLarge = errors.New("http transport: headers too large")
	// ErrInvalidCall means a ClientCall is incomplete.
	ErrInvalidCall = errors.New("http transport: invalid client call")
)

// ErrorEncoder writes a transport-neutral error to HTTP.
type ErrorEncoder func(context.Context, nethttp.ResponseWriter, error)

// ErrorEnvelope is Keelith's default HTTP JSON error representation.
type ErrorEnvelope struct {
	Code     int32             `json:"code"`
	Reason   string            `json:"reason"`
	Message  string            `json:"message"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func newErrorEncoder(allowedMetadata []string) ErrorEncoder {
	allowed := make(map[string]struct{}, len(allowedMetadata))
	for _, key := range allowedMetadata {
		allowed[strings.ToLower(key)] = struct{}{}
	}
	return func(
		_ context.Context,
		writer nethttp.ResponseWriter,
		err error,
	) {
		status, envelope := errorEnvelope(err, allowed)
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.WriteHeader(status)
		_ = json.NewEncoder(writer).Encode(envelope)
	}
}

func errorEnvelope(
	err error,
	allowedMetadata map[string]struct{},
) (int, ErrorEnvelope) {
	var frameworkErr *kerrors.Error
	if errors.As(err, &frameworkErr) {
		status := statusFromCode(frameworkErr.Code())
		return status, ErrorEnvelope{
			Code:     frameworkErr.Code(),
			Reason:   frameworkErr.Reason(),
			Message:  frameworkErr.Message(),
			Metadata: filterErrorMetadata(frameworkErr.Metadata(), allowedMetadata),
		}
	}

	status := nethttp.StatusInternalServerError
	reason := "INTERNAL"
	message := "internal server error"
	var maxBytesError *nethttp.MaxBytesError
	switch {
	case errors.As(err, &maxBytesError), errors.Is(err, ErrRequestTooLarge):
		status = nethttp.StatusRequestEntityTooLarge
		reason = "REQUEST_TOO_LARGE"
		message = "request body exceeds configured limit"
	case errors.Is(err, ErrHeaderTooLarge):
		status = nethttp.StatusRequestHeaderFieldsTooLarge
		reason = "HEADER_TOO_LARGE"
		message = "request headers exceed configured limit"
	case errors.Is(err, ErrResponseTooLarge):
		reason = "RESPONSE_TOO_LARGE"
		message = "response body exceeds configured limit"
	default:
		var syntaxError *json.SyntaxError
		var typeError *json.UnmarshalTypeError
		if errors.As(err, &syntaxError) ||
			errors.As(err, &typeError) ||
			errors.Is(err, io.ErrUnexpectedEOF) {
			status = nethttp.StatusBadRequest
			reason = "INVALID_REQUEST"
			message = "request body is invalid"
		}
	}
	return status, ErrorEnvelope{
		Code:    int32(status),
		Reason:  reason,
		Message: message,
	}
}

func statusFromCode(code int32) int {
	if code >= nethttp.StatusBadRequest && code <= 599 {
		return int(code)
	}
	return nethttp.StatusInternalServerError
}

func filterErrorMetadata(
	source map[string]string,
	allowed map[string]struct{},
) map[string]string {
	if len(source) == 0 || len(allowed) == 0 {
		return nil
	}
	result := make(map[string]string)
	for key, value := range source {
		normalized := strings.ToLower(key)
		if _, ok := allowed[normalized]; ok {
			result[normalized] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func validateErrorMetadata(keys []string) ([]string, error) {
	normalized := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		value := strings.ToLower(strings.TrimSpace(key))
		if value == "" {
			return nil, fmt.Errorf("%w: error metadata key is empty", ErrInvalidOption)
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate error metadata key %q",
				ErrInvalidOption,
				value,
			)
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	sort.Strings(normalized)
	return normalized, nil
}
