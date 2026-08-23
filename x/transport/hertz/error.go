package hertz

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	kerrors "github.com/keelab/keelith/errors"
)

type errorEnvelope struct {
	Code     int32             `json:"code"`
	Reason   string            `json:"reason"`
	Message  string            `json:"message"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (server *Server) writeError(
	request *app.RequestContext,
	err error,
) {
	status, envelope := server.errorEnvelope(err)
	payload, marshalErr := json.Marshal(envelope)
	if marshalErr != nil ||
		len(payload)+1 > server.semantics.maxResponseBytes {
		status = consts.StatusInternalServerError
		payload = []byte(
			`{"code":500,"reason":"INTERNAL","message":"internal server error"}`,
		)
	}
	request.Response.Reset()
	request.Response.Header.Set("X-Content-Type-Options", "nosniff")
	request.Data(
		status,
		"application/json; charset=utf-8",
		append(payload, '\n'),
	)
}

func (server *Server) errorEnvelope(err error) (int, errorEnvelope) {
	var frameworkError *kerrors.Error
	if errors.As(err, &frameworkError) {
		status := statusFromCode(frameworkError.Code())
		return status, errorEnvelope{
			Code:    frameworkError.Code(),
			Reason:  frameworkError.Reason(),
			Message: frameworkError.Message(),
			Metadata: server.filterErrorMetadata(
				frameworkError.Metadata(),
			),
		}
	}
	switch {
	case errors.Is(err, ErrRequestTooLarge):
		return consts.StatusRequestEntityTooLarge, errorEnvelope{
			Code:    consts.StatusRequestEntityTooLarge,
			Reason:  "REQUEST_TOO_LARGE",
			Message: "request body exceeds configured limit",
		}
	case errors.Is(err, ErrHeaderTooLarge):
		return consts.StatusRequestHeaderFieldsTooLarge, errorEnvelope{
			Code:    consts.StatusRequestHeaderFieldsTooLarge,
			Reason:  "HEADER_TOO_LARGE",
			Message: "request headers exceed configured limit",
		}
	case errors.Is(err, ErrResponseTooLarge):
		return consts.StatusInternalServerError, errorEnvelope{
			Code:    consts.StatusInternalServerError,
			Reason:  "RESPONSE_TOO_LARGE",
			Message: "response body exceeds configured limit",
		}
	default:
		return consts.StatusInternalServerError, errorEnvelope{
			Code:    consts.StatusInternalServerError,
			Reason:  "INTERNAL",
			Message: "internal server error",
		}
	}
}

func (server *Server) filterErrorMetadata(
	source map[string]string,
) map[string]string {
	if len(source) == 0 || len(server.semantics.errorMetadata) == 0 {
		return nil
	}
	result := make(map[string]string)
	for key, value := range source {
		normalized := strings.ToLower(key)
		if _, allowed := server.semantics.errorMetadata[normalized]; allowed {
			result[normalized] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func statusFromCode(code int32) int {
	if code >= consts.StatusBadRequest && code <= 599 {
		return int(code)
	}
	return consts.StatusInternalServerError
}

func normalizeErrorMetadataKeys(keys []string) ([]string, error) {
	normalized := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		value := strings.ToLower(strings.TrimSpace(key))
		if !validMetadataKey(value) {
			return nil, errors.New(
				"hertz profile: error metadata key is malformed",
			)
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, errors.New(
				"hertz profile: duplicate error metadata key",
			)
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func validMetadataKey(key string) bool {
	if key == "" {
		return false
	}
	for _, character := range key {
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' ||
			character == '_' ||
			character == '.' {
			continue
		}
		return false
	}
	return true
}
