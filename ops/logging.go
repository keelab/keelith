package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/observability/logging/audit"
)

const maxLoggingAdminBody = 4 * 1024

// LoggingController is the minimum mutable logging policy required by Ops.
type LoggingController interface {
	SetLevel(string) error
	Status() logging.Status
	UpdateLevel(logging.Update) (logging.Status, error)
	Restore(logging.Status, uint64) (logging.Status, error)
}

// LoggingAdminConfig enables value-free logging status and a protected level
// update operation. Audit is mandatory because the mutation changes production
// observability and may increase data volume.
type LoggingAdminConfig struct {
	Controller LoggingController
	Audit      *audit.Logger
}

// WithLoggingAdmin exposes GET /debug/logging and PUT /admin/logging/level.
// Both endpoints always use the Ops diagnostic AccessPolicy.
func WithLoggingAdmin(config LoggingAdminConfig) Option {
	return optionFunc(func(options *options) error {
		if config.Controller == nil || config.Audit == nil {
			return fmt.Errorf("logging controller and audit logger are required")
		}
		options.loggingAdmin = loggingAdminHandler(config)
		return nil
	})
}

type loggingLevelRequest struct {
	Level            string `json:"level"`
	ExpectedRevision uint64 `json:"expectedRevision"`
	TTL              string `json:"ttl"`
	Reason           string `json:"reason"`
}

func loggingAdminHandler(config LoggingAdminConfig) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			writeDiagnosticJSON(writer, http.StatusOK, config.Controller.Status())
		case http.MethodPut:
			updateLoggingLevel(writer, request, config)
		default:
			writeDiagnosticError(writer, http.StatusMethodNotAllowed, "method_not_allowed")
		}
	})
}

func updateLoggingLevel(writer http.ResponseWriter, request *http.Request, config LoggingAdminConfig) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxLoggingAdminBody)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var input loggingLevelRequest
	if err := decoder.Decode(&input); err != nil {
		writeDiagnosticError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeDiagnosticError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	input.Level = strings.TrimSpace(input.Level)
	input.Reason = strings.TrimSpace(input.Reason)
	principal, authenticated := PrincipalFromContext(request.Context())
	var ttl time.Duration
	if input.TTL != "" {
		var err error
		ttl, err = time.ParseDuration(input.TTL)
		if err != nil || ttl <= 0 || ttl > 24*time.Hour {
			writeDiagnosticError(writer, http.StatusBadRequest, "invalid_request")
			return
		}
	}
	if !authenticated || input.Reason == "" {
		writeDiagnosticError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	previous := config.Controller.Status()
	current, err := config.Controller.UpdateLevel(logging.Update{Level: input.Level, ExpectedRevision: input.ExpectedRevision, TTL: ttl})
	if err != nil {
		_ = config.Audit.Record(request.Context(), audit.Event{
			Actor: principal.Subject, Action: "logging.level.update", ResourceType: "logging",
			ResourceID: "application", Outcome: "denied", Reason: input.Reason,
		})
		if errors.Is(err, logging.ErrRevisionConflict) {
			writeDiagnosticError(writer, http.StatusConflict, "revision_conflict")
			return
		}
		writeDiagnosticError(writer, http.StatusBadRequest, "invalid_level")
		return
	}
	if err := config.Audit.Record(request.Context(), audit.Event{
		Actor: principal.Subject, Action: "logging.level.update", ResourceType: "logging",
		ResourceID: previous.Level + "->" + current.Level,
		Outcome:    "succeeded", Reason: input.Reason,
	}); err != nil {
		_, _ = config.Controller.Restore(previous, current.Revision)
		writeDiagnosticError(writer, http.StatusServiceUnavailable, "audit_unavailable")
		return
	}
	writeDiagnosticJSON(writer, http.StatusOK, current)
}
