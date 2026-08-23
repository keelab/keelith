// Package validation defines transport-neutral request validation and stable
// field violation errors.
package validation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
)

const (
	// Reason is the stable machine-readable reason used for validation
	// failures.
	Reason = "VALIDATION_FAILED"
	// Code is the transport-neutral invalid-argument code.
	Code int32 = 400

	maxViolations   = 100
	maxFieldBytes   = 512
	maxRuleBytes    = 256
	maxMessageBytes = 1024
)

var (
	// ErrInvalidViolation reports a malformed field violation.
	ErrInvalidViolation = errors.New("validation: invalid violation")
	// ErrInvalidValidator reports a nil validator in a middleware chain.
	ErrInvalidValidator = errors.New("validation: invalid validator")
)

// Violation describes one field rule failure without retaining the rejected
// value.
type Violation struct {
	Field   string `json:"field"`
	Rule    string `json:"rule"`
	Message string `json:"message"`
}

// Error is an immutable collection of field violations.
type Error struct {
	violations []Violation
}

// New validates and snapshots field violations.
func New(violations ...Violation) (*Error, error) {
	if len(violations) == 0 {
		return nil, fmt.Errorf("%w: at least one violation is required", ErrInvalidViolation)
	}
	if len(violations) > maxViolations {
		return nil, fmt.Errorf("%w: violation count exceeds %d", ErrInvalidViolation, maxViolations)
	}
	snapshot := make([]Violation, len(violations))
	for index, violation := range violations {
		violation.Field = strings.TrimSpace(violation.Field)
		violation.Rule = strings.TrimSpace(violation.Rule)
		violation.Message = strings.TrimSpace(violation.Message)

		if !validViolationText(violation.Field, maxFieldBytes) || !validViolationText(violation.Rule, maxRuleBytes) || !validViolationText(violation.Message, maxMessageBytes) {
			return nil, fmt.Errorf("%w: violation %d is empty, oversized, or malformed", ErrInvalidViolation, index)
		}
		snapshot[index] = violation
	}
	return &Error{violations: snapshot}, nil
}

// Error returns the first field message and a count for remaining violations.
func (e *Error) Error() string {
	if e == nil || len(e.violations) == 0 {
		return "validation failed"
	}
	first := e.violations[0]
	if len(e.violations) == 1 {
		return first.Field + ": " + first.Message
	}
	return fmt.Sprintf("%s: %s (and %d more)", first.Field, first.Message, len(e.violations)-1)
}

// Violations returns an independent ordered snapshot.
func (e *Error) Violations() []Violation {
	if e == nil {
		return nil
	}
	return append([]Violation(nil), e.violations...)
}

// Validator validates one request value.
type Validator interface {
	Validate(context.Context, any) error
}

// Func adapts a function to Validator.
type Func func(context.Context, any) error

// Validate delegates to fn.
func (fn Func) Validate(ctx context.Context, request any) error {
	return fn(ctx, request)
}

// ContextValidatable is implemented by request values that validate with a
// cancellation-aware context.
type ContextValidatable interface {
	ValidateContext(context.Context) error
}

// Validatable is implemented by generated or hand-written request values.
type Validatable interface {
	Validate() error
}

// Self validates values implementing ContextValidatable or Validatable.
func Self() Validator {
	return Func(func(ctx context.Context, request any) error {
		switch value := request.(type) {
		case ContextValidatable:
			return value.ValidateContext(ctx)
		case Validatable:
			return value.Validate()
		default:
			return nil
		}
	})
}

// Middleware validates requests in order before invoking the next handler.
func Middleware(validators ...Validator) (middleware.Middleware, error) {
	snapshot := append([]Validator(nil), validators...)
	for index, validator := range snapshot {
		if isNilValidator(validator) {
			return nil, fmt.Errorf("%w: validator %d is nil", ErrInvalidValidator, index)
		}
	}
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			for _, validator := range snapshot {
				if err := validator.Validate(ctx, request); err != nil {
					return nil, FrameworkError(err)
				}
			}
			return next(ctx, request)
		}
	}, nil
}

func isNilValidator(validator Validator) bool {
	if validator == nil {
		return true
	}
	value := reflect.ValueOf(validator)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// FrameworkError converts any validation failure to the common Keelith error
// shape while preserving its cause.
func FrameworkError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if _, ok := errors.AsType[*kerrors.Error](err); ok {
		return err
	}

	message := "request validation failed"
	metadata := map[string]string(nil)

	if validationError, ok := errors.AsType[*Error](err); ok {
		message = validationError.Error()
		content, encodeErr := json.Marshal(validationError.Violations())
		if encodeErr == nil {
			metadata = map[string]string{"violations": string(content)}
		}
	}
	return kerrors.Wrap(err, Code, Reason, message, kerrors.WithMetadata(metadata))
}

func validViolationText(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
