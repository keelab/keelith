// Package protovalidate adapts Buf Protovalidate to Keelith's
// transport-neutral validation contract.
package protovalidate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	bufprotovalidate "buf.build/go/protovalidate"
	keelitherrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
	corevalidation "github.com/keelab/keelith/validation"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxViolations = 100
	maxFieldBytes        = 512
	maxRuleBytes         = 256
	maxMessageBytes      = 1024

	// RuntimeReason is the stable internal failure reason.
	RuntimeReason = "VALidATION_RUNTIME_FAILED"
)

var (
	// ErrInvalidOption means an Adapter option is nil or out of range.
	ErrInvalidOption = errors.New("protovalidate: invalid option")
	// ErrUnsupportedRequest means the adapter received a non-Protobuf value.
	ErrUnsupportedRequest = errors.New("protovalidate: unsupported request")
)

// Option configures an Adapter.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (function optionFunc) apply(options *options) error {
	return function(options)
}

type options struct {
	validator     bufprotovalidate.Validator
	maxViolations int
}

// WithValidator uses an application-owned concurrent Protovalidate runtime.
func WithValidator(validator bufprotovalidate.Validator) Option {
	return optionFunc(func(options *options) error {
		if isNilValidator(validator) {
			return fmt.Errorf("validator is nil")
		}
		options.validator = validator
		return nil
	})
}

// WithMaxViolations bounds field details copied to framework error metadata.
func WithMaxViolations(maximum int) Option {
	return optionFunc(func(options *options) error {
		if maximum <= 0 || maximum > defaultMaxViolations {
			return fmt.Errorf(
				"maximum violations must be within [1, %d]",
				defaultMaxViolations,
			)
		}
		options.maxViolations = maximum
		return nil
	})
}

// Adapter validates proto.Message requests and maps rule failures to
// validation.Error without retaining rejected field values.
type Adapter struct {
	validator     bufprotovalidate.Validator
	maxViolations int
}

// Messages constructs a Validator that applies Protovalidate only to
// proto.Message requests. Other request kinds pass through unchanged so one
// mixed http router can also serve transport-neutral health or operational
// handlers.
//
// A typed nil proto.Message still reaches Adapter and is treated as a runtime
// wiring failure.
func Messages(optionList ...Option) (corevalidation.Validator, error) {
	adapter, err := New(optionList...)
	if err != nil {
		return nil, err
	}
	return corevalidation.Func(func(ctx context.Context, request any) error {
		if request == nil {
			return nil
		}
		if _, ok := request.(proto.Message); !ok {
			return nil
		}
		return adapter.Validate(ctx, request)
	}), nil
}

// ServerStreamMessages validates each successfully decoded inbound Protobuf
// message on a server stream. Receive transport errors take precedence and
// outbound/client-side events pass through unchanged.
func ServerStreamMessages(
	optionList ...Option,
) (middleware.StreamMiddleware, error) {
	messageValidator, err := Messages(optionList...)
	if err != nil {
		return nil, err
	}
	return func(next middleware.StreamHandler) middleware.StreamHandler {
		return func(ctx context.Context, event middleware.StreamEvent) error {
			err := next(ctx, event)
			if err != nil ||
				event.Side != middleware.StreamSideServer ||
				event.Phase != middleware.StreamPhaseReceive {
				return err
			}
			return corevalidation.FrameworkError(
				messageValidator.Validate(ctx, event.Message),
			)
		}
	}, nil
}

// New constructs an Adapter backed by Protovalidate's shared global runtime.
func New(optionList ...Option) (*Adapter, error) {
	settings := options{
		validator:     bufprotovalidate.GlobalValidator,
		maxViolations: defaultMaxViolations,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf(
				"%w: option %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
	}
	return &Adapter{
		validator:     settings.validator,
		maxViolations: settings.maxViolations,
	}, nil
}

// Validate implements validation.Validator.
func (adapter *Adapter) Validate(
	ctx context.Context,
	request any,
) error {
	if ctx == nil {
		return runtimeFailure(ErrUnsupportedRequest)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if adapter == nil || isNilValidator(adapter.validator) {
		return runtimeFailure(ErrInvalidOption)
	}
	message, ok := request.(proto.Message)
	if !ok || isNilProto(message) {
		return runtimeFailure(ErrUnsupportedRequest)
	}
	err := callValidator(adapter.validator, message)
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err == nil {
		return nil
	}
	var validationError *bufprotovalidate.ValidationError
	if !errors.As(err, &validationError) ||
		len(validationError.Violations) == 0 {
		return runtimeFailure(err)
	}

	limit := min(adapter.maxViolations, len(validationError.Violations))
	violations := make([]corevalidation.Violation, 0, limit)
	for _, violation := range validationError.Violations[:limit] {
		violations = append(violations, mapViolation(violation))
	}
	mapped, mapErr := corevalidation.New(violations...)
	if mapErr != nil {
		return runtimeFailure(mapErr)
	}
	return mapped
}

func mapViolation(
	violation *bufprotovalidate.Violation,
) corevalidation.Violation {
	field := "$"
	rule := "protovalidate"
	message := "validation rule failed"
	if violation != nil && violation.Proto != nil {
		field = boundedText(
			bufprotovalidate.FieldPathString(
				violation.Proto.GetField(),
			),
			maxFieldBytes,
			field,
		)
		ruleValue := violation.Proto.GetRuleId()
		if ruleValue == "" {
			ruleValue = bufprotovalidate.FieldPathString(
				violation.Proto.GetRule(),
			)
		}
		rule = boundedText(ruleValue, maxRuleBytes, rule)
		message = boundedText(
			violation.Proto.GetMessage(),
			maxMessageBytes,
			message,
		)
	}
	return corevalidation.Violation{
		Field:   field,
		Rule:    rule,
		Message: message,
	}
}

func boundedText(value string, maxBytes int, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" ||
		len(value) > maxBytes ||
		!utf8.ValidString(value) {
		return fallback
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fallback
		}
	}
	return value
}

func callValidator(
	validator bufprotovalidate.Validator,
	message proto.Message,
) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("protovalidate runtime panicked")
		}
	}()
	return validator.Validate(message)
}

func runtimeFailure(cause error) error {
	return keelitherrors.Wrap(
		cause,
		500,
		RuntimeReason,
		"request validation unavailable",
	)
}

func isNilValidator(validator bufprotovalidate.Validator) bool {
	if validator == nil {
		return true
	}
	value := reflect.ValueOf(validator)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilProto(message proto.Message) bool {
	value := reflect.ValueOf(message)
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		return value.IsNil()
	default:
		return false
	}
}

var _ corevalidation.Validator = (*Adapter)(nil)
