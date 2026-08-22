// Package errors defines transport-neutral Keelith application errors.
package errors

import (
	"maps"
	"strconv"
)

// Error is an immutable transport-neutral application error.
//
// Code and Reason form its stable machine identity. Message is human-facing
// and may vary without changing errors.Is matching.
type Error struct {
	code     int32
	reason   string
	message  string
	metadata map[string]string
	cause    error
}

// Option configures a new Error or an immutable Clone.
type Option interface {
	apply(*Error)
}

type optionFunc func(*Error)

func (function optionFunc) apply(target *Error) {
	function(target)
}

// WithMessage replaces the human-facing message.
func WithMessage(message string) Option {
	return optionFunc(func(target *Error) {
		target.message = message
	})
}

// WithMetadata merges a defensive copy of metadata into the Error.
func WithMetadata(metadata map[string]string) Option {
	snapshot := cloneMetadata(metadata)
	return optionFunc(func(target *Error) {
		if len(snapshot) == 0 {
			return
		}
		if target.metadata == nil {
			target.metadata = make(map[string]string, len(snapshot))
		}
		for key, value := range snapshot {
			target.metadata[key] = value
		}
	})
}

// New constructs an Error without a Cause.
func New(code int32, reason, message string, options ...Option) *Error {
	return newError(nil, code, reason, message, options...)
}

// Wrap constructs an Error that unwraps to cause.
func Wrap(cause error, code int32, reason, message string, options ...Option) *Error {
	return newError(cause, code, reason, message, options...)
}

func newError(cause error, code int32, reason, message string, options ...Option) *Error {
	target := &Error{
		code:    code,
		reason:  reason,
		message: message,
		cause:   cause,
	}
	for _, option := range options {
		if option != nil {
			option.apply(target)
		}
	}
	return target
}

// Error returns a human-readable description without exposing the Cause.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	switch {
	case e.reason != "" && e.message != "":
		return e.reason + ": " + e.message
	case e.reason != "":
		return e.reason
	case e.message != "":
		return e.message
	default:
		return "error code " + strconv.FormatInt(int64(e.code), 10)
	}
}

// Unwrap returns the private implementation Cause.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Is matches another Error by stable Code and Reason.
func (e *Error) Is(candidate error) bool {
	other, ok := candidate.(*Error)
	if !ok || e == nil || other == nil {
		return false
	}
	return e.code == other.code && e.reason == other.reason
}

// Clone returns an independent immutable copy with options applied.
func (e *Error) Clone(options ...Option) *Error {
	if e == nil {
		return nil
	}
	clone := &Error{
		code:     e.code,
		reason:   e.reason,
		message:  e.message,
		metadata: cloneMetadata(e.metadata),
		cause:    e.cause,
	}
	for _, option := range options {
		if option != nil {
			option.apply(clone)
		}
	}
	return clone
}

// Code returns the stable numeric application code.
func (e *Error) Code() int32 {
	if e == nil {
		return 0
	}
	return e.code
}

// Reason returns the stable machine-readable reason.
func (e *Error) Reason() string {
	if e == nil {
		return ""
	}
	return e.reason
}

// Message returns the human-facing message.
func (e *Error) Message() string {
	if e == nil {
		return ""
	}
	return e.message
}

// Metadata returns a defensive copy of error metadata.
func (e *Error) Metadata() map[string]string {
	if e == nil {
		return nil
	}
	return cloneMetadata(e.metadata)
}

func cloneMetadata(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string]string, len(source))
	maps.Copy(clone, source)
	return clone
}
