package placement

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxIdentityBytes = 256
	// NoGroup is the bounded telemetry value for a direct Profile Binding.
	// Placement.Group itself remains empty so callers can distinguish absence.
	NoGroup = "_none"
)

// ErrInvalid reports an empty, malformed, or unbounded placement identity.
var ErrInvalid = errors.New("placement: invalid placement")

// Placement identifies the listener, profile, optional group, and generated
// service that own one inbound invocation. Every value is startup-scoped and
// safe for low-cardinality diagnostics and telemetry.
type Placement struct {
	listener string
	profile  string
	group    string
	service  string
}

// New validates and constructs an immutable Placement. Group may be empty for
// a Binding declared directly in a Profile.
func New(listener, profile, group, service string) (Placement, error) {
	if !validIdentity(listener, false) ||
		!validIdentity(profile, false) ||
		!validIdentity(group, true) ||
		!validIdentity(service, false) {
		return Placement{}, fmt.Errorf("%w: identity is empty or malformed", ErrInvalid)
	}
	return Placement{
		listener: listener,
		profile:  profile,
		group:    group,
		service:  service,
	}, nil
}

// Listener returns the stable listener identity.
func (value Placement) Listener() string { return value.listener }

// Profile returns the stable service Profile identity.
func (value Placement) Profile() string { return value.profile }

// Group returns the optional stable policy Group identity.
func (value Placement) Group() string { return value.group }

// Service returns the generated service identity.
func (value Placement) Service() string { return value.service }

// GroupAttribute returns a stable non-empty, low-cardinality telemetry value.
func (value Placement) GroupAttribute() string {
	if value.group == "" {
		return NoGroup
	}
	return value.group
}

type contextKey struct{}

// WithContext attaches value to ctx.
func WithContext(ctx context.Context, value Placement) context.Context {
	return context.WithValue(ctx, contextKey{}, value)
}

// FromContext returns the Placement attached to ctx.
func FromContext(ctx context.Context) (Placement, bool) {
	value, ok := ctx.Value(contextKey{}).(Placement)
	return value, ok
}

func validIdentity(value string, optional bool) bool {
	if value == "" {
		return optional
	}
	if len(value) > maxIdentityBytes || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
