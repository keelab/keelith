// Package secret defines provider-neutral secret references and values.
package secret

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
)

var (
	// ErrInvalidReference reports a malformed secret:// reference.
	ErrInvalidReference = errors.New("secret: invalid reference")
	// ErrInvalidValue reports incomplete secret material.
	ErrInvalidValue = errors.New("secret: invalid value")
	// ErrProviderNotFound reports a reference to an unregistered provider.
	ErrProviderNotFound = errors.New("secret: provider not found")
	// ErrNotFound reports a provider key that does not exist.
	ErrNotFound = errors.New("secret: value not found")
	// ErrWatchUnsupported reports a provider without hot-update support.
	ErrWatchUnsupported = errors.New("secret: watch is unsupported")
	// ErrWatcherClosed reports a closed secret Watcher.
	ErrWatcherClosed = errors.New("secret: watcher closed")
)

// Reference identifies secret material without storing it in a config
// snapshot.
type Reference struct {
	provider string
	key      string
}

// NewReference validates a provider and provider-local key.
func NewReference(provider, key string) (Reference, error) {
	if !validProvider(provider) || !validKey(key) {
		return Reference{}, fmt.Errorf(
			"%w: provider %q key %q",
			ErrInvalidReference,
			provider,
			key,
		)
	}
	return Reference{provider: provider, key: key}, nil
}

// Parse parses secret://provider/path/to/key.
func Parse(raw string) (Reference, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return Reference{}, fmt.Errorf("%w: %w", ErrInvalidReference, err)
	}
	if parsed.Scheme != "secret" ||
		parsed.Host == "" ||
		parsed.User != nil ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return Reference{}, fmt.Errorf("%w: %q", ErrInvalidReference, raw)
	}
	key, err := url.PathUnescape(strings.TrimPrefix(parsed.EscapedPath(), "/"))
	if err != nil {
		return Reference{}, fmt.Errorf("%w: %w", ErrInvalidReference, err)
	}
	return NewReference(parsed.Host, key)
}

// Provider returns the provider selector.
func (reference Reference) Provider() string { return reference.provider }

// Key returns the provider-local key.
func (reference Reference) Key() string { return reference.key }

// String returns a safe reference and never secret material.
func (reference Reference) String() string {
	return "secret://" + reference.provider + "/" + escapePath(reference.key)
}

// Value is an immutable copy of resolved secret material.
type Value struct {
	content   []byte
	version   string
	expiresAt time.Time
}

// NewValue snapshots secret bytes and provider revision.
func NewValue(content []byte, version string, expiresAt time.Time) (Value, error) {
	version = strings.TrimSpace(version)
	if len(content) == 0 || version == "" {
		return Value{}, fmt.Errorf(
			"%w: content and version are required",
			ErrInvalidValue,
		)
	}
	return Value{
		content:   append([]byte(nil), content...),
		version:   version,
		expiresAt: expiresAt,
	}, nil
}

// Bytes returns an independent secret copy.
func (value Value) Bytes() []byte {
	return append([]byte(nil), value.content...)
}

// TrimLineBreaks removes file-style record delimiters around secret material.
func TrimLineBreaks(content []byte) []byte {
	return bytes.Trim(content, "\r\n")
}

// Version returns the provider revision.
func (value Value) Version() string { return value.version }

// ExpiresAt returns the provider expiry, or zero when unknown.
func (value Value) ExpiresAt() time.Time { return value.expiresAt }

// Expired reports whether a non-zero expiry has passed.
func (value Value) Expired(now time.Time) bool {
	return !value.expiresAt.IsZero() && !now.Before(value.expiresAt)
}

// Validate reports whether Value contains provider revision and material.
func (value Value) Validate() error {
	if len(value.content) == 0 || strings.TrimSpace(value.version) == "" {
		return ErrInvalidValue
	}
	return nil
}

// Provider resolves provider-local keys and optionally watches them.
type Provider interface {
	Resolve(context.Context, string) (Value, error)
	Watch(context.Context, string) (Watcher, error)
}

// ResolveStatus is the closed, material-free outcome of a classified resolve.
// It never carries provider errors, cancellation causes, references, or secret
// material.
type ResolveStatus uint8

const (
	_ ResolveStatus = iota
	// ResolveStatusSuccess reports a validated Value.
	ResolveStatusSuccess
	// ResolveStatusNotFound reports an absent provider object or data key.
	ResolveStatusNotFound
	// ResolveStatusInvalid reports an invalid reference, key, object, or value.
	ResolveStatusInvalid
	// ResolveStatusUnavailable reports a provider that could not resolve safely.
	ResolveStatusUnavailable
	// ResolveStatusCanceled reports cancellation or deadline expiry.
	ResolveStatusCanceled
)

// ClassifiedProvider is an optional Provider capability for callers that need
// a closed outcome without inspecting an error chain. Implementations must
// return a zero Value for every non-success status.
type ClassifiedProvider interface {
	Provider
	ResolveClassified(context.Context, string) (Value, ResolveStatus)
}

// Watcher emits complete replacement values.
type Watcher interface {
	Next(context.Context) (Value, error)
	Close() error
}

// Registration binds a stable provider name to an implementation.
type Registration struct {
	Name     string
	Provider Provider
}

// Manager routes references to immutable provider registrations.
type Manager struct {
	providers map[string]Provider
	names     []string
}

// NewManager validates and snapshots provider registrations.
func NewManager(registrations ...Registration) (*Manager, error) {
	providers := make(map[string]Provider, len(registrations))
	names := make([]string, 0, len(registrations))
	for index, registration := range registrations {
		if !validProvider(registration.Name) || isNil(registration.Provider) {
			return nil, fmt.Errorf(
				"%w: registration %d",
				ErrInvalidReference,
				index,
			)
		}
		if _, duplicate := providers[registration.Name]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate provider %q",
				ErrInvalidReference,
				registration.Name,
			)
		}
		providers[registration.Name] = registration.Provider
		names = append(names, registration.Name)
	}
	sort.Strings(names)
	return &Manager{providers: providers, names: names}, nil
}

// Providers returns registered provider names in lexical order.
func (m *Manager) Providers() []string {
	if m == nil {
		return nil
	}
	return append([]string(nil), m.names...)
}

// Resolve resolves a reference without caching or logging its value.
func (m *Manager) Resolve(
	ctx context.Context,
	reference Reference,
) (Value, error) {
	provider, err := m.provider(reference)
	if err != nil {
		return Value{}, err
	}
	value, err := provider.Resolve(ctx, reference.key)
	if err != nil {
		return Value{}, fmt.Errorf(
			"secret: resolve %s: %w",
			reference.String(),
			err,
		)
	}
	return value, nil
}

// ResolveClassified resolves a reference into a validated Value and a closed
// status. It does not inspect, wrap, or expose arbitrary provider errors or
// cancellation causes. Providers without ClassifiedProvider support are not
// called and return ResolveStatusUnavailable.
func (m *Manager) ResolveClassified(
	ctx context.Context,
	reference Reference,
) (Value, ResolveStatus) {
	if isNil(ctx) || m == nil {
		return Value{}, ResolveStatusInvalid
	}
	if ctx.Err() != nil {
		return Value{}, ResolveStatusCanceled
	}
	if !validProvider(reference.provider) || !validKey(reference.key) {
		return Value{}, ResolveStatusInvalid
	}
	provider, exists := m.providers[reference.provider]
	if !exists || isNil(provider) {
		return Value{}, ResolveStatusInvalid
	}

	if classified, ok := provider.(ClassifiedProvider); ok {
		if isNil(classified) {
			return Value{}, ResolveStatusInvalid
		}
		value, status := classified.ResolveClassified(ctx, reference.key)
		if ctx.Err() != nil {
			return Value{}, ResolveStatusCanceled
		}
		return normalizeResolve(value, status)
	}

	return Value{}, ResolveStatusUnavailable
}

// Watch watches complete replacements for a reference.
func (m *Manager) Watch(
	ctx context.Context,
	reference Reference,
) (Watcher, error) {
	provider, err := m.provider(reference)
	if err != nil {
		return nil, err
	}
	watcher, err := provider.Watch(ctx, reference.key)
	if err != nil {
		return nil, fmt.Errorf(
			"secret: watch %s: %w",
			reference.String(),
			err,
		)
	}
	if isNil(watcher) {
		return nil, fmt.Errorf(
			"secret: watch %s returned a nil watcher",
			reference.String(),
		)
	}
	return watcher, nil
}

func (m *Manager) provider(reference Reference) (Provider, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: manager is nil", ErrProviderNotFound)
	}
	if _, err := NewReference(reference.provider, reference.key); err != nil {
		return nil, err
	}
	provider, exists := m.providers[reference.provider]
	if !exists {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrProviderNotFound,
			reference.provider,
		)
	}
	return provider, nil
}

func validProvider(provider string) bool {
	if provider == "" || strings.ToLower(provider) != provider {
		return false
	}
	for index, character := range provider {
		if character >= 'a' && character <= 'z' ||
			index > 0 && character >= '0' && character <= '9' ||
			index > 0 && (character == '-' || character == '_') {
			continue
		}
		return false
	}
	return true
}

func validKey(key string) bool {
	if key == "" ||
		strings.TrimSpace(key) != key ||
		strings.HasPrefix(key, "/") ||
		strings.HasSuffix(key, "/") {
		return false
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
		for _, character := range segment {
			if unicode.IsControl(character) {
				return false
			}
		}
	}
	return true
}

func escapePath(key string) string {
	segments := strings.Split(key, "/")
	for index, segment := range segments {
		segments[index] = url.PathEscape(segment)
	}
	return strings.Join(segments, "/")
}

func normalizeResolve(value Value, status ResolveStatus) (Value, ResolveStatus) {
	switch status {
	case ResolveStatusSuccess:
		if err := value.Validate(); err != nil {
			return Value{}, ResolveStatusInvalid
		}
		return value, ResolveStatusSuccess
	case ResolveStatusNotFound,
		ResolveStatusInvalid,
		ResolveStatusUnavailable,
		ResolveStatusCanceled:
		return Value{}, status
	default:
		return Value{}, ResolveStatusUnavailable
	}
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
