package metadata

import (
	"context"
	"fmt"
	"reflect"
)

type inboundContextKey struct{}
type outboundContextKey struct{}
type localContextKey struct{}

// WithInbound attaches immutable inbound Metadata to ctx.
func WithInbound(ctx context.Context, metadata Metadata) context.Context {
	return context.WithValue(ctx, inboundContextKey{}, metadata.Clone())
}

// Inbound returns an independent copy of inbound Metadata.
func Inbound(ctx context.Context) (Metadata, bool) {
	metadata, ok := ctx.Value(inboundContextKey{}).(Metadata)
	if !ok {
		return Metadata{}, false
	}
	return metadata.Clone(), true
}

// WithOutbound attaches immutable outbound Metadata to ctx.
func WithOutbound(ctx context.Context, metadata Metadata) context.Context {
	return context.WithValue(ctx, outboundContextKey{}, metadata.Clone())
}

// Outbound returns an independent copy of outbound Metadata.
func Outbound(ctx context.Context) (Metadata, bool) {
	metadata, ok := ctx.Value(outboundContextKey{}).(Metadata)
	if !ok {
		return Metadata{}, false
	}
	return metadata.Clone(), true
}

// WithLocal attaches local-only Metadata to ctx.
func WithLocal(ctx context.Context, metadata Metadata) context.Context {
	return context.WithValue(ctx, localContextKey{}, metadata.Clone())
}

// Local returns an independent copy of local-only Metadata.
func Local(ctx context.Context) (Metadata, bool) {
	metadata, ok := ctx.Value(localContextKey{}).(Metadata)
	if !ok {
		return Metadata{}, false
	}
	return metadata.Clone(), true
}

// Carrier is the minimal multi-value transport carrier used by Policy.
type Carrier interface {
	Values(string) []string
	Set(string, []string)
}

// MapCarrier adapts a normalized map to Carrier.
//
// Callers that inject metadata must initialize the map.
type MapCarrier map[string][]string

// Values returns a defensive copy of a normalized carrier value.
func (c MapCarrier) Values(key string) []string {
	normalized, err := normalizeKey(key)
	if err != nil {
		return nil
	}
	values, exists := c[normalized]
	if !exists {
		return nil
	}
	return append([]string(nil), values...)
}

// Set writes a defensive copy under a normalized key.
func (c MapCarrier) Set(key string, values []string) {
	normalized, err := normalizeKey(key)
	if err != nil || c == nil {
		return
	}
	c[normalized] = append([]string(nil), values...)
}

// Policy is an immutable allowlist and byte budget for transport propagation.
type Policy struct {
	allowed   []string
	sensitive map[string]struct{}
	maxBytes  int
}

// PolicyOption configures a propagation Policy.
type PolicyOption interface {
	applyPolicy(*policyOptions) error
}

type policyOptionFunc func(*policyOptions) error

func (f policyOptionFunc) applyPolicy(options *policyOptions) error {
	return f(options)
}

type policyOptions struct {
	sensitive map[string]struct{}
	maxBytes  int
}

// WithSensitiveKeys marks allowed propagation keys as sensitive.
func WithSensitiveKeys(keys ...string) PolicyOption {
	snapshot := append([]string(nil), keys...)
	return policyOptionFunc(func(options *policyOptions) error {
		for _, key := range snapshot {
			normalized, err := normalizeKey(key)
			if err != nil {
				return err
			}
			options.sensitive[normalized] = struct{}{}
		}
		return nil
	})
}

// WithPolicyMaxBytes sets the maximum propagated key/value bytes.
func WithPolicyMaxBytes(maxBytes int) PolicyOption {
	return policyOptionFunc(func(options *policyOptions) error {
		if maxBytes < 0 {
			return fmt.Errorf("%w: policy max bytes must not be negative", ErrInvalidOption)
		}
		options.maxBytes = maxBytes
		return nil
	})
}

// NewPolicy constructs an immutable default-deny propagation policy.
func NewPolicy(allowed []string, options ...PolicyOption) (Policy, error) {
	settings := policyOptions{
		sensitive: make(map[string]struct{}),
		maxBytes:  DefaultMaxBytes,
	}
	for index, option := range options {
		if option == nil {
			return Policy{}, fmt.Errorf("%w: policy option %d is nil", ErrInvalidOption, index)
		}
		if err := option.applyPolicy(&settings); err != nil {
			return Policy{}, fmt.Errorf("metadata policy option %d: %w", index, err)
		}
	}

	normalizedAllowed := make([]string, 0, len(allowed))
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		normalized, err := normalizeKey(key)
		if err != nil {
			return Policy{}, err
		}
		if _, duplicate := allowedSet[normalized]; duplicate {
			return Policy{}, fmt.Errorf("%w: duplicate normalized allowlist key %q", ErrInvalidKey, normalized)
		}
		allowedSet[normalized] = struct{}{}
		normalizedAllowed = append(normalizedAllowed, normalized)
	}
	for key := range settings.sensitive {
		if _, allowed := allowedSet[key]; !allowed {
			return Policy{}, fmt.Errorf("%w: sensitive key %q is not allowlisted", ErrInvalidOption, key)
		}
	}

	return Policy{
		allowed:   normalizedAllowed,
		sensitive: cloneSet(settings.sensitive),
		maxBytes:  settings.maxBytes,
	}, nil
}

// Extract copies only allowlisted fields from carrier.
func (p Policy) Extract(carrier Carrier) (Metadata, error) {
	if isNilCarrier(carrier) {
		return Metadata{}, ErrNilCarrier
	}
	values := make(map[string][]string, len(p.allowed))
	for _, key := range p.allowed {
		if extracted := carrier.Values(key); extracted != nil {
			values[key] = extracted
		}
	}
	return New(
		values,
		WithMaxBytes(p.maxBytes),
		WithSensitive(setValues(p.sensitive)...),
	)
}

// Inject copies only allowlisted fields into carrier.
func (p Policy) Inject(metadata Metadata, carrier Carrier) error {
	if isNilCarrier(carrier) {
		return ErrNilCarrier
	}
	values := make(map[string][]string, len(p.allowed))
	for _, key := range p.allowed {
		if propagated := metadata.Values(key); propagated != nil {
			values[key] = propagated
		}
	}
	validated, err := New(values, WithMaxBytes(p.maxBytes))
	if err != nil {
		return err
	}
	for _, key := range validated.Keys() {
		carrier.Set(key, validated.Values(key))
	}
	return nil
}

func setValues(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	return values
}

func isNilCarrier(carrier Carrier) bool {
	if carrier == nil {
		return true
	}
	value := reflect.ValueOf(carrier)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
