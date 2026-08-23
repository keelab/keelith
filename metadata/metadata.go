// Package metadata provides immutable request metadata and explicit
// propagation policies.
package metadata

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	// DefaultMaxBytes is the default metadata key/value budget.
	DefaultMaxBytes = 8 * 1024
)

var (
	// ErrInvalidKey means a metadata key cannot be normalized safely.
	ErrInvalidKey = errors.New("metadata: invalid key")
	// ErrTooLarge means metadata exceeds its configured byte budget.
	ErrTooLarge = errors.New("metadata: size limit exceeded")
	// ErrInvalidOption means a metadata option is invalid.
	ErrInvalidOption = errors.New("metadata: invalid option")
	// ErrNilCarrier means a propagation Carrier is nil.
	ErrNilCarrier = errors.New("metadata: nil carrier")
)

// Metadata is an immutable collection of normalized multi-value metadata.
type Metadata struct {
	values    map[string][]string
	sensitive map[string]struct{}
}

// Option configures Metadata construction.
type Option interface {
	apply(*metadataOptions) error
}

type optionFunc func(*metadataOptions) error

func (f optionFunc) apply(options *metadataOptions) error {
	return f(options)
}

type metadataOptions struct {
	maxBytes  int
	sensitive map[string]struct{}
}

// WithMaxBytes sets the maximum combined key/value bytes.
func WithMaxBytes(maxBytes int) Option {
	return optionFunc(func(options *metadataOptions) error {
		if maxBytes < 0 {
			return fmt.Errorf("%w: max bytes must not be negative", ErrInvalidOption)
		}
		options.maxBytes = maxBytes
		return nil
	})
}

// WithSensitive marks keys for redaction without granting propagation rights.
func WithSensitive(keys ...string) Option {
	snapshot := append([]string(nil), keys...)
	return optionFunc(func(options *metadataOptions) error {
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

// New constructs immutable Metadata and enforces its byte budget.
func New(values map[string][]string, options ...Option) (Metadata, error) {
	settings := metadataOptions{
		maxBytes:  DefaultMaxBytes,
		sensitive: make(map[string]struct{}),
	}
	for index, option := range options {
		if option == nil {
			return Metadata{}, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return Metadata{}, fmt.Errorf("metadata option %d: %w", index, err)
		}
	}

	result := Metadata{
		values:    make(map[string][]string, len(values)),
		sensitive: cloneSet(settings.sensitive),
	}
	size := 0
	for key, sourceValues := range values {
		normalized, err := normalizeKey(key)
		if err != nil {
			return Metadata{}, err
		}
		if _, exists := result.values[normalized]; exists {
			return Metadata{}, fmt.Errorf("%w: duplicate normalized key %q", ErrInvalidKey, normalized)
		}

		size, err = addSize(size, len(normalized), settings.maxBytes)
		if err != nil {
			return Metadata{}, err
		}
		clonedValues := make([]string, len(sourceValues))
		for index, value := range sourceValues {
			size, err = addSize(size, len(value), settings.maxBytes)
			if err != nil {
				return Metadata{}, err
			}
			clonedValues[index] = value
		}
		result.values[normalized] = clonedValues
	}
	return result, nil
}

// Values returns an independent copy of every value for key.
func (m Metadata) Values(key string) []string {
	normalized, err := normalizeKey(key)
	if err != nil {
		return nil
	}
	values, exists := m.values[normalized]
	if !exists {
		return nil
	}
	return append([]string(nil), values...)
}

// Keys returns normalized keys in lexical order.
func (m Metadata) Keys() []string {
	keys := make([]string, 0, len(m.values))
	for key := range m.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Clone returns a deep immutable copy.
func (m Metadata) Clone() Metadata {
	return Metadata{
		values:    cloneValues(m.values),
		sensitive: cloneSet(m.sensitive),
	}
}

// IsSensitive reports whether key must be redacted in diagnostic output.
func (m Metadata) IsSensitive(key string) bool {
	normalized, err := normalizeKey(key)
	if err != nil {
		return false
	}
	_, exists := m.sensitive[normalized]
	return exists
}

// Redacted returns an independent map with sensitive values replaced.
func (m Metadata) Redacted(replacement string) map[string][]string {
	result := cloneValues(m.values)
	for key := range m.sensitive {
		values, exists := result[key]
		if !exists {
			continue
		}
		for index := range values {
			values[index] = replacement
		}
	}
	return result
}

func normalizeKey(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: key is empty", ErrInvalidKey)
	}
	normalized := strings.ToLower(key)
	for _, character := range normalized {
		valid := character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' ||
			character == '_' ||
			character == '.'
		if !valid {
			return "", fmt.Errorf("%w: %q", ErrInvalidKey, key)
		}
	}
	return normalized, nil
}

func addSize(current, additional, limit int) (int, error) {
	if additional > limit-current {
		return 0, fmt.Errorf("%w: at least %d bytes exceeds %d", ErrTooLarge, current+additional, limit)
	}
	return current + additional, nil
}

func cloneValues(source map[string][]string) map[string][]string {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string][]string, len(source))
	for key, values := range source {
		clone[key] = append([]string(nil), values...)
	}
	return clone
}

func cloneSet(source map[string]struct{}) map[string]struct{} {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string]struct{}, len(source))
	for value := range source {
		clone[value] = struct{}{}
	}
	return clone
}
