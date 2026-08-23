// Package resource defines the single service identity shared by telemetry.
package resource

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"unicode"

	"github.com/keelab/keelith/service"
	"go.opentelemetry.io/otel/attribute"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// ErrInvalid reports an invalid telemetry resource identity.
var ErrInvalid = errors.New("observability resource: invalid identity")

// Config defines stable service-level telemetry identity.
type Config struct {
	ServiceName string
	Version     string
	Instance    string
	Environment string
	Region      string
	Zone        string
	Attributes  map[string]string
}

// Resource is immutable and projects the same identity to slog and OTel.
type Resource struct {
	values     map[string]string
	attributes []attribute.KeyValue
	otel       *sdkresource.Resource
}

// New validates and constructs a Resource.
func New(config Config) (*Resource, error) {
	if !validRequired(config.ServiceName) {
		return nil, fmt.Errorf("%w: service name is required", ErrInvalid)
	}
	standard := map[string]string{
		"service.name": strings.TrimSpace(config.ServiceName),
	}
	optional := []struct {
		key   string
		value string
	}{
		{key: "service.version", value: config.Version},
		{key: "service.instance.id", value: config.Instance},
		{key: "deployment.environment.name", value: config.Environment},
		{key: "cloud.region", value: config.Region},
		{key: "cloud.availability_zone", value: config.Zone},
	}
	for _, candidate := range optional {
		if candidate.value == "" {
			continue
		}
		if !validOptional(candidate.value) {
			return nil, fmt.Errorf("%w: %s is malformed", ErrInvalid, candidate.key)
		}
		standard[candidate.key] = strings.TrimSpace(candidate.value)
	}
	for key, value := range config.Attributes {
		normalizedKey := strings.TrimSpace(key)
		if normalizedKey == "" || !validOptional(value) {
			return nil, fmt.Errorf("%w: custom attribute %q", ErrInvalid, key)
		}
		if _, reserved := standard[normalizedKey]; reserved ||
			isReserved(normalizedKey) {
			return nil, fmt.Errorf("%w: reserved attribute %q", ErrInvalid, key)
		}
		standard[normalizedKey] = strings.TrimSpace(value)
	}

	keys := make([]string, 0, len(standard))
	for key := range standard {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	attributes := make([]attribute.KeyValue, 0, len(keys))
	for _, key := range keys {
		attributes = append(attributes, attribute.String(key, standard[key]))
	}
	return &Resource{
		values:     standard,
		attributes: attributes,
		otel: sdkresource.NewWithAttributes(
			semconv.SchemaURL,
			attributes...,
		),
	}, nil
}

// FromIdentity projects one service.Identity into the common telemetry
// resource without maintaining a second identity model in application code.
func FromIdentity(identity service.Identity) (*Resource, error) {
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return New(Config{
		ServiceName: identity.Name(),
		Version:     identity.Version(),
		Instance:    identity.ID(),
		Environment: identity.Environment(),
		Region:      identity.Region(),
		Zone:        identity.Zone(),
		Attributes:  identity.Metadata(),
	})
}

// Values returns an independent key/value snapshot.
func (r *Resource) Values() map[string]string {
	if r == nil {
		return nil
	}
	result := make(map[string]string, len(r.values))
	for key, value := range r.values {
		result[key] = value
	}
	return result
}

// Attributes returns independent OTel attributes.
func (r *Resource) Attributes() []attribute.KeyValue {
	if r == nil {
		return nil
	}
	return append([]attribute.KeyValue(nil), r.attributes...)
}

// SlogAttributes returns the same values as slog attributes.
func (r *Resource) SlogAttributes() []slog.Attr {
	if r == nil {
		return nil
	}
	result := make([]slog.Attr, 0, len(r.attributes))
	for _, value := range r.attributes {
		result = append(
			result,
			slog.String(string(value.Key), value.Value.AsString()),
		)
	}
	return result
}

// OTel returns the immutable SDK Resource.
func (r *Resource) OTel() *sdkresource.Resource {
	if r == nil {
		return sdkresource.Empty()
	}
	return r.otel
}

func validRequired(value string) bool {
	return value != "" && validOptional(value)
}

func validOptional(value string) bool {
	if strings.TrimSpace(value) != value || value == "" {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func isReserved(key string) bool {
	switch key {
	case "service.name", "service.version", "service.instance.id",
		"deployment.environment.name", "cloud.region",
		"cloud.availability_zone":
		return true
	default:
		return false
	}
}
