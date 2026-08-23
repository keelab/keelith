// Package service defines the immutable identity shared by runtime,
// registration, logs, traces, and metrics.
package service

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"unicode"
)

// ErrInvalidIdentity reports a missing or malformed identity field.
var ErrInvalidIdentity = errors.New("service: invalid identity")

// Spec contains the stable identity assigned to one service instance.
type Spec struct {
	ID          string
	Name        string            // logical service name
	Version     string            // deployed service version
	Environment string            // environment name
	Region      string            // region name
	Zone        string            // zone name
	Metadata    map[string]string // metadata key-value pairs
}

// Identity is an immutable service-instance identity.
type Identity struct {
	id          string
	name        string
	version     string
	environment string
	region      string
	zone        string
	metadata    map[string]string
}

type metaSnapshot struct {
	name  string
	value string
}

// New validates and snapshots a service identity.
func New(spec Spec) (Identity, error) {
	required := []metaSnapshot{
		{name: "id", value: spec.ID},
		{name: "name", value: spec.Name},
	}

	for _, field := range required {
		if !validValue(field.value, true) {
			return Identity{}, fmt.Errorf("%w: %s is required and must be normalized", ErrInvalidIdentity, field.name)
		}
	}
	optional := []metaSnapshot{
		{name: "version", value: spec.Version},
		{name: "environment", value: spec.Environment},
		{name: "region", value: spec.Region},
		{name: "zone", value: spec.Zone},
	}

	for _, field := range optional {
		if field.value != "" && !validValue(field.value, false) {
			return Identity{}, fmt.Errorf("%w: %s is malformed", ErrInvalidIdentity, field.name)
		}
	}

	metadata := make(map[string]string, len(spec.Metadata))
	for key, value := range spec.Metadata {
		if !validMetadataKey(key) || !validValue(value, false) {
			return Identity{}, fmt.Errorf("%w: metadata %q is malformed", ErrInvalidIdentity, key)
		}

		if reservedMetadataKey(key) {
			return Identity{}, fmt.Errorf("%w: metadata key %q is reserved", ErrInvalidIdentity, key)
		}
		metadata[key] = value
	}

	return Identity{
		id:          spec.ID,
		name:        spec.Name,
		version:     spec.Version,
		environment: spec.Environment,
		region:      spec.Region,
		zone:        spec.Zone,
		metadata:    metadata,
	}, nil
}

// ID returns the stable instance identifier.
func (i Identity) ID() string { return i.id }

// Name returns the logical service name.
func (i Identity) Name() string { return i.name }

// Version returns the deployed service version.
func (i Identity) Version() string { return i.version }

// Environment returns the deployment environment.
func (i Identity) Environment() string { return i.environment }

// Region returns the deployment region.
func (i Identity) Region() string { return i.region }

// Zone returns the deployment zone.
func (i Identity) Zone() string { return i.zone }

// Metadata returns an independent metadata snapshot.
func (i Identity) Metadata() map[string]string {
	result := make(map[string]string, len(i.metadata))
	maps.Copy(result, i.metadata)

	return result
}

// Attributes returns the complete identity as stable, sorted key/value pairs.
func (i Identity) Attributes() []Attribute {
	values := i.attributeMap()
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Attribute, 0, len(keys))
	for _, key := range keys {
		result = append(result, Attribute{Key: key, Value: values[key]})
	}

	return result
}

// Attribute is one stable service identity value.
type Attribute struct {
	Key   string
	Value string
}

// Validate verifies that Identity was created by New and remains usable.
func (i Identity) Validate() error {
	_, err := New(Spec{
		ID:          i.id,
		Name:        i.name,
		Version:     i.version,
		Environment: i.environment,
		Region:      i.region,
		Zone:        i.zone,
		Metadata:    i.metadata,
	})
	return err
}

func (i Identity) attributeMap() map[string]string {
	result := i.Metadata()
	result["service.instance.id"] = i.id
	result["service.name"] = i.name
	if i.version != "" {
		result["service.version"] = i.version
	}
	if i.environment != "" {
		result["deployment.environment.name"] = i.environment
	}
	if i.region != "" {
		result["cloud.region"] = i.region
	}
	if i.zone != "" {
		result["cloud.availability_zone"] = i.zone
	}

	return result
}

func validValue(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}

	return true
}

func validMetadataKey(key string) bool {
	if key == "" || strings.TrimSpace(key) != key {
		return false
	}
	for _, r := range key {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func reservedMetadataKey(key string) bool {
	switch key {
	case "service.instance.id",
		"service.name",
		"service.version",
		"deployment.environment.name",
		"cloud.region",
		"cloud.availability_zone":
		return true
	default:
		return false
	}
}
