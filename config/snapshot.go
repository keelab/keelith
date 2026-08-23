package config

import (
	"fmt"
	"reflect"
	"strings"
)

type deleteValue struct{}

// Delete returns the explicit map-key deletion marker.
//
// nil remains a regular configuration value.
func Delete() any {
	return deleteValue{}
}

// Snapshot is an immutable revisioned JSON-like configuration tree.
type Snapshot struct {
	revision string
	values   map[string]any
}

// NewSnapshot validates and defensively copies a source-local Snapshot.
func NewSnapshot(revision string, values map[string]any) (Snapshot, error) {
	if strings.TrimSpace(revision) == "" {
		return Snapshot{}, fmt.Errorf("%w: revision is empty", ErrInvalidSnapshot)
	}
	cloned, err := cloneMap(values, true)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{revision: revision, values: cloned}, nil
}

// Revision returns the source or merged revision.
func (snapshot Snapshot) Revision() string {
	return snapshot.revision
}

// Values returns a deep independent copy of the configuration tree.
func (snapshot Snapshot) Values() map[string]any {
	values, _ := cloneMap(snapshot.values, true)
	return values
}

// Lookup returns a deep independent value at path.
func (snapshot Snapshot) Lookup(path ...string) (any, bool) {
	if len(path) == 0 {
		return snapshot.Values(), true
	}
	var current any = snapshot.values
	for _, segment := range path {
		values, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, exists := values[segment]
		if !exists {
			return nil, false
		}
		current = next
	}
	cloned, err := cloneValue(current, true)
	if err != nil {
		return nil, false
	}
	return cloned, true
}

// Clone returns a deep immutable copy.
func (snapshot Snapshot) Clone() Snapshot {
	values, _ := cloneMap(snapshot.values, true)
	return Snapshot{revision: snapshot.revision, values: values}
}

func (snapshot Snapshot) validate() error {
	if strings.TrimSpace(snapshot.revision) == "" {
		return fmt.Errorf("%w: revision is empty", ErrInvalidSnapshot)
	}
	_, err := cloneMap(snapshot.values, true)
	return err
}

func cloneMap(source map[string]any, allowDelete bool) (map[string]any, error) {
	if len(source) == 0 {
		return map[string]any{}, nil
	}
	clone := make(map[string]any, len(source))
	for key, value := range source {
		if key == "" {
			return nil, fmt.Errorf("%w: map key is empty", ErrInvalidSnapshot)
		}
		cloned, err := cloneValue(value, allowDelete)
		if err != nil {
			return nil, fmt.Errorf("%w: key %q: %w", ErrInvalidSnapshot, key, err)
		}
		clone[key] = cloned
	}
	return clone, nil
}

func cloneValue(value any, allowDelete bool) (any, error) {
	if _, deleting := value.(deleteValue); deleting {
		if !allowDelete {
			return nil, fmt.Errorf("%w: Delete() outside a map merge", ErrInvalidSnapshot)
		}
		return deleteValue{}, nil
	}
	if value == nil {
		return nil, nil
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Float32,
		reflect.Float64,
		reflect.String:
		return value, nil
	case reflect.Map:
		if reflected.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("%w: map key type %s is not string", ErrInvalidSnapshot, reflected.Type().Key())
		}
		result := make(map[string]any, reflected.Len())
		iterator := reflected.MapRange()
		for iterator.Next() {
			key := iterator.Key().String()
			if key == "" {
				return nil, fmt.Errorf("%w: map key is empty", ErrInvalidSnapshot)
			}
			cloned, err := cloneValue(iterator.Value().Interface(), allowDelete)
			if err != nil {
				return nil, err
			}
			result[key] = cloned
		}
		return result, nil
	case reflect.Array, reflect.Slice:
		result := make([]any, reflected.Len())
		for index := range reflected.Len() {
			cloned, err := cloneValue(reflected.Index(index).Interface(), allowDelete)
			if err != nil {
				return nil, err
			}
			result[index] = cloned
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type %T", ErrInvalidSnapshot, value)
	}
}
