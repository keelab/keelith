package cache

import (
	"encoding/json"

	"github.com/pelletier/go-toml/v2"
	"gopkg.in/yaml.v3"
)

// Codec maps application values to backend payloads.
type Codec[T any] interface {
	// Encode serializes value into a backend payload.
	Encode(T) ([]byte, error)
	// Decode deserializes a backend payload into value.
	Decode([]byte) (T, error)
}

// TOMLCodec encodes cache values with github.com/pelletier/go-toml/v2.
type TOMLCodec[T any] struct{}

// Encode serializes value as TOML.
func (TOMLCodec[T]) Encode(value T) ([]byte, error) {
	return toml.Marshal(value)
}

// Decode deserializes a TOML value.
func (TOMLCodec[T]) Decode(payload []byte) (T, error) {
	var value T
	err := toml.Unmarshal(payload, &value)
	return value, err
}

// JSONCodec encodes cache values with encoding/json.
type JSONCodec[T any] struct{}

// Encode serializes value as JSON.
func (JSONCodec[T]) Encode(value T) ([]byte, error) {
	return json.Marshal(value)
}

// Decode deserializes a JSON value.
func (JSONCodec[T]) Decode(payload []byte) (T, error) {
	var value T
	err := json.Unmarshal(payload, &value)
	return value, err
}

// YAMLCodec encodes cache values with gopkg.in/yaml.v3.
type YAMLCodec[T any] struct{}

// Encode serializes value as YAML.
func (YAMLCodec[T]) Encode(value T) ([]byte, error) {
	return yaml.Marshal(value)
}

// Decode deserializes a YAML value.
func (YAMLCodec[T]) Decode(payload []byte) (T, error) {
	var value T
	err := yaml.Unmarshal(payload, &value)
	return value, err
}
