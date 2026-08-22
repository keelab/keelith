package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

const snapshotEncodingVersion = 1

type encodedSnapshot struct {
	Version  int                  `json:"version"`
	Revision string               `json:"revision"`
	Values   map[string]wireValue `json:"values"`
}

type wireValue struct {
	Kind   string               `json:"kind"`
	Scalar string               `json:"scalar,omitempty"`
	Array  []wireValue          `json:"array,omitempty"`
	Object map[string]wireValue `json:"object,omitempty"`
}

// MarshalSnapshot encodes an immutable Snapshot without losing scalar types.
//
// The format is versioned for durable last-good configuration caches. It is
// not intended as a human-authored configuration format.
func MarshalSnapshot(snapshot Snapshot) ([]byte, error) {
	if err := snapshot.validate(); err != nil {
		return nil, err
	}
	values := make(map[string]wireValue, len(snapshot.values))
	for key, value := range snapshot.values {
		encoded, err := encodeValue(value)
		if err != nil {
			return nil, fmt.Errorf("config: encode key %q: %w", key, err)
		}
		values[key] = encoded
	}
	payload, err := json.Marshal(encodedSnapshot{
		Version:  snapshotEncodingVersion,
		Revision: snapshot.revision,
		Values:   values,
	})
	if err != nil {
		return nil, fmt.Errorf("config: encode snapshot: %w", err)
	}
	return payload, nil
}

// UnmarshalSnapshot decodes the durable versioned Snapshot representation.
func UnmarshalSnapshot(payload []byte) (Snapshot, error) {
	var encoded encodedSnapshot
	if err := json.Unmarshal(payload, &encoded); err != nil {
		return Snapshot{}, fmt.Errorf("%w: decode snapshot: %w", ErrInvalidSnapshot, err)
	}
	if encoded.Version != snapshotEncodingVersion {
		return Snapshot{}, fmt.Errorf("%w: unsupported encoding version %d", ErrInvalidSnapshot, encoded.Version)
	}
	values := make(map[string]any, len(encoded.Values))
	for key, value := range encoded.Values {
		decoded, err := decodeValue(value)
		if err != nil {
			return Snapshot{}, fmt.Errorf("%w: decode key %q: %w", ErrInvalidSnapshot, key, err)
		}
		values[key] = decoded
	}
	return NewSnapshot(encoded.Revision, values)
}

func encodeValue(value any) (wireValue, error) {
	if value == nil {
		return wireValue{Kind: "nil"}, nil
	}
	if _, ok := value.(deleteValue); ok {
		return wireValue{Kind: "delete"}, nil
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Bool:
		return wireValue{
			Kind:   "bool",
			Scalar: strconv.FormatBool(reflected.Bool()),
		}, nil
	case reflect.String:
		return wireValue{
			Kind:   "string",
			Scalar: reflected.String(),
		}, nil
	case reflect.Int:
		return integerValue("int", reflected.Int()), nil
	case reflect.Int8:
		return integerValue("int8", reflected.Int()), nil
	case reflect.Int16:
		return integerValue("int16", reflected.Int()), nil
	case reflect.Int32:
		return integerValue("int32", reflected.Int()), nil
	case reflect.Int64:
		return integerValue("int64", reflected.Int()), nil
	case reflect.Uint:
		return unsignedValue("uint", reflected.Uint()), nil
	case reflect.Uint8:
		return unsignedValue("uint8", reflected.Uint()), nil
	case reflect.Uint16:
		return unsignedValue("uint16", reflected.Uint()), nil
	case reflect.Uint32:
		return unsignedValue("uint32", reflected.Uint()), nil
	case reflect.Uint64:
		return unsignedValue("uint64", reflected.Uint()), nil
	case reflect.Float32:
		return wireValue{
			Kind:   "float32",
			Scalar: strconv.FormatFloat(reflected.Float(), 'g', -1, 32),
		}, nil
	case reflect.Float64:
		return wireValue{
			Kind:   "float64",
			Scalar: strconv.FormatFloat(reflected.Float(), 'g', -1, 64),
		}, nil
	case reflect.Array, reflect.Slice:
		result := make([]wireValue, reflected.Len())
		for index := range reflected.Len() {
			encoded, err := encodeValue(reflected.Index(index).Interface())
			if err != nil {
				return wireValue{}, err
			}
			result[index] = encoded
		}
		return wireValue{Kind: "array", Array: result}, nil
	case reflect.Map:
		if reflected.Type().Key().Kind() != reflect.String {
			return wireValue{}, fmt.Errorf("map key type %s is not string", reflected.Type().Key())
		}
		result := make(map[string]wireValue, reflected.Len())
		iterator := reflected.MapRange()
		for iterator.Next() {
			encoded, err := encodeValue(iterator.Value().Interface())
			if err != nil {
				return wireValue{}, err
			}
			result[iterator.Key().String()] = encoded
		}
		return wireValue{Kind: "object", Object: result}, nil
	default:
		return wireValue{}, fmt.Errorf("unsupported value type %T", value)
	}
}

func decodeValue(value wireValue) (any, error) {
	switch value.Kind {
	case "nil":
		return nil, nil
	case "delete":
		return Delete(), nil
	case "bool":
		return strconv.ParseBool(value.Scalar)
	case "string":
		return value.Scalar, nil
	case "int":
		parsed, err := strconv.ParseInt(value.Scalar, 10, 0)
		return int(parsed), err
	case "int8":
		parsed, err := strconv.ParseInt(value.Scalar, 10, 8)
		return int8(parsed), err
	case "int16":
		parsed, err := strconv.ParseInt(value.Scalar, 10, 16)
		return int16(parsed), err
	case "int32":
		parsed, err := strconv.ParseInt(value.Scalar, 10, 32)
		return int32(parsed), err
	case "int64":
		return strconv.ParseInt(value.Scalar, 10, 64)
	case "uint":
		parsed, err := strconv.ParseUint(value.Scalar, 10, 0)
		return uint(parsed), err
	case "uint8":
		parsed, err := strconv.ParseUint(value.Scalar, 10, 8)
		return uint8(parsed), err
	case "uint16":
		parsed, err := strconv.ParseUint(value.Scalar, 10, 16)
		return uint16(parsed), err
	case "uint32":
		parsed, err := strconv.ParseUint(value.Scalar, 10, 32)
		return uint32(parsed), err
	case "uint64":
		return strconv.ParseUint(value.Scalar, 10, 64)
	case "float32":
		parsed, err := strconv.ParseFloat(value.Scalar, 32)
		return float32(parsed), err
	case "float64":
		return strconv.ParseFloat(value.Scalar, 64)
	case "array":
		result := make([]any, len(value.Array))
		for index, item := range value.Array {
			decoded, err := decodeValue(item)
			if err != nil {
				return nil, err
			}
			result[index] = decoded
		}
		return result, nil
	case "object":
		result := make(map[string]any, len(value.Object))
		for key, item := range value.Object {
			decoded, err := decodeValue(item)
			if err != nil {
				return nil, err
			}
			result[key] = decoded
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unknown value kind %q", value.Kind)
	}
}

func integerValue(kind string, value int64) wireValue {
	return wireValue{Kind: kind, Scalar: strconv.FormatInt(value, 10)}
}

func unsignedValue(kind string, value uint64) wireValue {
	return wireValue{Kind: kind, Scalar: strconv.FormatUint(value, 10)}
}
