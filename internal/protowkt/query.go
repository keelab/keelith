// Package protowkt centralizes Protobuf well-known-type wire projections.
package protowkt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

// QueryKind describes the JSON scalar shape used by one well-known type.
type QueryKind uint8

const (
	// QueryUnsupported marks an ordinary or complex Protobuf message.
	QueryUnsupported QueryKind = iota
	// QueryString uses a JSON string and an unquoted URL Query value.
	QueryString
	// QueryBool uses a JSON boolean.
	QueryBool
	// QueryNumber uses a JSON number or a quoted non-finite float token.
	QueryNumber
	// QueryIntegerString uses the Protobuf JSON quoted int64/uint64 form.
	QueryIntegerString
)

// QueryKindFor returns the supported scalar Query shape for a full message
// name. Complex well-known types deliberately remain unsupported.
func QueryKindFor(fullName string) QueryKind {
	switch fullName {
	case "google.protobuf.Timestamp",
		"google.protobuf.Duration",
		"google.protobuf.FieldMask",
		"google.protobuf.StringValue",
		"google.protobuf.BytesValue":
		return QueryString
	case "google.protobuf.BoolValue":
		return QueryBool
	case "google.protobuf.DoubleValue",
		"google.protobuf.FloatValue",
		"google.protobuf.Int32Value",
		"google.protobuf.UInt32Value":
		return QueryNumber
	case "google.protobuf.Int64Value",
		"google.protobuf.UInt64Value":
		return QueryIntegerString
	default:
		return QueryUnsupported
	}
}

// JSONToQuery removes the Protobuf JSON scalar wrapper from one value.
func JSONToQuery(kind QueryKind, payload []byte) (string, error) {
	payload = bytes.TrimSpace(payload)
	switch kind {
	case QueryString, QueryIntegerString:
		var value string
		if err := json.Unmarshal(payload, &value); err != nil {
			return "", fmt.Errorf("proto wkt query: decode json string: %w", err)
		}
		if kind == QueryIntegerString && !validInteger(value) {
			return "", fmt.Errorf("proto WKT query: invalid integer %q", value)
		}
		return value, nil
	case QueryBool:
		var value bool
		if err := json.Unmarshal(payload, &value); err != nil {
			return "", fmt.Errorf("proto wkt query: decode json bool: %w", err)
		}
		return strconv.FormatBool(value), nil
	case QueryNumber:
		value, err := decodeJSONNumber(payload)
		if err != nil {
			return "", err
		}
		return value, nil
	default:
		return "", fmt.Errorf("proto WKT query: unsupported scalar kind %d", kind)
	}
}

// QueryToJSON creates the strict Protobuf JSON scalar token for one Query
// value. The caller still delegates semantic range and WKT validation to
// protojson.
func QueryToJSON(kind QueryKind, value string) ([]byte, error) {
	switch kind {
	case QueryString:
		return json.Marshal(value)
	case QueryIntegerString:
		if !validInteger(value) {
			return nil, fmt.Errorf("proto WKT query: invalid integer %q", value)
		}
		return json.Marshal(value)
	case QueryBool:
		if value != "true" && value != "false" {
			return nil, fmt.Errorf("proto WKT query: invalid bool %q", value)
		}
		return []byte(value), nil
	case QueryNumber:
		if value == "NaN" || value == "Infinity" || value == "-Infinity" {
			return json.Marshal(value)
		}
		if _, err := decodeJSONNumber([]byte(value)); err != nil {
			return nil, err
		}
		return []byte(value), nil
	default:
		return nil, fmt.Errorf("proto WKT query: unsupported scalar kind %d", kind)
	}
}

func decodeJSONNumber(payload []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", fmt.Errorf("proto wkt query: decode json number: %w", err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return "", err
	}
	switch typed := value.(type) {
	case json.Number:
		return typed.String(), nil
	case string:
		if typed == "NaN" || typed == "Infinity" || typed == "-Infinity" {
			return typed, nil
		}
	}
	return "", fmt.Errorf("proto wkt query: json value is not a number")
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return fmt.Errorf("proto wkt query: json value has trailing content")
	} else if err != io.EOF {
		return fmt.Errorf("proto wkt query: decode trailing json: %w", err)
	}
	return nil
}

func validInteger(value string) bool {
	if value == "" {
		return false
	}
	if value[0] == '-' {
		value = value[1:]
		if value == "" {
			return false
		}
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
