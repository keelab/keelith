// Package http provides Keelith's standard-library HTTP transport.
package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/keelab/keelith/internal/protowkt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Decoder converts one HTTP request to a transport-neutral handler request.
type Decoder func(*nethttp.Request) (any, error)

// Encoder writes one transport-neutral handler response.
type Encoder func(context.Context, nethttp.ResponseWriter, any) error

// NoBody is a Decoder for requests without an application body.
func NoBody(*nethttp.Request) (any, error) {
	return nil, nil
}

// DecodeJSON returns a strict JSON request Decoder for T.
func DecodeJSON[T any]() Decoder {
	return func(request *nethttp.Request) (any, error) {
		var value T
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if err := ensureJSONEOF(decoder); err != nil {
			return nil, err
		}
		return value, nil
	}
}

// ProtoDecodeOption configures Protobuf HTTP binding.
type ProtoDecodeOption interface {
	applyProtoDecode(*protoDecodeOptions) error
}

type protoDecodeOptionFunc func(*protoDecodeOptions) error

func (f protoDecodeOptionFunc) applyProtoDecode(
	options *protoDecodeOptions,
) error {
	return f(options)
}

type protoDecodeOptions struct {
	bodyField         string
	allowUnknownQuery bool
	queryDisabled     bool
	pathBindings      []protoPathBinding
	pathTemplate      string
}

// WithProtoBody decodes a strict protojson body before path/query overlays.
func WithProtoBody() ProtoDecodeOption {
	return protoDecodeOptionFunc(func(options *protoDecodeOptions) error {
		options.bodyField = "*"
		return nil
	})
}

// WithProtoBodyField decodes the JSON body into one Protobuf field path.
func WithProtoBodyField(fieldPath string) ProtoDecodeOption {
	return protoDecodeOptionFunc(func(options *protoDecodeOptions) error {
		if !validProtoFieldPath(fieldPath) || fieldPath == "*" {
			return fmt.Errorf("http: invalid proto body field %q", fieldPath)
		}
		options.bodyField = fieldPath
		return nil
	})
}

// WithUnknownProtoQuery allows query keys absent from the input message.
func WithUnknownProtoQuery() ProtoDecodeOption {
	return protoDecodeOptionFunc(func(options *protoDecodeOptions) error {
		options.allowUnknownQuery = true
		return nil
	})
}

// WithProtoQueryDisabled rejects every non-empty URL query. Generated
// google.api.http adapters use it with body:"*", where all non-path request
// fields belong to the JSON body.
func WithProtoQueryDisabled() ProtoDecodeOption {
	return protoDecodeOptionFunc(func(options *protoDecodeOptions) error {
		options.queryDisabled = true
		return nil
	})
}

// WithProtoPathField binds one Protobuf scalar field path from a router path
// value name. Generated adapters use an internal-safe value name for nested
// google.api.http fields.
func WithProtoPathField(fieldPath string, valueName string) ProtoDecodeOption {
	return protoDecodeOptionFunc(func(options *protoDecodeOptions) error {
		if !validProtoFieldPath(fieldPath) ||
			!validProtoPathValueName(valueName) {
			return fmt.Errorf(
				"http: invalid proto path binding %q=%q",
				fieldPath,
				valueName,
			)
		}
		if len(options.pathBindings) >= maximumProtoFieldPathSegments {
			return fmt.Errorf("http: too many proto path bindings")
		}
		options.pathBindings = append(options.pathBindings, protoPathBinding{
			fieldPath: fieldPath,
			valueName: valueName,
		})
		return nil
	})
}

// WithProtoPathTemplate binds every google.api.http path variable from the
// request's original escaped path. It supports assigned resource-name
// patterns and preserves encoded slashes captured by a terminal "**".
func WithProtoPathTemplate(pathTemplate string) ProtoDecodeOption {
	return protoDecodeOptionFunc(func(options *protoDecodeOptions) error {
		if err := validateProtoPathTemplate(pathTemplate); err != nil {
			return err
		}
		options.pathTemplate = pathTemplate
		return nil
	})
}

// DecodeProto binds protojson body, path values, and query parameters to one
// Protobuf request. Path/query values overlay body values.
func DecodeProto[T proto.Message](
	factory func() T,
	optionList ...ProtoDecodeOption,
) Decoder {
	return func(request *nethttp.Request) (any, error) {
		if request == nil || factory == nil {
			return nil, fmt.Errorf("http: proto request or factory is nil")
		}
		options := protoDecodeOptions{}
		for index, option := range optionList {
			if option == nil {
				return nil, fmt.Errorf("http: proto decode option %d is nil", index)
			}
			if err := option.applyProtoDecode(&options); err != nil {
				return nil, fmt.Errorf(
					"http: proto decode option %d: %w",
					index,
					err,
				)
			}
		}
		if options.pathTemplate != "" && len(options.pathBindings) > 0 {
			return nil, fmt.Errorf(
				"http: proto path template and field bindings conflict",
			)
		}
		message := factory()
		if isNilProto(message) {
			return nil, fmt.Errorf("http: proto factory returned nil")
		}
		reflection := message.ProtoReflect()
		var bodyPath []protoreflect.FieldDescriptor
		if options.bodyField != "" {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			rawHTTPBody := false
			if options.bodyField != "*" {
				bodyPath, err = resolveProtoFieldPath(
					reflection,
					options.bodyField,
				)
				if err != nil {
					return nil, err
				}
				if protoFieldPathUsesHTTPBody(bodyPath) {
					rawHTTPBody = true
				} else {
					payload, err = wrapProtoBodyField(payload, bodyPath)
					if err != nil {
						return nil, err
					}
				}
			} else if isProtoHTTPBodyDescriptor(reflection.Descriptor()) {
				rawHTTPBody = true
			}
			if rawHTTPBody {
				if err := UnmarshalProtoHTTPBody(
					payload,
					message,
					strings.TrimPrefix(options.bodyField, "*"),
					request.Header.Get("Content-Type"),
				); err != nil {
					return nil, err
				}
			} else if len(bytes.TrimSpace(payload)) > 0 {
				if err := (protojson.UnmarshalOptions{
					DiscardUnknown: false,
				}).Unmarshal(payload, message); err != nil {
					return nil, err
				}
			}
		}
		var pathFields [][]protoreflect.FieldDescriptor
		var pathErr error
		if options.pathTemplate != "" {
			pathFields, pathErr = bindProtoPathTemplate(
				request,
				reflection,
				options.pathTemplate,
			)
		} else {
			pathFields, pathErr = bindPathValues(
				request,
				reflection,
				options.pathBindings,
			)
		}
		if pathErr != nil {
			return nil, pathErr
		}
		if len(bodyPath) > 0 {
			for _, pathField := range pathFields {
				if protoFieldPathsOverlap(pathField, bodyPath) {
					return nil, fmt.Errorf(
						"http: proto body field %q overlaps request path field %q",
						options.bodyField,
						pathField[len(pathField)-1].Name(),
					)
				}
			}
		}
		if options.queryDisabled && request.URL.RawQuery != "" {
			return nil, fmt.Errorf("http: proto query is disabled for this route")
		}
		query, err := url.ParseQuery(request.URL.RawQuery)
		if err != nil {
			return nil, fmt.Errorf("http: invalid query: %w", err)
		}
		if err := bindQuery(
			reflection,
			query,
			options.allowUnknownQuery,
			bodyPath,
			pathFields,
		); err != nil {
			return nil, err
		}
		return message, nil
	}
}

// EncodeJSON serializes response as application/json.
func EncodeJSON(
	_ context.Context,
	writer nethttp.ResponseWriter,
	response any,
) error {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	return json.NewEncoder(writer).Encode(response)
}

// EncodeProto serializes a Protobuf response with canonical protojson names.
func EncodeProto(
	_ context.Context,
	writer nethttp.ResponseWriter,
	response any,
) error {
	payload, err := MarshalProtoResponseBody(response, "")
	if err != nil {
		return err
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	payload = append(payload, '\n')
	_, err = writer.Write(payload)
	return err
}

// EncodeProtoResponseBody returns an Encoder that projects one
// google.api.HttpRule.response_body field path.
func EncodeProtoResponseBody(responseBody string) Encoder {
	return func(
		_ context.Context,
		writer nethttp.ResponseWriter,
		response any,
	) error {
		payload, err := MarshalProtoResponseBody(response, responseBody)
		if err != nil {
			return err
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		payload = append(payload, '\n')
		_, err = writer.Write(payload)
		return err
	}
}

// EncodeProtoHTTPBody returns an Encoder that writes google.api.HttpBody data
// as the raw HTTP entity rather than ProtoJSON. responseBody may select one
// nested singular HttpBody field.
func EncodeProtoHTTPBody(responseBody string) Encoder {
	return func(
		_ context.Context,
		writer nethttp.ResponseWriter,
		response any,
	) error {
		contentType, payload, err := MarshalProtoHTTPBody(response, responseBody)
		if err != nil {
			return err
		}
		writer.Header().Set("Content-Type", contentType)
		_, err = writer.Write(payload)
		return err
	}
}

// EncodeText serializes string or byte responses as text/plain.
func EncodeText(
	_ context.Context,
	writer nethttp.ResponseWriter,
	response any,
) error {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	switch value := response.(type) {
	case string:
		_, err := io.WriteString(writer, value)
		return err
	case []byte:
		_, err := writer.Write(value)
		return err
	default:
		_, err := fmt.Fprint(writer, value)
		return err
	}
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("http: request contains multiple json values")
	}
	return err
}

func bindPathValues(
	request *nethttp.Request,
	message protoreflect.Message,
	bindings []protoPathBinding,
) ([][]protoreflect.FieldDescriptor, error) {
	if len(bindings) > 0 {
		return bindDeclaredPathValues(request, message, bindings)
	}
	result := make([][]protoreflect.FieldDescriptor, 0)
	fields := message.Descriptor().Fields()
	for index := range fields.Len() {
		field := fields.Get(index)
		value := request.PathValue(string(field.Name()))
		if value == "" && field.JSONName() != string(field.Name()) {
			value = request.PathValue(field.JSONName())
		}
		if value == "" {
			continue
		}
		if field.IsList() || field.IsMap() {
			return nil, fmt.Errorf(
				"http: path field %q must be scalar",
				field.Name(),
			)
		}
		parsed, err := parseProtoScalar(message, field, value)
		if err != nil {
			return nil, fmt.Errorf("http: path field %q: %w", field.Name(), err)
		}
		if err := setProtoBoundField(message, field, parsed, "path"); err != nil {
			return nil, fmt.Errorf("http: path field %q: %w", field.Name(), err)
		}
		result = append(result, []protoreflect.FieldDescriptor{field})
	}
	return result, nil
}

func bindDeclaredPathValues(
	request *nethttp.Request,
	message protoreflect.Message,
	bindings []protoPathBinding,
) ([][]protoreflect.FieldDescriptor, error) {
	result := make([][]protoreflect.FieldDescriptor, 0, len(bindings))
	seenFields := make(map[string]struct{}, len(bindings))
	seenValues := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		path, err := resolveProtoPathField(message, binding.fieldPath)
		if err != nil {
			return nil, err
		}
		fieldKey := protoFieldPathKey(path)
		if _, duplicate := seenFields[fieldKey]; duplicate {
			return nil, fmt.Errorf(
				"http: duplicate proto path field %q",
				binding.fieldPath,
			)
		}
		if _, duplicate := seenValues[binding.valueName]; duplicate {
			return nil, fmt.Errorf(
				"http: duplicate router path value %q",
				binding.valueName,
			)
		}
		seenFields[fieldKey] = struct{}{}
		seenValues[binding.valueName] = struct{}{}
		value := request.PathValue(binding.valueName)
		if value == "" {
			continue
		}
		field := path[len(path)-1]
		parent, _ := protoPathParent(message, path, true)
		parsed, err := parseProtoScalar(parent, field, value)
		if err != nil {
			return nil, fmt.Errorf(
				"http: path field %q: %w",
				binding.fieldPath,
				err,
			)
		}
		if err := setProtoBoundField(parent, field, parsed, "path"); err != nil {
			return nil, fmt.Errorf(
				"http: path field %q: %w",
				binding.fieldPath,
				err,
			)
		}
		result = append(result, path)
	}
	return result, nil
}

func bindQuery(
	message protoreflect.Message,
	query url.Values,
	allowUnknown bool,
	bodyPath []protoreflect.FieldDescriptor,
	pathFields [][]protoreflect.FieldDescriptor,
) error {
	if len(query) > maxProtoQueryValues {
		return fmt.Errorf(
			"http: query exceeds maximum field count %d",
			maxProtoQueryValues,
		)
	}
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	valueCount := 0
	for _, key := range keys {
		values := query[key]
		valueCount += len(values)
		if valueCount > maxProtoQueryValues {
			return fmt.Errorf(
				"http: query exceeds maximum value count %d",
				maxProtoQueryValues,
			)
		}
		fieldPath, known, err := resolveProtoQueryPath(
			message.Descriptor(),
			key,
		)
		if err != nil {
			if protoFieldPathsOverlap(fieldPath, bodyPath) {
				return fmt.Errorf(
					"http: query field %q is already bound by the request body",
					key,
				)
			}
			return err
		}
		if !known {
			if allowUnknown {
				continue
			}
			return fmt.Errorf("http: unknown query field %q", key)
		}
		if protoFieldPathsOverlap(fieldPath, bodyPath) {
			return fmt.Errorf(
				"http: query field %q is already bound by the request body",
				key,
			)
		}
		for _, pathField := range pathFields {
			if sameProtoFieldPath(fieldPath, pathField) {
				return fmt.Errorf(
					"http: query field %q is already bound by the request path",
					key,
				)
			}
		}
		parent := mutableProtoQueryParent(message, fieldPath)
		field := fieldPath[len(fieldPath)-1]
		if field.IsList() {
			list := parent.Mutable(field).List()
			list.Truncate(0)
			for _, raw := range values {
				parsed, err := parseProtoScalar(parent, field, raw)
				if err != nil {
					return fmt.Errorf("http: query field %q: %w", key, err)
				}
				list.Append(parsed)
			}
			continue
		}
		if len(values) != 1 {
			return fmt.Errorf("http: query field %q occurs multiple times", key)
		}
		parsed, err := parseProtoScalar(parent, field, values[0])
		if err != nil {
			return fmt.Errorf("http: query field %q: %w", key, err)
		}
		if err := setProtoBoundField(parent, field, parsed, "query"); err != nil {
			return fmt.Errorf("http: query field %q: %w", key, err)
		}
	}
	return nil
}

func parseProtoScalar(
	parent protoreflect.Message,
	field protoreflect.FieldDescriptor,
	raw string,
) (protoreflect.Value, error) {
	if field.Message() != nil {
		kind := protowkt.QueryKindFor(string(field.Message().FullName()))
		if kind == protowkt.QueryUnsupported {
			return protoreflect.Value{}, fmt.Errorf(
				"field kind %s is unsupported",
				field.Kind(),
			)
		}
		payload, err := protowkt.QueryToJSON(kind, raw)
		if err != nil {
			return protoreflect.Value{}, err
		}
		if parent == nil || !parent.IsValid() {
			return protoreflect.Value{}, fmt.Errorf("message parent is invalid")
		}
		message := parent.NewField(field).Message()
		if err := (protojson.UnmarshalOptions{
			DiscardUnknown: false,
		}).Unmarshal(payload, message.Interface()); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(message), nil
	}
	switch field.Kind() {
	case protoreflect.BoolKind:
		value, err := strconv.ParseBool(raw)
		return protoreflect.ValueOfBool(value), err
	case protoreflect.EnumKind:
		if value := field.Enum().Values().ByName(protoreflect.Name(raw)); value != nil {
			return protoreflect.ValueOfEnum(value.Number()), nil
		}
		number, err := strconv.ParseInt(raw, 10, 32)
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(number)), err
	case protoreflect.Int32Kind, protoreflect.Sint32Kind,
		protoreflect.Sfixed32Kind:
		value, err := strconv.ParseInt(raw, 10, 32)
		return protoreflect.ValueOfInt32(int32(value)), err
	case protoreflect.Int64Kind, protoreflect.Sint64Kind,
		protoreflect.Sfixed64Kind:
		value, err := strconv.ParseInt(raw, 10, 64)
		return protoreflect.ValueOfInt64(value), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		value, err := strconv.ParseUint(raw, 10, 32)
		return protoreflect.ValueOfUint32(uint32(value)), err
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		value, err := strconv.ParseUint(raw, 10, 64)
		return protoreflect.ValueOfUint64(value), err
	case protoreflect.FloatKind:
		value, err := strconv.ParseFloat(raw, 32)
		return protoreflect.ValueOfFloat32(float32(value)), err
	case protoreflect.DoubleKind:
		value, err := strconv.ParseFloat(raw, 64)
		return protoreflect.ValueOfFloat64(value), err
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(raw), nil
	case protoreflect.BytesKind:
		value, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			value, err = base64.RawStdEncoding.DecodeString(raw)
		}
		return protoreflect.ValueOfBytes(value), err
	default:
		return protoreflect.Value{}, fmt.Errorf(
			"field kind %s is unsupported",
			field.Kind(),
		)
	}
}

func isNilProto(message proto.Message) bool {
	if message == nil {
		return true
	}
	reflection := message.ProtoReflect()
	return !reflection.IsValid()
}
