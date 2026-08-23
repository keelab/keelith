package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	nethttp "net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/keelab/keelith/internal/httptemplate"
	"github.com/keelab/keelith/internal/protowkt"
	"github.com/keelab/keelith/operation"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// NormalizeClientBaseURL validates and normalizes a generated client's base
// URL. A path prefix is allowed; credentials, query, and fragments are not.
func NormalizeClientBaseURL(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	parsed, err := url.Parse(value)
	if err != nil ||
		parsed.Scheme != "http" && parsed.Scheme != "https" ||
		parsed.Host == "" ||
		parsed.User != nil ||
		parsed.Opaque != "" ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return "", fmt.Errorf("%w: unsafe http base url %q", ErrInvalidCall, raw)
	}
	if strings.Contains(parsed.Path, "..") {
		return "", fmt.Errorf("%w: unsafe http base path", ErrInvalidCall)
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	parsed.RawPath = strings.TrimSuffix(parsed.RawPath, "/")
	return parsed.String(), nil
}

// NewProtoRequest creates one generated-client request from an annotated
// Protobuf mapping.
//
// body may be empty, "*", or a Protobuf field path. Path/body fields are
// removed from query parameters. Path fields are also removed from an "*"
// body and overlay the request on the server.
func NewProtoRequest(
	ctx context.Context,
	baseURL string,
	method string,
	pathTemplate string,
	input proto.Message,
	body string,
) (*nethttp.Request, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidCall)
	}
	if isNilProto(input) {
		return nil, fmt.Errorf("%w: proto request is nil", ErrInvalidCall)
	}
	normalizedBase, err := NormalizeClientBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	switch method {
	case nethttp.MethodGet, nethttp.MethodPost, nethttp.MethodPut,
		nethttp.MethodPatch, nethttp.MethodDelete, nethttp.MethodHead,
		nethttp.MethodOptions:
	default:
		return nil, fmt.Errorf(
			"%w: unsupported HTTP method %q",
			ErrInvalidCall,
			method,
		)
	}
	if body != "" && body != "*" && !validProtoFieldPath(body) {
		return nil, fmt.Errorf(
			"%w: unsupported proto body mapping %q",
			ErrInvalidCall,
			body,
		)
	}
	if (method == nethttp.MethodGet || method == nethttp.MethodDelete ||
		method == nethttp.MethodHead) &&
		body != "" {
		return nil, fmt.Errorf(
			"%w: %s request cannot have a body",
			ErrInvalidCall,
			method,
		)
	}

	reflection := input.ProtoReflect()
	rawPath, pathFields, err := expandProtoPath(pathTemplate, reflection)
	if err != nil {
		return nil, err
	}
	target, err := joinProtoClientURL(normalizedBase, rawPath)
	if err != nil {
		return nil, err
	}

	var content io.Reader
	contentType := ""
	queryExclusions := append(
		[][]protoreflect.FieldDescriptor(nil),
		pathFields...,
	)
	if body == "*" {
		message := proto.Clone(input)
		cloned := message.ProtoReflect()
		for _, fieldPath := range pathFields {
			clearProtoFieldPath(cloned, fieldPath)
		}
		var payload []byte
		var marshalErr error
		if isProtoHTTPBodyDescriptor(cloned.Descriptor()) {
			contentType, payload, marshalErr = MarshalProtoHTTPBody(message, "")
		} else {
			payload, marshalErr = (protojson.MarshalOptions{
				UseProtoNames:   false,
				EmitUnpopulated: false,
			}).Marshal(message)
		}
		if marshalErr != nil {
			return nil, fmt.Errorf(
				"%w: encode proto request: %w",
				ErrInvalidCall,
				marshalErr,
			)
		}
		content = bytes.NewReader(payload)
	} else {
		if body != "" {
			bodyPath, bodyErr := resolveProtoFieldPath(reflection, body)
			if bodyErr != nil {
				return nil, bodyErr
			}
			for _, pathField := range pathFields {
				if protoFieldPathsOverlap(pathField, bodyPath) {
					return nil, fmt.Errorf(
						"%w: proto body field %q overlaps an HTTP path field",
						ErrInvalidCall,
						body,
					)
				}
			}
			var payload []byte
			var marshalErr error
			if protoFieldPathUsesHTTPBody(bodyPath) {
				contentType, payload, marshalErr = MarshalProtoHTTPBody(input, body)
			} else {
				payload, marshalErr = marshalProtoBodyField(reflection, bodyPath)
			}
			if marshalErr != nil {
				return nil, marshalErr
			}
			content = bytes.NewReader(payload)
			queryExclusions = append(
				queryExclusions,
				bodyPath,
			)
		}
		query, queryErr := protoQuery(reflection, queryExclusions)
		if queryErr != nil {
			return nil, queryErr
		}
		target.RawQuery = query.Encode()
	}

	request, err := nethttp.NewRequestWithContext(
		ctx,
		method,
		target.String(),
		content,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: build proto request: %w", ErrInvalidCall, err)
	}
	request.Header.Set("Accept", "application/json")
	if body != "" {
		if contentType == "" {
			contentType = "application/json"
		}
		request.Header.Set("Content-Type", contentType)
	}
	return request, nil
}

// InvokeProto invokes a generated request and strictly decodes protojson.
func InvokeProto[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *nethttp.Request,
	factory func() T,
) (T, error) {
	return invokeProto(ctx, client, target, request, factory, "")
}

// InvokeProtoResponseBody invokes a generated request and reconstructs one
// google.api.HttpRule.response_body field path into the output message.
func InvokeProtoResponseBody[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *nethttp.Request,
	factory func() T,
	responseBody string,
) (T, error) {
	return invokeProto(ctx, client, target, request, factory, responseBody)
}

// InvokeProtoHTTPBody invokes a generated request and reconstructs a raw
// google.api.HttpBody response or selected nested HttpBody field.
func InvokeProtoHTTPBody[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *nethttp.Request,
	factory func() T,
	responseBody string,
) (T, error) {
	var zero T
	if client == nil || request == nil || factory == nil {
		return zero, fmt.Errorf(
			"%w: proto client, request, or factory is nil",
			ErrInvalidCall,
		)
	}
	request.Header.Set("Accept", "*/*")
	response, err := client.Invoke(ctx, target, ClientCall{
		Request: request,
		Decode: func(
			_ context.Context,
			response *nethttp.Response,
		) (any, error) {
			message := factory()
			if isNilProto(message) {
				return nil, fmt.Errorf(
					"%w: proto response factory returned nil",
					ErrInvalidCall,
				)
			}
			payload, readErr := io.ReadAll(response.Body)
			if readErr != nil {
				return nil, readErr
			}
			if unmarshalErr := UnmarshalProtoHTTPBody(
				payload,
				message,
				responseBody,
				response.Header.Get("Content-Type"),
			); unmarshalErr != nil {
				return nil, unmarshalErr
			}
			return message, nil
		},
	})
	if err != nil {
		return zero, err
	}
	typed, ok := response.(T)
	if !ok || isNilProto(typed) {
		return zero, fmt.Errorf(
			"%w: proto response type %T",
			ErrInvalidCall,
			response,
		)
	}
	return typed, nil
}

func invokeProto[T proto.Message](
	ctx context.Context,
	client *Client,
	target operation.Operation,
	request *nethttp.Request,
	factory func() T,
	responseBody string,
) (T, error) {
	var zero T
	if client == nil || request == nil || factory == nil {
		return zero, fmt.Errorf(
			"%w: proto client, request, or factory is nil",
			ErrInvalidCall,
		)
	}
	response, err := client.Invoke(ctx, target, ClientCall{
		Request: request,
		Decode: func(
			_ context.Context,
			response *nethttp.Response,
		) (any, error) {
			message := factory()
			if isNilProto(message) {
				return nil, fmt.Errorf(
					"%w: proto response factory returned nil",
					ErrInvalidCall,
				)
			}
			payload, readErr := io.ReadAll(response.Body)
			if readErr != nil {
				return nil, readErr
			}
			if unmarshalErr := UnmarshalProtoResponseBody(
				payload,
				message,
				responseBody,
			); unmarshalErr != nil {
				return nil, unmarshalErr
			}
			return message, nil
		},
	})
	if err != nil {
		return zero, err
	}
	typed, ok := response.(T)
	if !ok || isNilProto(typed) {
		return zero, fmt.Errorf(
			"%w: proto response type %T",
			ErrInvalidCall,
			response,
		)
	}
	return typed, nil
}

func expandProtoPath(
	template string,
	message protoreflect.Message,
) (string, [][]protoreflect.FieldDescriptor, error) {
	parsedTemplate, err := httptemplate.Parse(template)
	if err != nil {
		return "", nil, fmt.Errorf(
			"%w: unsafe HTTP path template %q: %w",
			ErrInvalidCall,
			template,
			err,
		)
	}
	selected := make([][]protoreflect.FieldDescriptor, 0)
	values := make(map[string]string)
	for _, variable := range parsedTemplate.Variables() {
		fieldPath, resolveErr := resolveProtoPathField(message, variable.FieldPath)
		if resolveErr != nil {
			return "", nil, fmt.Errorf(
				"%w: path field %q: %w",
				ErrInvalidCall,
				variable.FieldPath,
				resolveErr,
			)
		}
		parent, exists := protoPathParent(message, fieldPath, false)
		field := fieldPath[len(fieldPath)-1]
		if variable.RequiresString() && field.Kind() != protoreflect.StringKind {
			return "", nil, fmt.Errorf(
				"%w: assigned path field %q must be string",
				ErrInvalidCall,
				variable.FieldPath,
			)
		}
		if !exists || !parent.Has(field) {
			return "", nil, fmt.Errorf(
				"%w: path field %q is empty",
				ErrInvalidCall,
				variable.FieldPath,
			)
		}
		formatted, formatErr := formatProtoScalar(field, parent.Get(field))
		if formatErr != nil {
			return "", nil, fmt.Errorf(
				"%w: path field %q: %w",
				ErrInvalidCall,
				variable.FieldPath,
				formatErr,
			)
		}
		values[variable.FieldPath] = formatted
		selected = append(selected, fieldPath)
	}
	result, err := parsedTemplate.Expand(values)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", ErrInvalidCall, err)
	}
	return result, selected, nil
}

func joinProtoClientURL(baseURL, rawPath string) (*url.URL, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: parse base URL: %w", ErrInvalidCall, err)
	}
	combinedRaw := strings.TrimSuffix(base.EscapedPath(), "/") + rawPath
	combinedPath, err := url.PathUnescape(combinedRaw)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid escaped http path", ErrInvalidCall)
	}
	base.Path = combinedPath
	base.RawPath = combinedRaw
	return base, nil
}

func protoQuery(
	message protoreflect.Message,
	excluded [][]protoreflect.FieldDescriptor,
) (url.Values, error) {
	query := make(url.Values)
	projection := proto.Clone(message.Interface()).ProtoReflect()
	for _, fieldPath := range excluded {
		clearProtoFieldPath(projection, fieldPath)
	}
	valueCount := 0
	stack := map[protoreflect.FullName]struct{}{
		message.Descriptor().FullName(): {},
	}
	if err := appendProtoQuery(
		projection,
		"",
		query,
		&valueCount,
		stack,
		1,
	); err != nil {
		return nil, err
	}
	return query, nil
}

func formatProtoScalar(
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
) (string, error) {
	if field.Message() != nil {
		kind := protowkt.QueryKindFor(string(field.Message().FullName()))
		if kind == protowkt.QueryUnsupported {
			return "", fmt.Errorf("field kind %s is unsupported", field.Kind())
		}
		payload, err := (protojson.MarshalOptions{
			UseProtoNames:   false,
			EmitUnpopulated: false,
		}).Marshal(value.Message().Interface())
		if err != nil {
			return "", fmt.Errorf("encode well-known query scalar: %w", err)
		}
		return protowkt.JSONToQuery(kind, payload)
	}
	switch field.Kind() {
	case protoreflect.BoolKind:
		return strconv.FormatBool(value.Bool()), nil
	case protoreflect.EnumKind:
		enum := field.Enum().Values().ByNumber(value.Enum())
		if enum == nil {
			return strconv.FormatInt(int64(value.Enum()), 10), nil
		}
		return string(enum.Name()), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind,
		protoreflect.Sfixed32Kind, protoreflect.Int64Kind,
		protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(value.Int(), 10), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(value.Uint(), 10), nil
	case protoreflect.FloatKind:
		return strconv.FormatFloat(value.Float(), 'g', -1, 32), nil
	case protoreflect.DoubleKind:
		return strconv.FormatFloat(value.Float(), 'g', -1, 64), nil
	case protoreflect.StringKind:
		return value.String(), nil
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(value.Bytes()), nil
	default:
		return "", fmt.Errorf("field kind %s is unsupported", field.Kind())
	}
}
