package http

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ValidateProtoResponseBody validates one optional standard
// google.api.HttpRule response_body field against a concrete output message.
func ValidateProtoResponseBody(message proto.Message, responseBody string) error {
	if isNilProto(message) {
		return fmt.Errorf("http: Protobuf response is nil")
	}
	if responseBody == "" {
		return nil
	}
	_, err := protoResponseBodyPath(message.ProtoReflect(), responseBody)
	return err
}

// MarshalProtoResponseBody encodes a complete Protobuf response or one
// field path selected by google.api.HttpRule.response_body.
func MarshalProtoResponseBody(response any, responseBody string) ([]byte, error) {
	message, ok := response.(proto.Message)
	if !ok || isNilProto(message) {
		return nil, fmt.Errorf(
			"http: response type %T is not a Protobuf message",
			response,
		)
	}
	if responseBody == "" {
		payload, err := (protojson.MarshalOptions{
			UseProtoNames:   false,
			EmitUnpopulated: false,
		}).Marshal(message)
		if err != nil {
			return nil, err
		}
		return payload, nil
	}
	path, err := protoResponseBodyPath(message.ProtoReflect(), responseBody)
	if err != nil {
		return nil, err
	}
	payload, err := marshalProtoBodyField(
		message.ProtoReflect(),
		path,
	)
	if err != nil {
		return nil, fmt.Errorf("http: encode proto response body: %w", err)
	}
	return payload, nil
}

// UnmarshalProtoResponseBody decodes a complete Protobuf response or wraps a
// selected response_body field path back into its output message.
func UnmarshalProtoResponseBody(
	payload []byte,
	message proto.Message,
	responseBody string,
) error {
	if isNilProto(message) {
		return fmt.Errorf("http: Protobuf response is nil")
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return ValidateProtoResponseBody(message, responseBody)
	}
	if responseBody != "" {
		path, err := protoResponseBodyPath(message.ProtoReflect(), responseBody)
		if err != nil {
			return err
		}
		trimmed, err = wrapProtoBodyField(
			trimmed,
			path,
		)
		if err != nil {
			return fmt.Errorf("http: decode proto response body: %w", err)
		}
	}
	if err := (protojson.UnmarshalOptions{
		DiscardUnknown: false,
	}).Unmarshal(trimmed, message); err != nil {
		return err
	}
	return nil
}

func protoResponseBodyPath(
	message protoreflect.Message,
	responseBody string,
) ([]protoreflect.FieldDescriptor, error) {
	path, err := resolveProtoFieldPath(message, responseBody)
	if err != nil {
		return nil, fmt.Errorf(
			"http: invalid proto response body field %q: %w",
			responseBody,
			err,
		)
	}
	return path, nil
}
