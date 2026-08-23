package http

import (
	"fmt"
	"mime"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	protoHTTPBodyFullName            protoreflect.FullName = "google.api.HttpBody"
	defaultProtoHTTPContentType      string                = "application/octet-stream"
	maximumProtoHTTPContentTypeBytes int                   = 512
)

// MarshalProtoHTTPBody extracts the wire media type and raw payload from a
// google.api.HttpBody response. responseBody may select a nested singular
// HttpBody field from a larger output message.
func MarshalProtoHTTPBody(
	response any,
	responseBody string,
) (string, []byte, error) {
	message, ok := response.(proto.Message)
	if !ok || isNilProto(message) {
		return "", nil, fmt.Errorf(
			"http: response type %T is not a Protobuf message",
			response,
		)
	}
	selected, err := selectProtoHTTPBody(message, responseBody, false)
	if err != nil {
		return "", nil, err
	}
	if selected == nil {
		return defaultProtoHTTPContentType, nil, nil
	}
	contentTypeField, dataField, err := protoHTTPBodyFields(selected.Descriptor())
	if err != nil {
		return "", nil, err
	}
	contentType, err := normalizeProtoHTTPContentType(
		selected.Get(contentTypeField).String(),
	)
	if err != nil {
		return "", nil, err
	}
	payload := append([]byte(nil), selected.Get(dataField).Bytes()...)
	return contentType, payload, nil
}

// UnmarshalProtoHTTPBody assigns one raw HTTP payload to a
// google.api.HttpBody message or selected nested field. Extensions are not
// represented on the HTTP wire and are left untouched.
func UnmarshalProtoHTTPBody(
	payload []byte,
	message proto.Message,
	responseBody string,
	contentType string,
) error {
	if isNilProto(message) {
		return fmt.Errorf("http: Protobuf HttpBody is nil")
	}
	normalized, err := normalizeProtoHTTPContentType(contentType)
	if err != nil {
		return err
	}
	selected, err := selectProtoHTTPBody(message, responseBody, true)
	if err != nil {
		return err
	}
	if selected == nil {
		return fmt.Errorf("http: cannot allocate HttpBody field %q", responseBody)
	}
	contentTypeField, dataField, err := protoHTTPBodyFields(selected.Descriptor())
	if err != nil {
		return err
	}
	selected.Set(contentTypeField, protoreflect.ValueOfString(normalized))
	selected.Set(
		dataField,
		protoreflect.ValueOfBytes(append([]byte(nil), payload...)),
	)
	return nil
}

func selectProtoHTTPBody(
	message proto.Message,
	fieldPath string,
	mutable bool,
) (protoreflect.Message, error) {
	root := message.ProtoReflect()
	if fieldPath == "" {
		if !isProtoHTTPBodyDescriptor(root.Descriptor()) {
			return nil, fmt.Errorf(
				"http: Protobuf message %s is not google.api.HttpBody",
				root.Descriptor().FullName(),
			)
		}
		return root, nil
	}
	path, err := resolveProtoFieldPath(root, fieldPath)
	if err != nil {
		return nil, err
	}
	field := path[len(path)-1]
	if field.IsList() || field.IsMap() ||
		!isProtoHTTPBodyDescriptor(field.Message()) {
		return nil, fmt.Errorf(
			"http: proto field %q is not a singular google.api.HttpBody",
			fieldPath,
		)
	}
	parent, exists := protoPathParent(root, path, mutable)
	if !exists {
		return nil, nil
	}
	if mutable {
		return parent.Mutable(field).Message(), nil
	}
	if !parent.Has(field) {
		return nil, nil
	}
	return parent.Get(field).Message(), nil
}

func isProtoHTTPBodyDescriptor(
	descriptor protoreflect.MessageDescriptor,
) bool {
	return descriptor != nil && descriptor.FullName() == protoHTTPBodyFullName
}

func protoFieldPathUsesHTTPBody(
	path []protoreflect.FieldDescriptor,
) bool {
	if len(path) == 0 {
		return false
	}
	field := path[len(path)-1]
	return !field.IsList() && !field.IsMap() &&
		isProtoHTTPBodyDescriptor(field.Message())
}

func protoHTTPBodyFields(
	descriptor protoreflect.MessageDescriptor,
) (protoreflect.FieldDescriptor, protoreflect.FieldDescriptor, error) {
	if !isProtoHTTPBodyDescriptor(descriptor) {
		return nil, nil, fmt.Errorf(
			"http: Protobuf message %s is not google.api.HttpBody",
			descriptor.FullName(),
		)
	}
	contentType := descriptor.Fields().ByName("content_type")
	data := descriptor.Fields().ByName("data")
	if contentType == nil || contentType.IsList() || contentType.IsMap() ||
		contentType.Kind() != protoreflect.StringKind ||
		data == nil || data.IsList() || data.IsMap() ||
		data.Kind() != protoreflect.BytesKind {
		return nil, nil, fmt.Errorf(
			"http: invalid google.api.HttpBody descriptor %s",
			descriptor.FullName(),
		)
	}
	return contentType, data, nil
}

func normalizeProtoHTTPContentType(value string) (string, error) {
	if value == "" {
		return defaultProtoHTTPContentType, nil
	}
	if len(value) > maximumProtoHTTPContentTypeBytes ||
		strings.ContainsAny(value, "\r\n\x00") ||
		strings.TrimSpace(value) != value {
		return "", fmt.Errorf("http: invalid HttpBody Content-Type %q", value)
	}
	mediaType, parameters, err := mime.ParseMediaType(value)
	if err != nil || mediaType == "" {
		return "", fmt.Errorf("http: invalid HttpBody Content-Type %q", value)
	}
	return mime.FormatMediaType(mediaType, parameters), nil
}
