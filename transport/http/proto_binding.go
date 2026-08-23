package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	maximumProtoFieldPathBytes    = 1024
	maximumProtoFieldPathSegments = 16
)

func resolveProtoFieldPath(
	message protoreflect.Message,
	path string,
) ([]protoreflect.FieldDescriptor, error) {
	if !message.IsValid() || !validProtoFieldPath(path) {
		return nil, fmt.Errorf("http: invalid proto field path %q", path)
	}
	segments := strings.Split(path, ".")
	result := make([]protoreflect.FieldDescriptor, 0, len(segments))
	descriptor := message.Descriptor()
	for index, segment := range segments {
		field := lookupProtoField(descriptor.Fields(), segment)
		if field == nil {
			return nil, fmt.Errorf(
				"http: proto body field %q is absent from %s",
				strings.Join(segments[:index+1], "."),
				descriptor.FullName(),
			)
		}
		result = append(result, field)
		if index == len(segments)-1 {
			continue
		}
		if field.IsList() || field.IsMap() || field.Message() == nil {
			return nil, fmt.Errorf(
				"http: proto body field %q is not a singular message",
				strings.Join(segments[:index+1], "."),
			)
		}
		descriptor = field.Message()
	}
	return result, nil
}

func validProtoFieldPath(path string) bool {
	if path == "" || len(path) > maximumProtoFieldPathBytes ||
		!utf8.ValidString(path) || strings.TrimSpace(path) != path {
		return false
	}
	segments := strings.Split(path, ".")
	if len(segments) > maximumProtoFieldPathSegments {
		return false
	}
	for _, segment := range segments {
		if segment == "" {
			return false
		}
		for index, character := range segment {
			if unicode.IsControl(character) ||
				index == 0 && !isProtoFieldStart(character) ||
				index > 0 && !isProtoFieldContinue(character) {
				return false
			}
		}
	}
	return true
}

func isProtoFieldStart(character rune) bool {
	return character == '_' ||
		character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z'
}

func isProtoFieldContinue(character rune) bool {
	return isProtoFieldStart(character) ||
		character >= '0' && character <= '9'
}

func lookupProtoField(
	fields protoreflect.FieldDescriptors,
	name string,
) protoreflect.FieldDescriptor {
	if field := fields.ByName(protoreflect.Name(name)); field != nil {
		return field
	}
	for index := range fields.Len() {
		field := fields.Get(index)
		if field.JSONName() == name {
			return field
		}
	}
	return nil
}

func wrapProtoBodyField(
	payload []byte,
	path []protoreflect.FieldDescriptor,
) ([]byte, error) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return nil, nil
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("http: proto body field contains invalid JSON")
	}
	result := append([]byte(nil), payload...)
	for index := len(path) - 1; index >= 0; index-- {
		key, err := json.Marshal(path[index].JSONName())
		if err != nil {
			return nil, fmt.Errorf("http: encode proto body field: %w", err)
		}
		wrapped := make([]byte, 0, len(key)+len(result)+3)
		wrapped = append(wrapped, '{')
		wrapped = append(wrapped, key...)
		wrapped = append(wrapped, ':')
		wrapped = append(wrapped, result...)
		wrapped = append(wrapped, '}')
		result = wrapped
	}
	return result, nil
}

func marshalProtoBodyField(
	message protoreflect.Message,
	path []protoreflect.FieldDescriptor,
) ([]byte, error) {
	payload, err := (protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: true,
	}).Marshal(message.Interface())
	if err != nil {
		return nil, fmt.Errorf("http: encode proto body field: %w", err)
	}
	current := json.RawMessage(payload)
	for _, field := range path {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(current, &object); err != nil {
			return nil, fmt.Errorf("http: select proto body field: %w", err)
		}
		selected, exists := object[field.JSONName()]
		if !exists {
			return []byte("null"), nil
		}
		current = selected
	}
	return append([]byte(nil), current...), nil
}

func setProtoBoundField(
	parent protoreflect.Message,
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
	source string,
) error {
	if parent == nil || !parent.IsValid() || field == nil {
		return fmt.Errorf("http: invalid proto %s field binding", source)
	}
	oneof := field.ContainingOneof()
	if oneof != nil && !oneof.IsSynthetic() {
		if current := parent.WhichOneof(oneof); current != nil &&
			current.Number() != field.Number() {
			return fmt.Errorf(
				"http: proto %s field %q conflicts with active oneof %q member %q",
				source,
				field.Name(),
				oneof.Name(),
				current.Name(),
			)
		}
	}
	parent.Set(field, value)
	return nil
}
