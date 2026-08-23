package http

import (
	"fmt"
	nethttp "net/http"
	"strings"

	"github.com/keelab/keelith/internal/httptemplate"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type protoPathBinding struct {
	fieldPath string
	valueName string
}

func resolveProtoPathField(
	message protoreflect.Message,
	fieldPath string,
) ([]protoreflect.FieldDescriptor, error) {
	if !message.IsValid() || !validProtoFieldPath(fieldPath) {
		return nil, fmt.Errorf("http: invalid proto path field %q", fieldPath)
	}
	segments := strings.Split(fieldPath, ".")
	result := make([]protoreflect.FieldDescriptor, 0, len(segments))
	descriptor := message.Descriptor()
	for index, segment := range segments {
		field := lookupProtoField(descriptor.Fields(), segment)
		if field == nil {
			return nil, fmt.Errorf(
				"http: proto path field %q is absent from %s",
				strings.Join(segments[:index+1], "."),
				descriptor.FullName(),
			)
		}
		result = append(result, field)
		last := index == len(segments)-1
		if last {
			if field.IsList() || field.IsMap() || field.Message() != nil {
				return nil, fmt.Errorf(
					"http: proto path field %q must be scalar",
					fieldPath,
				)
			}
			continue
		}
		if field.IsList() || field.IsMap() || field.Message() == nil {
			return nil, fmt.Errorf(
				"http: proto path field %q is not a singular message",
				strings.Join(segments[:index+1], "."),
			)
		}
		descriptor = field.Message()
	}
	return result, nil
}

func protoPathParent(
	message protoreflect.Message,
	path []protoreflect.FieldDescriptor,
	mutable bool,
) (protoreflect.Message, bool) {
	current := message
	for _, field := range path[:len(path)-1] {
		if !mutable && !current.Has(field) {
			return nil, false
		}
		if mutable {
			current = current.Mutable(field).Message()
		} else {
			current = current.Get(field).Message()
		}
	}
	return current, true
}

func clearProtoFieldPath(
	message protoreflect.Message,
	path []protoreflect.FieldDescriptor,
) {
	if len(path) == 0 {
		return
	}
	parent, exists := protoPathParent(message, path, false)
	if !exists {
		return
	}
	parent.Clear(path[len(path)-1])
}

func sameProtoFieldPath(
	left []protoreflect.FieldDescriptor,
	right []protoreflect.FieldDescriptor,
) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].FullName() != right[index].FullName() {
			return false
		}
	}
	return true
}

func protoFieldPathsOverlap(
	left []protoreflect.FieldDescriptor,
	right []protoreflect.FieldDescriptor,
) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for index := 0; index < limit; index++ {
		if left[index].FullName() != right[index].FullName() {
			return false
		}
	}
	return true
}

func protoFieldPathKey(path []protoreflect.FieldDescriptor) string {
	parts := make([]string, 0, len(path))
	for _, field := range path {
		parts = append(parts, string(field.FullName()))
	}
	return strings.Join(parts, "\x00")
}

func validProtoPathValueName(value string) bool {
	return validProtoFieldPath(value) && !strings.Contains(value, ".")
}

func validateProtoPathTemplate(raw string) error {
	if _, err := httptemplate.Parse(raw); err != nil {
		return fmt.Errorf("http: invalid proto path template: %w", err)
	}
	return nil
}

func bindProtoPathTemplate(
	request *nethttp.Request,
	message protoreflect.Message,
	raw string,
) ([][]protoreflect.FieldDescriptor, error) {
	template, err := httptemplate.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("http: invalid proto path template: %w", err)
	}
	if request == nil || request.URL == nil {
		return nil, fmt.Errorf("http: request path is unavailable")
	}
	values, err := template.Match(request.URL.EscapedPath())
	if err != nil {
		return nil, fmt.Errorf("http: proto path template: %w", err)
	}
	variables := template.Variables()
	result := make([][]protoreflect.FieldDescriptor, 0, len(variables))
	for _, variable := range variables {
		path, resolveErr := resolveProtoPathField(message, variable.FieldPath)
		if resolveErr != nil {
			return nil, resolveErr
		}
		field := path[len(path)-1]
		if variable.RequiresString() && field.Kind() != protoreflect.StringKind {
			return nil, fmt.Errorf(
				"http: assigned proto path field %q must be string",
				variable.FieldPath,
			)
		}
		parent, _ := protoPathParent(message, path, true)
		parsed, parseErr := parseProtoScalar(
			parent,
			field,
			values[variable.FieldPath],
		)
		if parseErr != nil {
			return nil, fmt.Errorf(
				"http: path field %q: %w",
				variable.FieldPath,
				parseErr,
			)
		}
		if err := setProtoBoundField(parent, field, parsed, "path"); err != nil {
			return nil, fmt.Errorf(
				"http: path field %q: %w",
				variable.FieldPath,
				err,
			)
		}
		result = append(result, path)
	}
	return result, nil
}
