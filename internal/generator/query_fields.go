package generator

import (
	"fmt"
	"strings"

	"github.com/keelab/keelith/internal/protowkt"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	maximumHTTPQueryDepth  = 16
	maximumHTTPQueryFields = 256
)

type httpQueryField struct {
	name     string
	field    *protogen.Field
	required bool
}

func httpQueryFields(
	message *protogen.Message,
	excluded [][]protoreflect.FieldDescriptor,
) ([]httpQueryField, error) {
	if message == nil {
		return nil, fmt.Errorf("http query request message is nil")
	}
	result := make([]httpQueryField, 0)
	excludedKeys := make(map[string]struct{}, len(excluded))
	for _, path := range excluded {
		excludedKeys[generatorFieldPathKey(path)] = struct{}{}
	}
	stack := map[protoreflect.FullName]struct{}{
		message.Desc.FullName(): {},
	}
	if err := appendHTTPQueryFields(
		&result,
		message,
		"",
		nil,
		excludedKeys,
		true,
		stack,
		1,
	); err != nil {
		return nil, err
	}
	return result, nil
}

func appendHTTPQueryFields(
	result *[]httpQueryField,
	message *protogen.Message,
	prefix string,
	path []protoreflect.FieldDescriptor,
	excluded map[string]struct{},
	parentsRequired bool,
	stack map[protoreflect.FullName]struct{},
	depth int,
) error {
	if depth > maximumHTTPQueryDepth {
		return fmt.Errorf(
			"HTTP query field %q exceeds maximum nesting depth %d",
			prefix,
			maximumHTTPQueryDepth,
		)
	}
	for _, field := range message.Fields {
		fieldPath := append(
			append([]protoreflect.FieldDescriptor(nil), path...),
			field.Desc,
		)
		if _, skip := excluded[generatorFieldPathKey(fieldPath)]; skip {
			continue
		}
		name := field.Desc.JSONName()
		if prefix != "" {
			name = prefix + "." + name
		}
		if field.Desc.IsMap() {
			return fmt.Errorf(
				"HTTP query field %q has unsupported map type",
				name,
			)
		}
		if field.Message != nil {
			queryKind := protowkt.QueryKindFor(
				string(field.Message.Desc.FullName()),
			)
			if queryKind != protowkt.QueryUnsupported {
				if field.Desc.IsList() {
					return fmt.Errorf(
						"HTTP query field %q has unsupported repeated well-known type",
						name,
					)
				}
				if len(*result) >= maximumHTTPQueryFields {
					return fmt.Errorf(
						"HTTP query exceeds maximum field count %d",
						maximumHTTPQueryFields,
					)
				}
				*result = append(*result, httpQueryField{
					name:     name,
					field:    field,
					required: parentsRequired && fieldRequired(field),
				})
				continue
			}
			if field.Desc.IsList() {
				return fmt.Errorf(
					"HTTP query field %q has unsupported repeated message type",
					name,
				)
			}
			fullName := field.Message.Desc.FullName()
			if _, recursive := stack[fullName]; recursive {
				return fmt.Errorf(
					"HTTP query field %q has recursive message type %s",
					name,
					fullName,
				)
			}
			stack[fullName] = struct{}{}
			err := appendHTTPQueryFields(
				result,
				field.Message,
				name,
				fieldPath,
				excluded,
				parentsRequired && fieldRequired(field),
				stack,
				depth+1,
			)
			delete(stack, fullName)
			if err != nil {
				return err
			}
			continue
		}
		if len(*result) >= maximumHTTPQueryFields {
			return fmt.Errorf(
				"HTTP query exceeds maximum field count %d",
				maximumHTTPQueryFields,
			)
		}
		*result = append(*result, httpQueryField{
			name:     name,
			field:    field,
			required: parentsRequired && fieldRequired(field),
		})
	}
	return nil
}

func generatorFieldPathKey(path []protoreflect.FieldDescriptor) string {
	parts := make([]string, 0, len(path))
	for _, field := range path {
		parts = append(parts, string(field.FullName()))
	}
	return strings.Join(parts, "\x00")
}
