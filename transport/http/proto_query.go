package http

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/keelab/keelith/internal/protowkt"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	maxProtoQueryDepth  = 16
	maxProtoQueryValues = 256
)

func resolveProtoQueryPath(
	descriptor protoreflect.MessageDescriptor,
	path string,
) ([]protoreflect.FieldDescriptor, bool, error) {
	if !validProtoFieldPath(path) {
		return nil, false, fmt.Errorf("http: invalid query field path %q", path)
	}
	segments := strings.Split(path, ".")
	fields := make([]protoreflect.FieldDescriptor, 0, len(segments))
	current := descriptor
	for index, segment := range segments {
		field := lookupProtoField(current.Fields(), segment)
		if field == nil {
			return nil, false, nil
		}
		fields = append(fields, field)
		last := index == len(segments)-1
		if last {
			queryKind := protowkt.QueryUnsupported
			if field.Message() != nil {
				queryKind = protowkt.QueryKindFor(
					string(field.Message().FullName()),
				)
			}
			if field.IsMap() || field.IsList() && field.Message() != nil ||
				field.Message() != nil && queryKind == protowkt.QueryUnsupported {
				return fields, true, fmt.Errorf(
					"http: query field %q has unsupported message/map type",
					path,
				)
			}
			continue
		}
		if field.IsList() || field.IsMap() || field.Message() == nil ||
			protowkt.QueryKindFor(string(field.Message().FullName())) !=
				protowkt.QueryUnsupported {
			return fields, true, fmt.Errorf(
				"http: query field %q has unsupported message/map type",
				path,
			)
		}
		current = field.Message()
	}
	return fields, true, nil
}

func mutableProtoQueryParent(
	message protoreflect.Message,
	fields []protoreflect.FieldDescriptor,
) protoreflect.Message {
	current := message
	for _, field := range fields[:len(fields)-1] {
		current = current.Mutable(field).Message()
	}
	return current
}

func appendProtoQuery(
	message protoreflect.Message,
	prefix string,
	query url.Values,
	values *int,
	stack map[protoreflect.FullName]struct{},
	depth int,
) error {
	if depth > maxProtoQueryDepth {
		return fmt.Errorf(
			"%w: query field %q exceeds maximum nesting depth %d",
			ErrInvalidCall,
			prefix,
			maxProtoQueryDepth,
		)
	}
	fields := message.Descriptor().Fields()
	for index := range fields.Len() {
		field := fields.Get(index)
		name := field.JSONName()
		if prefix != "" {
			name = prefix + "." + name
		}
		if field.IsMap() {
			if message.Get(field).Map().Len() > 0 {
				return unsupportedProtoQueryShape(name)
			}
			continue
		}
		if field.Message() != nil {
			queryKind := protowkt.QueryKindFor(
				string(field.Message().FullName()),
			)
			if queryKind != protowkt.QueryUnsupported {
				if field.IsList() {
					if message.Get(field).List().Len() > 0 {
						return unsupportedProtoQueryShape(name)
					}
					continue
				}
				if !message.Has(field) {
					continue
				}
				if err := reserveProtoQueryValue(values, name); err != nil {
					return err
				}
				value, err := formatProtoScalar(field, message.Get(field))
				if err != nil {
					return fmt.Errorf(
						"%w: query field %q: %w",
						ErrInvalidCall,
						name,
						err,
					)
				}
				query.Set(name, value)
				continue
			}
			if field.IsList() {
				if message.Get(field).List().Len() > 0 {
					return unsupportedProtoQueryShape(name)
				}
				continue
			}
			if !message.Has(field) {
				continue
			}
			fullName := field.Message().FullName()
			if _, recursive := stack[fullName]; recursive {
				return fmt.Errorf(
					"%w: query field %q has recursive message type %s",
					ErrInvalidCall,
					name,
					fullName,
				)
			}
			stack[fullName] = struct{}{}
			err := appendProtoQuery(
				message.Get(field).Message(),
				name,
				query,
				values,
				stack,
				depth+1,
			)
			delete(stack, fullName)
			if err != nil {
				return err
			}
			continue
		}
		if field.IsList() {
			list := message.Get(field).List()
			for item := range list.Len() {
				if err := reserveProtoQueryValue(values, name); err != nil {
					return err
				}
				value, err := formatProtoScalar(field, list.Get(item))
				if err != nil {
					return fmt.Errorf(
						"%w: query field %q: %w",
						ErrInvalidCall,
						name,
						err,
					)
				}
				query.Add(name, value)
			}
			continue
		}
		if !message.Has(field) {
			continue
		}
		if err := reserveProtoQueryValue(values, name); err != nil {
			return err
		}
		value, err := formatProtoScalar(field, message.Get(field))
		if err != nil {
			return fmt.Errorf(
				"%w: query field %q: %w",
				ErrInvalidCall,
				name,
				err,
			)
		}
		query.Set(name, value)
	}
	return nil
}

func reserveProtoQueryValue(values *int, field string) error {
	*values++
	if *values > maxProtoQueryValues {
		return fmt.Errorf(
			"%w: query field %q exceeds maximum value count %d",
			ErrInvalidCall,
			field,
			maxProtoQueryValues,
		)
	}
	return nil
}

func unsupportedProtoQueryShape(field string) error {
	return fmt.Errorf(
		"%w: query field %q has unsupported message/map type",
		ErrInvalidCall,
		field,
	)
}
