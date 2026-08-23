package generator

import (
	"fmt"
	"strings"

	"github.com/keelab/keelith/internal/httptemplate"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type httpPathCapture struct {
	valueName   string
	openAPIName string
	multi       bool
}

type httpPathBinding struct {
	fieldPath      string
	explicit       bool
	requiresString bool
	captures       []httpPathCapture
	fields         []protoreflect.FieldDescriptor
}

func httpPathBindings(
	message *protogen.Message,
	path string,
) ([]httpPathBinding, error) {
	specs, err := httpPathBindingSpecs(path)
	if err != nil {
		return nil, err
	}
	for index := range specs {
		fields, fieldErr := httpPathFieldPath(message, specs[index].fieldPath)
		if fieldErr != nil {
			return nil, fieldErr
		}
		leaf := fields[len(fields)-1]
		if specs[index].requiresString &&
			leaf.Kind() != protoreflect.StringKind {
			return nil, fmt.Errorf(
				"assigned HTTP path field %q must be string",
				specs[index].fieldPath,
			)
		}
		specs[index].fields = fields
	}
	return specs, nil
}

func protoDecoderExpression(
	output *protogen.GeneratedFile,
	packagePath protogen.GoImportPath,
	input *protogen.Message,
	rule httpRule,
) (string, []httpPathBinding, error) {
	bindings, err := httpPathBindings(input, rule.Path)
	if err != nil {
		return "", nil, err
	}
	decoder := output.QualifiedGoIdent(
		packagePath.Ident("DecodeProto"),
	) + "(func() *" + output.QualifiedGoIdent(input.GoIdent) +
		" { return new(" + output.QualifiedGoIdent(input.GoIdent) + ") }"
	if rule.Body == "*" {
		decoder += ", " + output.QualifiedGoIdent(
			packagePath.Ident("WithProtoBody"),
		) + "()"
		decoder += ", " + output.QualifiedGoIdent(
			packagePath.Ident("WithProtoQueryDisabled"),
		) + "()"
	} else if rule.Body != "" {
		decoder += ", " + output.QualifiedGoIdent(
			packagePath.Ident("WithProtoBodyField"),
		) + "(" + fmt.Sprintf("%q", rule.Body) + ")"
	}
	useTemplate := false
	for _, binding := range bindings {
		if binding.explicit {
			useTemplate = true
			break
		}
	}
	if httpPathRequiresTemplateDispatch(rule.Path) {
		useTemplate = true
	}
	if useTemplate {
		decoder += ", " + output.QualifiedGoIdent(
			packagePath.Ident("WithProtoPathTemplate"),
		) + "(" + fmt.Sprintf("%q", rule.Path) + ")"
	} else {
		for _, binding := range bindings {
			decoder += ", " + output.QualifiedGoIdent(
				packagePath.Ident("WithProtoPathField"),
			) + "(" + fmt.Sprintf("%q", binding.fieldPath) +
				", " + fmt.Sprintf("%q", binding.captures[0].valueName) + ")"
		}
	}
	decoder += ")"
	return decoder, bindings, nil
}

func httpPathBindingSpecs(path string) ([]httpPathBinding, error) {
	template, err := httptemplate.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("HTTP path %q: %w", path, err)
	}
	variables := template.Variables()
	reservedValues := make(map[string]struct{}, len(variables))
	for _, variable := range variables {
		if !variable.Explicit && !strings.Contains(variable.FieldPath, ".") {
			reservedValues[variable.FieldPath] = struct{}{}
		}
	}
	result := make([]httpPathBinding, 0, len(variables))
	openAPINames := make(map[string]struct{}, len(variables))
	for _, variable := range variables {
		if !variable.Explicit {
			openAPINames[variable.FieldPath] = struct{}{}
		}
	}
	for variableIndex, variable := range variables {
		binding := httpPathBinding{
			fieldPath:      variable.FieldPath,
			explicit:       variable.Explicit,
			requiresString: variable.RequiresString(),
			captures:       make([]httpPathCapture, 0, variable.CaptureCount()),
		}
		captureIndex := 0
		for _, pattern := range variable.Pattern {
			if pattern.Wildcard == httptemplate.NoWildcard {
				continue
			}
			valueName := variable.FieldPath
			if variable.Explicit || strings.Contains(variable.FieldPath, ".") {
				if variable.Explicit {
					valueName = fmt.Sprintf(
						"keelith_path_%d_%d",
						variableIndex,
						captureIndex,
					)
				} else {
					valueName = fmt.Sprintf("keelith_path_%d", variableIndex)
				}
				base := valueName
				for suffix := 1; ; suffix++ {
					if _, collision := reservedValues[valueName]; !collision {
						break
					}
					valueName = fmt.Sprintf("%s_%d", base, suffix)
				}
			}
			reservedValues[valueName] = struct{}{}
			openAPIName := variable.FieldPath
			if variable.Explicit {
				openAPIName = strings.ReplaceAll(variable.FieldPath, ".", "_")
				if variable.CaptureCount() > 1 {
					openAPIName = fmt.Sprintf("%s_%d", openAPIName, captureIndex)
				}
			}
			if variable.Explicit {
				baseOpenAPIName := openAPIName
				for suffix := 1; ; suffix++ {
					if _, collision := openAPINames[openAPIName]; !collision {
						break
					}
					openAPIName = fmt.Sprintf("%s_%d", baseOpenAPIName, suffix)
				}
				openAPINames[openAPIName] = struct{}{}
			}
			binding.captures = append(binding.captures, httpPathCapture{
				valueName:   valueName,
				openAPIName: openAPIName,
				multi:       pattern.Wildcard == httptemplate.MultiWildcard,
			})
			captureIndex++
		}
		result = append(result, binding)
	}
	return result, nil
}

func standardHTTPRoutePath(
	path string,
	bindings []httpPathBinding,
) string {
	template, err := httptemplate.Parse(path)
	if err != nil {
		return path
	}
	route, err := template.Render(func(
		variableIndex int,
		captureIndex int,
		multi bool,
	) string {
		capture := bindings[variableIndex].captures[captureIndex]
		if multi {
			return "{" + capture.valueName + "...}"
		}
		return "{" + capture.valueName + "}"
	})
	if err != nil {
		return path
	}
	return route
}

func hertzHTTPRoutePath(
	path string,
	bindings []httpPathBinding,
) string {
	template, err := httptemplate.Parse(path)
	if err != nil {
		return path
	}
	route, err := template.Render(func(
		variableIndex int,
		captureIndex int,
		multi bool,
	) string {
		capture := bindings[variableIndex].captures[captureIndex]
		if multi {
			return "*" + capture.valueName
		}
		return ":" + capture.valueName
	})
	if err != nil {
		return path
	}
	return route
}

func openAPIHTTPRoutePath(
	path string,
	bindings []httpPathBinding,
) string {
	template, err := httptemplate.Parse(path)
	if err != nil {
		return path
	}
	route, err := template.Render(func(
		variableIndex int,
		captureIndex int,
		_ bool,
	) string {
		return "{" + bindings[variableIndex].captures[captureIndex].openAPIName + "}"
	})
	if err != nil {
		return path
	}
	return route
}

func canonicalHTTPRoutePath(path string) string {
	template, err := httptemplate.Parse(path)
	if err != nil {
		return path
	}
	route, err := template.Render(func(
		_ int,
		_ int,
		multi bool,
	) string {
		if multi {
			return "{...}"
		}
		return "{}"
	})
	if err != nil {
		return path
	}
	return route
}

func standardHTTPRegistration(
	path string,
	bindings []httpPathBinding,
) (string, string) {
	if httpPathRequiresTemplateDispatch(path) {
		return "HandleTemplate", path
	}
	return "Handle", standardHTTPRoutePath(path, bindings)
}

func httpPathRequiresTemplateDispatch(path string) bool {
	template, err := httptemplate.Parse(path)
	return err == nil && template.Verb != "" && template.EndsWithVariable()
}

func validHTTPPathFieldPath(path string) bool {
	if path == "" || len(path) > 1024 || strings.TrimSpace(path) != path {
		return false
	}
	segments := strings.Split(path, ".")
	if len(segments) > 16 {
		return false
	}
	for _, segment := range segments {
		if segment == "" {
			return false
		}
		for index, character := range segment {
			valid := character == '_' ||
				character >= 'a' && character <= 'z' ||
				character >= 'A' && character <= 'Z' ||
				index > 0 && character >= '0' && character <= '9'
			if !valid {
				return false
			}
		}
	}
	return true
}

func httpPathFieldPath(
	message *protogen.Message,
	path string,
) ([]protoreflect.FieldDescriptor, error) {
	if message == nil || !validHTTPPathFieldPath(path) {
		return nil, fmt.Errorf("HTTP path field %q is invalid", path)
	}
	segments := strings.Split(path, ".")
	descriptor := message.Desc
	result := make([]protoreflect.FieldDescriptor, 0, len(segments))
	for index, segment := range segments {
		field := lookupHTTPField(descriptor.Fields(), segment)
		if field == nil {
			return nil, fmt.Errorf(
				"HTTP path field %q is absent from %s",
				strings.Join(segments[:index+1], "."),
				descriptor.FullName(),
			)
		}
		result = append(result, field)
		last := index == len(segments)-1
		if last {
			if field.IsList() || field.IsMap() || field.Message() != nil {
				return nil, fmt.Errorf("HTTP path field %q must be scalar", path)
			}
			continue
		}
		if field.IsList() || field.IsMap() || field.Message() == nil {
			return nil, fmt.Errorf(
				"HTTP path field %q is not a singular message",
				strings.Join(segments[:index+1], "."),
			)
		}
		descriptor = field.Message()
	}
	return result, nil
}
