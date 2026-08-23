package scaffold

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	protoast "github.com/jhump/protoreflect/desc/protoparse/ast"
)

var (
	errorEnumPattern   = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	errorReasonPattern = regexp.MustCompile(
		`^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$`,
	)
)

// AddErrorOptions identifies one stable Proto enum error declaration.
type AddErrorOptions struct {
	Project string
	Package string
	Service string
	Enum    string
	Reason  string
	Number  int32
	Code    int32
}

// AddError creates or safely merges one top-level declared error enum value.
//
// The enum number is explicit because it is a persistent wire identity. The
// HTTP code is constrained to the same range accepted by the Keelith
// generator. Existing declarations are never renumbered or overwritten.
func AddError(
	ctx context.Context,
	options AddErrorOptions,
) (AddAPIResult, error) {
	if ctx == nil {
		return AddAPIResult{}, fmt.Errorf("%w: context is nil", ErrInvalidInput)
	}
	if cause := context.Cause(ctx); cause != nil {
		return AddAPIResult{}, cause
	}
	project, err := filepath.Abs(strings.TrimSpace(options.Project))
	if err != nil {
		return AddAPIResult{}, fmt.Errorf(
			"%w: project path: %w",
			ErrInvalidInput,
			err,
		)
	}
	project, err = filepath.EvalSymlinks(project)
	if err != nil {
		return AddAPIResult{}, fmt.Errorf(
			"%w: resolve project symlinks: %w",
			ErrInvalidInput,
			err,
		)
	}
	options.Package = strings.TrimSpace(options.Package)
	options.Service = strings.TrimSpace(options.Service)
	options.Enum = strings.TrimSpace(options.Enum)
	options.Reason = strings.TrimSpace(options.Reason)
	if !protoPackagePattern.MatchString(options.Package) ||
		!servicePattern.MatchString(options.Service) ||
		!errorEnumPattern.MatchString(options.Enum) {
		return AddAPIResult{}, fmt.Errorf(
			"%w: package, service, or enum is invalid",
			ErrInvalidInput,
		)
	}
	prefix := strings.ToUpper(snakeCase(options.Enum)) + "_"
	if !errorReasonPattern.MatchString(options.Reason) ||
		!strings.HasPrefix(options.Reason, prefix) ||
		options.Reason == prefix+"UNSPECIFIED" {
		return AddAPIResult{}, fmt.Errorf(
			"%w: reason must use the %s namespace",
			ErrInvalidInput,
			prefix,
		)
	}
	if options.Number <= 0 {
		return AddAPIResult{}, fmt.Errorf(
			"%w: enum number must be positive",
			ErrInvalidInput,
		)
	}
	if options.Code < 400 || options.Code > 599 {
		return AddAPIResult{}, fmt.Errorf(
			"%w: error code must be between 400 and 599",
			ErrInvalidInput,
		)
	}
	if info, statErr := os.Stat(project); statErr != nil || !info.IsDir() {
		return AddAPIResult{}, fmt.Errorf(
			"%w: project directory is unavailable",
			ErrInvalidInput,
		)
	}
	annotationsPath, err := safeAddOutputPath(
		project,
		"api/keelith/project/v1/annotations.proto",
		false,
	)
	if err != nil {
		return AddAPIResult{}, err
	}
	annotations, err := os.ReadFile(annotationsPath)
	if err != nil {
		return AddAPIResult{}, fmt.Errorf(
			"scaffold: read error annotations: %w",
			err,
		)
	}
	if !strings.Contains(string(annotations), "int32 error_code = 51005;") {
		return AddAPIResult{}, fmt.Errorf(
			"%w: project annotations do not declare error options; update the application-owned annotations contract",
			ErrConflict,
		)
	}
	relative := filepath.ToSlash(filepath.Join(
		"api",
		strings.ReplaceAll(options.Package, ".", "/"),
		snakeCase(options.Service)+".proto",
	))
	contractPath, err := safeAddOutputPath(project, relative, false)
	if err != nil {
		return AddAPIResult{}, err
	}
	content, err := os.ReadFile(contractPath)
	if err != nil {
		return AddAPIResult{}, fmt.Errorf(
			"scaffold: read %s: %w",
			relative,
			err,
		)
	}
	if len(content) == 0 || len(content) > maxAddProtoBytes {
		return AddAPIResult{}, fmt.Errorf(
			"%w: %s is empty or exceeds %d bytes",
			ErrConflict,
			relative,
			maxAddProtoBytes,
		)
	}
	updated, changed, err := mergeErrorDeclaration(content, options)
	if err != nil {
		return AddAPIResult{}, fmt.Errorf(
			"%w: %s: %w",
			ErrConflict,
			relative,
			err,
		)
	}
	result := AddAPIResult{Project: project}
	if !changed {
		result.Unchanged = []string{relative}
		return result, nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return AddAPIResult{}, cause
	}
	if err := atomicReplaceAddFile(contractPath, updated); err != nil {
		return AddAPIResult{}, fmt.Errorf(
			"scaffold: update %s: %w",
			relative,
			err,
		)
	}
	result.Updated = []string{relative}
	return result, nil
}

func mergeErrorDeclaration(
	content []byte,
	options AddErrorOptions,
) ([]byte, bool, error) {
	file, err := parseProjectProto(content)
	if err != nil {
		return nil, false, err
	}
	packageID := ""
	hasAnnotations := false
	var service *protoast.ServiceNode
	var target *protoast.EnumNode
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *protoast.PackageNode:
			packageID = string(node.Name.AsIdentifier())
		case *protoast.ImportNode:
			if node.Name.AsString() == "keelith/project/v1/annotations.proto" {
				hasAnnotations = true
			}
		case *protoast.ServiceNode:
			if node.Name.Val == options.Service {
				if service != nil {
					return nil, false, fmt.Errorf(
						"service %s is declared more than once",
						options.Service,
					)
				}
				service = node
			}
		case *protoast.EnumNode:
			if node.Name.Val == options.Enum {
				if target != nil {
					return nil, false, fmt.Errorf(
						"enum %s is declared more than once",
						options.Enum,
					)
				}
				target = node
			}
		}
	}
	if packageID != options.Package {
		return nil, false, fmt.Errorf(
			"package is %q, want %q",
			packageID,
			options.Package,
		)
	}
	if !hasAnnotations {
		return nil, false, errors.New(
			`required import "keelith/project/v1/annotations.proto" is missing`,
		)
	}
	if service == nil {
		return nil, false, fmt.Errorf("service %s is missing", options.Service)
	}
	if target == nil {
		return insertErrorEnum(content, service, options)
	}
	return insertErrorValue(content, target, options)
}

func insertErrorEnum(
	content []byte,
	service *protoast.ServiceNode,
	options AddErrorOptions,
) ([]byte, bool, error) {
	offset, err := protoSourceOffset(content, service.Start())
	if err != nil {
		return nil, false, fmt.Errorf("service position: %w", err)
	}
	prefix := strings.ToUpper(snakeCase(options.Enum))
	declaration := fmt.Sprintf(
		"enum %s {\n"+
			"  %s_UNSPECIFIED = 0;\n"+
			"  %s = %d [(keelith.project.v1.error_code) = %d];\n"+
			"}\n\n",
		options.Enum,
		prefix,
		options.Reason,
		options.Number,
		options.Code,
	)
	if offset > 0 && content[offset-1] != '\n' {
		declaration = "\n" + declaration
	}
	updated := make([]byte, 0, len(content)+len(declaration))
	updated = append(updated, content[:offset]...)
	updated = append(updated, declaration...)
	updated = append(updated, content[offset:]...)
	if _, err := parseProjectProto(updated); err != nil {
		return nil, false, fmt.Errorf("merged source is invalid: %w", err)
	}
	return updated, true, nil
}

func insertErrorValue(
	content []byte,
	target *protoast.EnumNode,
	options AddErrorOptions,
) ([]byte, bool, error) {
	for _, declaration := range target.Decls {
		value, ok := declaration.(*protoast.EnumValueNode)
		if !ok {
			continue
		}
		number, ok := value.Number.AsInt64()
		if !ok {
			return nil, false, fmt.Errorf(
				"enum value %s number is outside int64",
				value.Name.Val,
			)
		}
		code, hasCode, err := errorCodeFromEnumValue(value)
		if err != nil {
			return nil, false, err
		}
		if value.Name.Val == options.Reason {
			if number == int64(options.Number) &&
				hasCode &&
				code == options.Code {
				return append([]byte(nil), content...), false, nil
			}
			return nil, false, fmt.Errorf(
				"reason %s already exists with different semantics",
				options.Reason,
			)
		}
		if number == int64(options.Number) {
			return nil, false, fmt.Errorf(
				"enum number %d is already used by %s",
				options.Number,
				value.Name.Val,
			)
		}
	}
	offset, err := protoSourceOffset(content, target.CloseBrace.Start())
	if err != nil || offset >= len(content) || content[offset] != '}' {
		return nil, false, errors.New("enum closing brace has invalid position")
	}
	declaration := fmt.Sprintf(
		"  %s = %d [(keelith.project.v1.error_code) = %d];\n",
		options.Reason,
		options.Number,
		options.Code,
	)
	if offset > 0 && content[offset-1] != '\n' {
		declaration = "\n" + declaration
	}
	updated := make([]byte, 0, len(content)+len(declaration))
	updated = append(updated, content[:offset]...)
	updated = append(updated, declaration...)
	updated = append(updated, content[offset:]...)
	if _, err := parseProjectProto(updated); err != nil {
		return nil, false, fmt.Errorf("merged source is invalid: %w", err)
	}
	return updated, true, nil
}

func errorCodeFromEnumValue(
	value *protoast.EnumValueNode,
) (int32, bool, error) {
	if value == nil || value.Options == nil {
		return 0, false, nil
	}
	found := false
	var result int32
	for _, option := range value.Options.Options {
		if option == nil ||
			len(option.Name.Parts) != 1 ||
			option.Name.Parts[0].Value() !=
				"(keelith.project.v1.error_code)" {
			continue
		}
		if found {
			return 0, false, fmt.Errorf(
				"enum value %s repeats error_code",
				value.Name.Val,
			)
		}
		integer, ok := option.Val.(protoast.IntValueNode)
		if !ok {
			return 0, false, fmt.Errorf(
				"enum value %s error_code is not an integer",
				value.Name.Val,
			)
		}
		code, ok := integer.AsInt64()
		if !ok || code < 400 || code > 599 {
			return 0, false, fmt.Errorf(
				"enum value %s error_code is outside 400..599",
				value.Name.Val,
			)
		}
		found = true
		result = int32(code)
	}
	return result, found, nil
}
