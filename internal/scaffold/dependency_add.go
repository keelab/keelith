package scaffold

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	protoast "github.com/jhump/protoreflect/desc/protoparse/ast"
)

var dependencyReasonPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]*$`)

// AddDependencyOptions identifies one method-to-service dependency edge.
type AddDependencyOptions struct {
	Project     string
	Package     string
	Service     string
	Method      string
	Target      string
	Transport   string
	Binding     string
	Reason      string
	ProtoImport string
}

// AddDependency adds one idempotent dependency option to an existing method.
//
// The target Proto must be available to Buf through the supplied import. The
// command edits only the consumer source and never vendors or downloads the
// target contract.
func AddDependency(
	ctx context.Context,
	options AddDependencyOptions,
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
	options.Method = strings.TrimSpace(options.Method)
	options.Target = strings.TrimSpace(options.Target)
	options.Transport = strings.ToLower(strings.TrimSpace(options.Transport))
	options.Binding = strings.ToLower(strings.TrimSpace(options.Binding))
	options.Reason = strings.TrimSpace(options.Reason)
	options.ProtoImport = strings.TrimSpace(options.ProtoImport)
	if !protoPackagePattern.MatchString(options.Package) ||
		!servicePattern.MatchString(options.Service) ||
		!servicePattern.MatchString(options.Method) ||
		!validTargetService(options.Target) {
		return AddAPIResult{}, fmt.Errorf(
			"%w: package, service, method, or target is invalid",
			ErrInvalidInput,
		)
	}
	if !supportedDependencyTransport(options.Transport) {
		return AddAPIResult{}, fmt.Errorf(
			"%w: dependency transport must be grpc or http",
			ErrInvalidInput,
		)
	}
	if options.Binding != "remote" &&
		options.Binding != "auto" &&
		options.Binding != "local" {
		return AddAPIResult{}, fmt.Errorf(
			"%w: dependency binding must be remote, auto, or local",
			ErrInvalidInput,
		)
	}
	if !dependencyReasonPattern.MatchString(options.Reason) {
		return AddAPIResult{}, fmt.Errorf(
			"%w: dependency reason must be lower kebab/dot/underscore case",
			ErrInvalidInput,
		)
	}
	if !validProtoImport(options.ProtoImport) {
		return AddAPIResult{}, fmt.Errorf(
			"%w: target proto import is invalid",
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
			"scaffold: read dependency annotations: %w",
			err,
		)
	}
	if !strings.Contains(string(annotations), "dependency = 51004") {
		return AddAPIResult{}, fmt.Errorf(
			"%w: project annotations do not declare dependency options; update the application-owned annotations contract",
			ErrConflict,
		)
	}

	relative := filepath.ToSlash(filepath.Join(
		"api",
		strings.ReplaceAll(options.Package, ".", "/"),
		snakeCase(options.Service)+".proto",
	))
	filePath, err := safeAddOutputPath(project, relative, false)
	if err != nil {
		return AddAPIResult{}, err
	}
	content, err := os.ReadFile(filePath)
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
	updated, changed, err := mergeDependencyOption(content, options)
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
	if err := atomicReplaceAddFile(filePath, updated); err != nil {
		return AddAPIResult{}, fmt.Errorf(
			"scaffold: update %s: %w",
			relative,
			err,
		)
	}
	result.Updated = []string{relative}
	return result, nil
}

type dependencyInsertion struct {
	offset int
	value  string
}

func mergeDependencyOption(
	content []byte,
	options AddDependencyOptions,
) ([]byte, bool, error) {
	file, err := parseProjectProto(content)
	if err != nil {
		return nil, false, err
	}
	packageID := ""
	imports := make(map[string]struct{})
	var importTail *protoast.ImportNode
	var service *protoast.ServiceNode
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *protoast.PackageNode:
			packageID = string(node.Name.AsIdentifier())
		case *protoast.ImportNode:
			imports[node.Name.AsString()] = struct{}{}
			importTail = node
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
		}
	}
	if packageID != options.Package {
		return nil, false, fmt.Errorf(
			"package is %q, want %q",
			packageID,
			options.Package,
		)
	}
	if _, exists := imports["keelith/project/v1/annotations.proto"]; !exists {
		return nil, false, errors.New(
			`required import "keelith/project/v1/annotations.proto" is missing`,
		)
	}
	if service == nil {
		return nil, false, fmt.Errorf("service %s is missing", options.Service)
	}
	var method *protoast.RPCNode
	for _, declaration := range service.Decls {
		rpc, ok := declaration.(*protoast.RPCNode)
		if !ok || rpc.Name.Val != options.Method {
			continue
		}
		if method != nil {
			return nil, false, fmt.Errorf(
				"method %s is declared more than once",
				options.Method,
			)
		}
		method = rpc
	}
	if method == nil {
		return nil, false, fmt.Errorf("method %s is missing", options.Method)
	}
	if method.CloseBrace == nil {
		return nil, false, fmt.Errorf(
			"method %s has no option block",
			options.Method,
		)
	}
	for _, declaration := range method.Decls {
		option, ok := declaration.(*protoast.OptionNode)
		if !ok || dependencyOptionName(option) == "" {
			continue
		}
		dependency, err := parseDependencyOptionNode(option)
		if err != nil {
			return nil, false, err
		}
		if dependency.Service == options.Target &&
			dependency.Transport == options.Transport &&
			dependency.Reason == options.Reason &&
			dependency.Binding == options.Binding {
			return append([]byte(nil), content...), false, nil
		}
	}

	insertions := make([]dependencyInsertion, 0, 2)
	methodOffset, err := protoSourceOffset(content, method.CloseBrace.Start())
	if err != nil {
		return nil, false, fmt.Errorf(
			"method closing brace position: %w",
			err,
		)
	}
	insertions = append(insertions, dependencyInsertion{
		offset: methodOffset,
		value: fmt.Sprintf(
			"    option (keelith.project.v1.dependency) = {\n"+
				"      service: %q\n"+
				"      transport: %q\n"+
				"      binding: SERVICE_DEPENDENCY_BINDING_%s\n"+
				"      reason: %q\n"+
				"    };\n",
			options.Target,
			options.Transport,
			strings.ToUpper(options.Binding),
			options.Reason,
		),
	})
	if _, exists := imports[options.ProtoImport]; !exists {
		if importTail == nil {
			return nil, false, errors.New(
				"source has no import insertion point",
			)
		}
		importOffset, err := protoSourceOffset(content, importTail.End())
		if err != nil {
			return nil, false, fmt.Errorf(
				"import insertion position: %w",
				err,
			)
		}
		insertions = append(insertions, dependencyInsertion{
			offset: importOffset,
			value:  fmt.Sprintf("\nimport %q;", options.ProtoImport),
		})
	}
	sort.Slice(insertions, func(first, second int) bool {
		return insertions[first].offset > insertions[second].offset
	})
	updated := append([]byte(nil), content...)
	for _, insertion := range insertions {
		if insertion.offset < 0 || insertion.offset > len(updated) {
			return nil, false, errors.New("insertion point is outside source")
		}
		next := make([]byte, 0, len(updated)+len(insertion.value))
		next = append(next, updated[:insertion.offset]...)
		next = append(next, insertion.value...)
		next = append(next, updated[insertion.offset:]...)
		updated = next
	}
	if _, err := parseProjectProto(updated); err != nil {
		return nil, false, fmt.Errorf(
			"merged dependency source is invalid: %w",
			err,
		)
	}
	return updated, true, nil
}

func validTargetService(value string) bool {
	index := strings.LastIndexByte(value, '.')
	return index > 0 &&
		protoPackagePattern.MatchString(value[:index]) &&
		servicePattern.MatchString(value[index+1:])
}

func validProtoImport(value string) bool {
	return value != "" &&
		strings.HasSuffix(value, ".proto") &&
		!strings.HasPrefix(value, "/") &&
		path.Clean(value) == value &&
		!strings.HasPrefix(value, "../") &&
		!strings.ContainsAny(value, "\\\r\n")
}

func dependencyOptionName(option *protoast.OptionNode) string {
	if option == nil || len(option.Name.Parts) != 1 {
		return ""
	}
	name := option.Name.Parts[0].Value()
	if name == "(keelith.project.v1.dependency)" {
		return name
	}
	return ""
}

func parseDependencyOptionNode(
	option *protoast.OptionNode,
) (declaredDependencyOption, error) {
	literal, ok := option.Val.(*protoast.MessageLiteralNode)
	if !ok {
		return declaredDependencyOption{}, errors.New(
			"dependency option must use a message literal",
		)
	}
	values := make(map[string]string, len(literal.Elements))
	for _, element := range literal.Elements {
		name := element.Name.Value()
		if name != "service" && name != "transport" &&
			name != "binding" && name != "reason" {
			return declaredDependencyOption{}, fmt.Errorf(
				"dependency option field %q is unknown",
				name,
			)
		}
		var value string
		if name == "binding" {
			identifier, ok := element.Val.(protoast.IdentValueNode)
			if !ok {
				return declaredDependencyOption{}, errors.New(
					"dependency option binding must be an enum",
				)
			}
			value = strings.TrimPrefix(
				string(identifier.AsIdentifier()),
				"SERVICE_DEPENDENCY_BINDING_",
			)
			value = strings.ToLower(value)
		} else {
			text, ok := element.Val.(protoast.StringValueNode)
			if !ok {
				return declaredDependencyOption{}, fmt.Errorf(
					"dependency option field %q must be a string",
					name,
				)
			}
			value = text.AsString()
		}
		if _, duplicate := values[name]; duplicate {
			return declaredDependencyOption{}, fmt.Errorf(
				"dependency option field %q is duplicated",
				name,
			)
		}
		values[name] = value
	}
	dependency := declaredDependencyOption{
		Service:   values["service"],
		Transport: values["transport"],
		Binding:   values["binding"],
		Reason:    values["reason"],
	}
	if !validTargetService(dependency.Service) ||
		!supportedDependencyTransport(dependency.Transport) ||
		(dependency.Binding != "remote" &&
			dependency.Binding != "auto" &&
			dependency.Binding != "local") ||
		!dependencyReasonPattern.MatchString(dependency.Reason) {
		return declaredDependencyOption{}, errors.New(
			"dependency option is malformed",
		)
	}
	return dependency, nil
}

func supportedDependencyTransport(transport string) bool {
	switch transport {
	case "grpc", "http":
		return true
	default:
		return false
	}
}

type declaredDependencyOption struct {
	Service   string
	Transport string
	Binding   string
	Reason    string
}
