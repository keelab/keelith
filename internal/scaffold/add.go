package scaffold

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/jhump/protoreflect/desc/protoparse" //nolint:staticcheck // Source-preserving edits require the v1 AST position API.
	protoast "github.com/jhump/protoreflect/desc/protoparse/ast"
)

var (
	protoPackagePattern = regexp.MustCompile(
		`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`,
	)
	protoFieldPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	pathFieldPattern  = regexp.MustCompile(`\{([^{}]+)\}`)
)

const maxAddProtoBytes = 4 * 1024 * 1024

// AddAPIOptions defines one deterministic API addition.
type AddAPIOptions struct {
	Project    string
	Package    string
	Service    string
	Method     string
	HTTPMethod string
	HTTPPath   string
	Module     string
}

// AddAPIResult describes created or already-identical files.
type AddAPIResult struct {
	Project   string
	Created   []string
	Updated   []string
	Unchanged []string
}

type addMutation struct {
	content []byte
	before  []byte
	create  bool
}

// AddAPI adds or safely merges one method into a Keelith API contract using
// the standard google.api.http mapping.
//
// Existing source is parsed and validated before a structural insertion.
// Existing equivalent methods are idempotent; conflicting declarations and
// malformed or symlinked files are never overwritten.
func AddAPI(
	ctx context.Context,
	options AddAPIOptions,
) (AddAPIResult, error) {
	if ctx == nil {
		return AddAPIResult{}, fmt.Errorf("%w: context is nil", ErrInvalidInput)
	}
	if cause := context.Cause(ctx); cause != nil {
		return AddAPIResult{}, cause
	}
	project, err := filepath.Abs(strings.TrimSpace(options.Project))
	if err != nil {
		return AddAPIResult{}, fmt.Errorf("%w: project path: %w", ErrInvalidInput, err)
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
	options.HTTPMethod = strings.ToUpper(strings.TrimSpace(options.HTTPMethod))
	options.HTTPPath = strings.TrimSpace(options.HTTPPath)
	options.Module = strings.TrimSpace(options.Module)
	if !protoPackagePattern.MatchString(options.Package) {
		return AddAPIResult{}, fmt.Errorf(
			"%w: proto package %q is invalid",
			ErrInvalidInput,
			options.Package,
		)
	}
	if !servicePattern.MatchString(options.Service) ||
		!servicePattern.MatchString(options.Method) {
		return AddAPIResult{}, fmt.Errorf(
			"%w: service or method is not an identifier",
			ErrInvalidInput,
		)
	}
	switch options.HTTPMethod {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
	default:
		return AddAPIResult{}, fmt.Errorf(
			"%w: HTTP method %q is unsupported",
			ErrInvalidInput,
			options.HTTPMethod,
		)
	}
	if !strings.HasPrefix(options.HTTPPath, "/") ||
		strings.ContainsAny(options.HTTPPath, "\r\n") {
		return AddAPIResult{}, fmt.Errorf(
			"%w: HTTP path %q is invalid",
			ErrInvalidInput,
			options.HTTPPath,
		)
	}
	if err := validateModule(options.Module); err != nil {
		return AddAPIResult{}, err
	}
	if info, err := os.Stat(project); err != nil || !info.IsDir() {
		return AddAPIResult{}, fmt.Errorf(
			"%w: project directory is unavailable",
			ErrInvalidInput,
		)
	}

	fields, err := pathFields(options.HTTPPath)
	if err != nil {
		return AddAPIResult{}, err
	}
	packagePath := strings.ReplaceAll(options.Package, ".", "/")
	alias := strings.ReplaceAll(options.Package, ".", "")
	serviceFile := snakeCase(options.Service) + ".proto"
	serviceRelative := filepath.ToSlash(
		filepath.Join("api", packagePath, serviceFile),
	)
	files := map[string][]byte{
		"api/keelith/project/v1/annotations.proto": []byte(
			projectAnnotations(options.Module),
		),
		serviceRelative: []byte(
			projectService(options, fields, packagePath, alias),
		),
	}

	result := AddAPIResult{Project: project}
	mutations := make(map[string]addMutation, len(files))
	for relative, content := range files {
		path, err := safeAddOutputPath(project, relative, true)
		if err != nil {
			return AddAPIResult{}, err
		}
		existing, readErr := os.ReadFile(path)
		switch {
		case readErr == nil:
			if relative == serviceRelative {
				merged, changed, mergeErr := mergeProjectService(
					existing,
					options,
					fields,
				)
				if mergeErr != nil {
					return AddAPIResult{}, fmt.Errorf(
						"%w: %s: %w",
						ErrConflict,
						relative,
						mergeErr,
					)
				}
				if changed {
					mutations[relative] = addMutation{
						content: merged,
						before:  append([]byte(nil), existing...),
					}
				} else {
					result.Unchanged = append(result.Unchanged, relative)
				}
				continue
			}
			if !equalDigest(existing, content) {
				return AddAPIResult{}, fmt.Errorf(
					"%w: %s already exists with different content",
					ErrConflict,
					relative,
				)
			}
			result.Unchanged = append(result.Unchanged, relative)
		case errors.Is(readErr, os.ErrNotExist):
			mutations[relative] = addMutation{content: content, create: true}
		default:
			return AddAPIResult{}, fmt.Errorf(
				"scaffold: inspect %s: %w",
				relative,
				readErr,
			)
		}
	}
	paths := make([]string, 0, len(mutations))
	for relative := range mutations {
		paths = append(paths, relative)
	}
	sort.Strings(paths)
	applied := make([]string, 0, len(paths))
	for _, relative := range paths {
		if cause := context.Cause(ctx); cause != nil {
			return AddAPIResult{}, rollbackAdd(
				cause,
				project,
				mutations,
				applied,
			)
		}
		mutation := mutations[relative]
		path, pathErr := safeAddOutputPath(project, relative, true)
		if pathErr != nil {
			return AddAPIResult{}, rollbackAdd(
				pathErr,
				project,
				mutations,
				applied,
			)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return AddAPIResult{}, rollbackAdd(
				fmt.Errorf(
					"scaffold: create %s directory: %w",
					relative,
					err,
				),
				project,
				mutations,
				applied,
			)
		}
		if mutation.create {
			if err := createExclusiveFile(path, mutation.content); err != nil {
				return AddAPIResult{}, rollbackAdd(
					fmt.Errorf("%w: create %s: %w", ErrConflict, relative, err),
					project,
					mutations,
					applied,
				)
			}
			result.Created = append(result.Created, relative)
		} else {
			if err := atomicReplaceAddFile(path, mutation.content); err != nil {
				return AddAPIResult{}, rollbackAdd(
					fmt.Errorf("scaffold: update %s: %w", relative, err),
					project,
					mutations,
					applied,
				)
			}
			result.Updated = append(result.Updated, relative)
		}
		applied = append(applied, relative)
	}
	sort.Strings(result.Created)
	sort.Strings(result.Updated)
	sort.Strings(result.Unchanged)
	return result, nil
}

func pathFields(path string) ([]string, error) {
	matches := pathFieldPattern.FindAllStringSubmatch(path, -1)
	result := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		field := match[1]
		if !protoFieldPattern.MatchString(field) {
			return nil, fmt.Errorf(
				"%w: path field %q must be lower_snake_case",
				ErrInvalidInput,
				field,
			)
		}
		if _, duplicate := seen[field]; duplicate {
			continue
		}
		seen[field] = struct{}{}
		result = append(result, field)
	}
	return result, nil
}

func projectAnnotations(module string) string {
	return fmt.Sprintf(`syntax = "proto3";

// Code generated by keelith add api. DO NOT EDIT.
package keelith.project.v1;

import "google/protobuf/descriptor.proto";

option go_package = %q;

enum ServiceDependencyBinding {
  SERVICE_DEPENDENCY_BINDING_UNSPECIFIED = 0;
  SERVICE_DEPENDENCY_BINDING_REMOTE = 1;
  SERVICE_DEPENDENCY_BINDING_AUTO = 2;
  SERVICE_DEPENDENCY_BINDING_LOCAL = 3;
}

message ServiceDependency {
  string service = 1;
  string transport = 2;
  string reason = 3;
  ServiceDependencyBinding binding = 4;
}

message IdempotencyRule {
  string namespace = 1;
  string metadata_key = 2;
  int64 processing_ttl_seconds = 3;
  int64 result_ttl_seconds = 4;
}

extend google.protobuf.MethodOptions {
  string error_reason = 51002;
  repeated ServiceDependency dependency = 51004;
  IdempotencyRule idempotency = 51006;
}

extend google.protobuf.EnumValueOptions {
  int32 error_code = 51005;
}
`, module+"/api/keelith/project/v1;keelithprojectv1")
}

func projectService(
	options AddAPIOptions,
	fields []string,
	packagePath string,
	alias string,
) string {
	var output strings.Builder
	fmt.Fprintf(&output, `syntax = "proto3";

package %s;

import "buf/validate/validate.proto";
import "google/api/annotations.proto";
import "keelith/project/v1/annotations.proto";

option go_package = %q;

`, options.Package, options.Module+"/api/"+packagePath+";"+alias)
	output.WriteString(projectMethodMessages(options, fields))
	fmt.Fprintf(&output, `
service %s {
`, options.Service)
	output.WriteString(projectRPC(options))
	output.WriteString("}\n")
	return output.String()
}

func projectMethodMessages(options AddAPIOptions, fields []string) string {
	request := options.Method + "Request"
	response := options.Method + "Response"
	var output strings.Builder
	fmt.Fprintf(&output, "message %s {\n", request)
	for index, field := range fields {
		fmt.Fprintf(
			&output,
			"  string %s = %d [(buf.validate.field).required = true];\n",
			field,
			index+1,
		)
	}
	fmt.Fprintf(&output, "}\n\nmessage %s {}\n", response)
	return output.String()
}

func projectRPC(options AddAPIOptions) string {
	request := options.Method + "Request"
	response := options.Method + "Response"
	return fmt.Sprintf(`  rpc %s(%s) returns (%s) {
    option (google.api.http) = {
%s%s    };
  }
`, options.Method, request, response,
		projectHTTPPattern(options),
		protoBodyLine(options.HTTPMethod))
}

func projectHTTPPattern(options AddAPIOptions) string {
	switch options.HTTPMethod {
	case "HEAD", "OPTIONS":
		return fmt.Sprintf(
			"      custom: {\n        kind: %q\n        path: %q\n      }\n",
			options.HTTPMethod,
			options.HTTPPath,
		)
	default:
		return fmt.Sprintf(
			"      %s: %q\n",
			strings.ToLower(options.HTTPMethod),
			options.HTTPPath,
		)
	}
}

func protoBodyLine(method string) string {
	if method == "GET" ||
		method == "DELETE" ||
		method == "HEAD" ||
		method == "OPTIONS" {
		return ""
	}
	return "      body: \"*\"\n"
}

func snakeCase(value string) string {
	var output strings.Builder
	for index, character := range value {
		if unicode.IsUpper(character) {
			if index > 0 {
				output.WriteByte('_')
			}
			output.WriteRune(unicode.ToLower(character))
			continue
		}
		output.WriteRune(character)
	}
	return output.String()
}

func equalDigest(first, second []byte) bool {
	firstHash := sha256.Sum256(first)
	secondHash := sha256.Sum256(second)
	return hex.EncodeToString(firstHash[:]) == hex.EncodeToString(secondHash[:])
}

func mergeProjectService(
	content []byte,
	options AddAPIOptions,
	fields []string,
) ([]byte, bool, error) {
	if len(content) == 0 || len(content) > maxAddProtoBytes {
		return nil, false, fmt.Errorf(
			"source is empty or exceeds %d bytes",
			maxAddProtoBytes,
		)
	}
	file, err := parseProjectProto(content)
	if err != nil {
		return nil, false, err
	}
	imports := make(map[string]struct{})
	messages := make(map[string]struct{})
	var service *protoast.ServiceNode
	packageID := ""
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *protoast.PackageNode:
			packageID = string(node.Name.AsIdentifier())
		case *protoast.ImportNode:
			imports[node.Name.AsString()] = struct{}{}
		case *protoast.MessageNode:
			messages[node.Name.Val] = struct{}{}
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
	for _, required := range []string{
		"buf/validate/validate.proto",
		"google/api/annotations.proto",
		"keelith/project/v1/annotations.proto",
	} {
		if _, exists := imports[required]; !exists {
			return nil, false, fmt.Errorf("required import %q is missing", required)
		}
	}
	if service == nil {
		return nil, false, fmt.Errorf("service %s is missing", options.Service)
	}
	for _, declaration := range service.Decls {
		rpc, ok := declaration.(*protoast.RPCNode)
		if !ok || rpc.Name.Val != options.Method {
			continue
		}
		if !equivalentRPC(rpc, options) {
			return nil, false, fmt.Errorf(
				"method %s already exists with different semantics",
				options.Method,
			)
		}
		if _, exists := messages[options.Method+"Request"]; !exists {
			return nil, false, fmt.Errorf(
				"request message %sRequest is missing",
				options.Method,
			)
		}
		if _, exists := messages[options.Method+"Response"]; !exists {
			return nil, false, fmt.Errorf(
				"response message %sResponse is missing",
				options.Method,
			)
		}
		return append([]byte(nil), content...), false, nil
	}
	for _, message := range []string{
		options.Method + "Request",
		options.Method + "Response",
	} {
		if _, exists := messages[message]; exists {
			return nil, false, fmt.Errorf(
				"message %s already exists without method %s",
				message,
				options.Method,
			)
		}
	}
	offset, err := protoSourceOffset(content, service.CloseBrace.Start())
	if err != nil || offset >= len(content) || content[offset] != '}' {
		return nil, false, errors.New("service closing brace has invalid position")
	}
	insertion := projectRPC(options)
	if offset > 0 && content[offset-1] != '\n' {
		insertion = "\n" + insertion
	}
	updated := make([]byte, 0, len(content)+len(insertion)+256)
	updated = append(updated, content[:offset]...)
	updated = append(updated, insertion...)
	updated = append(updated, content[offset:]...)
	if len(updated) > 0 && updated[len(updated)-1] != '\n' {
		updated = append(updated, '\n')
	}
	updated = append(updated, '\n')
	updated = append(updated, projectMethodMessages(options, fields)...)
	if _, err := parseProjectProto(updated); err != nil {
		return nil, false, fmt.Errorf("merged source is invalid: %w", err)
	}
	return updated, true, nil
}

func protoSourceOffset(content []byte, position *protoast.SourcePos) (int, error) {
	if position == nil || position.Line < 1 || position.Col < 1 {
		return 0, errors.New("source position is invalid")
	}
	line := 1
	column := 1
	for offset, character := range string(content) {
		if line == position.Line && column == position.Col {
			return offset, nil
		}
		switch character {
		case '\n':
			line++
			column = 1
		case '\r':
		case '\t':
			column += 8 - ((column - 1) % 8)
		default:
			column++
		}
	}
	if line == position.Line && column == position.Col {
		return len(content), nil
	}
	return 0, errors.New("source position is outside content")
}

func parseProjectProto(content []byte) (*protoast.FileNode, error) {
	const filename = "service.proto"
	files := map[string]string{filename: string(content)}
	validator := protoparse.Parser{
		Accessor:              protoparse.FileContentsFromMap(files),
		ValidateUnlinkedFiles: true,
	}
	if _, err := validator.ParseFilesButDoNotLink(filename); err != nil {
		return nil, fmt.Errorf("parse source: %w", err)
	}
	parser := protoparse.Parser{
		Accessor: protoparse.FileContentsFromMap(files),
	}
	parsed, err := parser.ParseToAST(filename)
	if err != nil {
		return nil, fmt.Errorf("parse source AST: %w", err)
	}
	if len(parsed) != 1 {
		return nil, errors.New("parse source AST: expected one file")
	}
	return parsed[0], nil
}

func equivalentRPC(rpc *protoast.RPCNode, options AddAPIOptions) bool {
	if rpc.Input.Stream != nil ||
		rpc.Output.Stream != nil ||
		string(rpc.Input.MessageType.AsIdentifier()) != options.Method+"Request" ||
		string(rpc.Output.MessageType.AsIdentifier()) != options.Method+"Response" {
		return false
	}
	found := false
	for _, declaration := range rpc.Decls {
		option, ok := declaration.(*protoast.OptionNode)
		if !ok ||
			len(option.Name.Parts) != 1 ||
			option.Name.Parts[0].Value() != "(google.api.http)" {
			continue
		}
		if found || !equivalentHTTPRule(option.Val, options) {
			return false
		}
		found = true
	}
	return found
}

func equivalentHTTPRule(value protoast.ValueNode, options AddAPIOptions) bool {
	literal, ok := value.(*protoast.MessageLiteralNode)
	if !ok {
		return false
	}
	values := make(map[string]string, len(literal.Elements))
	customMethod := ""
	customPath := ""
	hasCustom := false
	for _, element := range literal.Elements {
		name := element.Name.Value()
		if name == "custom" {
			if hasCustom {
				return false
			}
			var valid bool
			customMethod, customPath, valid = equivalentCustomHTTPRule(element.Val)
			if !valid {
				return false
			}
			hasCustom = true
			continue
		}
		stringValue, ok := element.Val.(protoast.StringValueNode)
		if !ok {
			return false
		}
		if _, duplicate := values[name]; duplicate {
			return false
		}
		values[name] = stringValue.AsString()
	}
	if options.HTTPMethod == "HEAD" || options.HTTPMethod == "OPTIONS" {
		if !hasCustom ||
			customMethod != options.HTTPMethod ||
			customPath != options.HTTPPath {
			return false
		}
		return len(values) == 0
	}
	if hasCustom {
		return false
	}
	method := strings.ToLower(options.HTTPMethod)
	if values[method] != options.HTTPPath {
		return false
	}
	delete(values, method)
	if options.HTTPMethod == "GET" || options.HTTPMethod == "DELETE" {
		return len(values) == 0
	}
	return len(values) == 1 && values["body"] == "*"
}

func equivalentCustomHTTPRule(value protoast.ValueNode) (string, string, bool) {
	literal, ok := value.(*protoast.MessageLiteralNode)
	if !ok {
		return "", "", false
	}
	values := make(map[string]string, len(literal.Elements))
	for _, element := range literal.Elements {
		name := element.Name.Value()
		stringValue, ok := element.Val.(protoast.StringValueNode)
		if !ok {
			return "", "", false
		}
		if _, duplicate := values[name]; duplicate {
			return "", "", false
		}
		values[name] = stringValue.AsString()
	}
	if len(values) != 2 {
		return "", "", false
	}
	method, hasMethod := values["kind"]
	path, hasPath := values["path"]
	if !hasMethod || !hasPath {
		return "", "", false
	}
	return strings.ToUpper(strings.TrimSpace(method)), path, true
}

func safeAddOutputPath(
	root string,
	relative string,
	allowMissing bool,
) (string, error) {
	path, err := safeOutputPath(root, relative)
	if err != nil {
		return "", err
	}
	parts := strings.Split(
		filepath.Clean(filepath.FromSlash(relative)),
		string(filepath.Separator),
	)
	current := root
	for index, part := range parts {
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) {
			if allowMissing {
				return path, nil
			}
			return "", fmt.Errorf("%w: %s does not exist", ErrConflict, relative)
		}
		if statErr != nil {
			return "", fmt.Errorf("scaffold: inspect %s: %w", relative, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf(
				"%w: %s traverses a symlink",
				ErrConflict,
				relative,
			)
		}
		if index < len(parts)-1 && !info.IsDir() {
			return "", fmt.Errorf(
				"%w: %s has a non-directory parent",
				ErrConflict,
				relative,
			)
		}
		if index == len(parts)-1 && info.IsDir() {
			return "", fmt.Errorf("%w: %s is a directory", ErrConflict, relative)
		}
	}
	return path, nil
}

func createExclusiveFile(path string, content []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(content)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

func atomicReplaceAddFile(path string, content []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".keelith-add-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return err
	}
	_, writeErr := temporary.Write(content)
	syncErr := temporary.Sync()
	closeErr := temporary.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func rollbackAdd(
	cause error,
	project string,
	mutations map[string]addMutation,
	applied []string,
) error {
	rollbackErrors := make([]error, 0)
	for index := len(applied) - 1; index >= 0; index-- {
		relative := applied[index]
		mutation := mutations[relative]
		path, err := safeAddOutputPath(project, relative, true)
		if err != nil {
			rollbackErrors = append(rollbackErrors, err)
			continue
		}
		if mutation.create {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				rollbackErrors = append(rollbackErrors, err)
			}
			continue
		}
		if err := atomicReplaceAddFile(path, mutation.before); err != nil {
			rollbackErrors = append(rollbackErrors, err)
		}
	}
	return errors.Join(append([]error{cause}, rollbackErrors...)...)
}
