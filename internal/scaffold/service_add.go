package scaffold

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/keelab/keelith/contract"
)

// AddServiceOptions describes a small, transport-neutral unary service to add
// to a generated project. The starter operation deliberately uses
// google.protobuf.Empty so it can be compiled without a protoc toolchain;
// richer request schemas can still be added with AddAPI and the normal Buf
// workflow.
type AddServiceOptions struct {
	Project    string
	Package    string
	Service    string
	Method     string
	HTTPMethod string
	HTTPPath   string
	Module     string
}

// AddServiceResult describes files created by AddService and its generated
// adapter synchronization.
type AddServiceResult struct {
	Project   string
	Created   []string
	Updated   []string
	Unchanged []string
}

// AddService adds one runnable unary service contract and its Keelith
// adapter. It does not contact external services or invoke a protobuf
// compiler. Calling SyncServices afterwards creates the implementation root
// and aggregate registration file.
func AddService(
	ctx context.Context,
	options AddServiceOptions,
) (AddServiceResult, error) {
	if ctx == nil {
		return AddServiceResult{}, fmt.Errorf("%w: context is nil", ErrInvalidInput)
	}
	if cause := context.Cause(ctx); cause != nil {
		return AddServiceResult{}, cause
	}
	project, err := filepath.Abs(strings.TrimSpace(options.Project))
	if err != nil {
		return AddServiceResult{}, fmt.Errorf("%w: project path: %w", ErrInvalidInput, err)
	}
	project, err = filepath.EvalSymlinks(project)
	if err != nil {
		return AddServiceResult{}, fmt.Errorf("%w: resolve project symlinks: %w", ErrInvalidInput, err)
	}
	info, err := os.Stat(project)
	if err != nil || !info.IsDir() {
		return AddServiceResult{}, fmt.Errorf("%w: project directory is unavailable", ErrInvalidInput)
	}

	options.Package = strings.TrimSpace(options.Package)
	options.Service = strings.TrimSpace(options.Service)
	options.Method = strings.TrimSpace(options.Method)
	options.HTTPMethod = strings.ToUpper(strings.TrimSpace(options.HTTPMethod))
	options.HTTPPath = strings.TrimSpace(options.HTTPPath)
	options.Module = strings.TrimSpace(options.Module)
	if !protoPackagePattern.MatchString(options.Package) {
		return AddServiceResult{}, fmt.Errorf(
			"%w: proto package %q is invalid",
			ErrInvalidInput,
			options.Package,
		)
	}
	if !servicePattern.MatchString(options.Service) || !servicePattern.MatchString(options.Method) {
		return AddServiceResult{}, fmt.Errorf(
			"%w: service or method is not an identifier",
			ErrInvalidInput,
		)
	}
	if err := validateModule(options.Module); err != nil {
		return AddServiceResult{}, err
	}
	if !strings.HasPrefix(options.HTTPPath, "/") ||
		strings.ContainsAny(options.HTTPPath, "?#\r\n") ||
		strings.ContainsAny(options.HTTPPath, "{}") {
		return AddServiceResult{}, fmt.Errorf(
			"%w: starter HTTP path %q must be a literal path without captures",
			ErrInvalidInput,
			options.HTTPPath,
		)
	}
	switch options.HTTPMethod {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
	default:
		return AddServiceResult{}, fmt.Errorf(
			"%w: HTTP method %q is unsupported",
			ErrInvalidInput,
			options.HTTPMethod,
		)
	}

	packagePath := strings.ReplaceAll(options.Package, ".", "/")
	goPackage := serviceGoPackage(options.Package)
	serviceFile := snakeCase(options.Service) + ".proto"
	sourceRelative := filepath.ToSlash(filepath.Join("api", packagePath, serviceFile))
	adapterRelative := filepath.ToSlash(filepath.Join(
		"gen", packagePath, strings.TrimSuffix(serviceFile, ".proto")+".keelith.gen.go",
	))
	manifestRelative := filepath.ToSlash(filepath.Join(
		"api", packagePath, strings.TrimSuffix(serviceFile, ".proto")+".keelith.manifest.json",
	))
	serviceName := options.Package + "." + options.Service
	operation := "/" + serviceName + "/" + options.Method
	files := map[string][]byte{
		sourceRelative:  []byte(renderStarterProto(options, goPackage)),
		adapterRelative: renderStarterAdapter(options, goPackage, serviceName, sourceRelative),
	}
	manifest, err := starterManifest(
		options,
		serviceName,
		operation,
		packagePath,
		goPackage,
		sourceRelative,
	)
	if err != nil {
		return AddServiceResult{}, err
	}
	files[manifestRelative] = manifest

	result := AddServiceResult{Project: project}
	mutations := make(map[string]addMutation, len(files))
	for relative, content := range files {
		output, pathErr := safeAddOutputPath(project, relative, true)
		if pathErr != nil {
			return AddServiceResult{}, pathErr
		}
		existing, readErr := os.ReadFile(output)
		switch {
		case readErr == nil:
			if !equalDigest(existing, content) {
				return AddServiceResult{}, fmt.Errorf(
					"%w: %s already exists with different content",
					ErrConflict,
					relative,
				)
			}
			result.Unchanged = append(result.Unchanged, relative)
		case errors.Is(readErr, os.ErrNotExist):
			mutations[relative] = addMutation{content: content, create: true}
		default:
			return AddServiceResult{}, fmt.Errorf("scaffold: inspect %s: %w", relative, readErr)
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
			return AddServiceResult{}, rollbackAdd(cause, project, mutations, applied)
		}
		output, pathErr := safeAddOutputPath(project, relative, true)
		if pathErr != nil {
			return AddServiceResult{}, rollbackAdd(pathErr, project, mutations, applied)
		}
		if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
			return AddServiceResult{}, rollbackAdd(
				fmt.Errorf("scaffold: create %s directory: %w", relative, err),
				project,
				mutations,
				applied,
			)
		}
		if err := createExclusiveFile(output, mutations[relative].content); err != nil {
			return AddServiceResult{}, rollbackAdd(
				fmt.Errorf("%w: create %s: %w", ErrConflict, relative, err),
				project,
				mutations,
				applied,
			)
		}
		applied = append(applied, relative)
		result.Created = append(result.Created, relative)
	}
	sort.Strings(result.Created)
	sort.Strings(result.Unchanged)
	return result, nil
}

func starterManifest(
	options AddServiceOptions,
	serviceName string,
	operation string,
	packagePath string,
	goPackage string,
	sourceRelative string,
) ([]byte, error) {
	manifest := contract.Manifest{
		SchemaVersion:     contract.ManifestSchemaVersion,
		GeneratorProtocol: "v1",
		Source:            filepath.ToSlash(strings.TrimPrefix(sourceRelative, "api/")),
		Package:           options.Package,
		GoImportPath:      options.Module + "/gen/" + packagePath,
		GoPackage:         goPackage,
		Services: []contract.Service{{
			Name:   serviceName,
			GoName: options.Service,
			Methods: []contract.Method{{
				Name:      options.Method,
				Operation: operation,
				Kind:      "unary",
				Input:     "google.protobuf.Empty",
				Output:    "google.protobuf.Empty",
				HTTP: &contract.HTTPBinding{
					Method: options.HTTPMethod,
					Path:   options.HTTPPath,
				},
			}},
		}},
		Listeners: []contract.Listener{
			{Transport: "grpc", Service: serviceName},
			{
				Transport: "http",
				Service:   serviceName,
				Routes: []contract.Route{{
					Method:    options.HTTPMethod,
					Path:      options.HTTPPath,
					Operation: operation,
				}},
			},
		},
		Dependencies: []contract.Dependency{{
			Kind:         contract.DependencyGeneratedAdapter,
			Transport:    "grpc",
			Service:      serviceName,
			GoImportPath: options.Module + "/gen/" + packagePath,
			GoPackage:    goPackage,
			GoName:       options.Service,
			Reason:       "generated-http-gateway",
			Operations:   []string{operation},
		}},
	}
	if err := contract.Validate(manifest); err != nil {
		return nil, fmt.Errorf("scaffold: validate starter manifest: %w", err)
	}
	document, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("scaffold: encode starter manifest: %w", err)
	}
	return append(document, '\n'), nil
}

func renderStarterProto(
	options AddServiceOptions,
	goPackage string,
) string {
	return fmt.Sprintf(`syntax = "proto3";

package %s;

import "google/protobuf/empty.proto";

option go_package = %q;

service %s {
  rpc %s(google.protobuf.Empty) returns (google.protobuf.Empty);
}
`, options.Package, options.Module+"/gen/"+strings.ReplaceAll(options.Package, ".", "/")+";"+goPackage, options.Service, options.Method)
}

func renderStarterAdapter(
	options AddServiceOptions,
	goPackage string,
	serviceName string,
	sourceRelative string,
) []byte {
	source := generatedEchoSource
	// Replace the fixed starter symbols with private sentinels first. Values
	// supplied by the caller are inserted only after all source symbols have
	// been removed, so names such as PingService cannot recursively rewrite the
	// generated method or service identifiers.
	const (
		commentToken      = "\x00keelith-comment\x00"
		packageToken      = "\x00keelith-package\x00"
		packagePathToken  = "\x00keelith-package-path\x00"
		serviceToken      = "\x00keelith-service\x00"
		serviceGoToken    = "\x00keelith-service-go\x00"
		methodToken       = "\x00keelith-method\x00"
		httpMethodToken   = "\x00keelith-http-method\x00"
		httpPathToken     = "\x00keelith-http-path\x00"
		metadataPathToken = "\x00keelith-metadata-path\x00"
	)
	replacements := []struct{ old, token string }{
		{"// Code generated by keelith new. DO NOT EDIT.", commentToken},
		{"echo.v1.EchoService", serviceToken},
		{"EchoService", serviceGoToken},
		{"Ping", methodToken},
		{"http.MethodGet", httpMethodToken},
		{"\"/v1/ping\"", httpPathToken},
		{"api/echo/v1/echo.proto", metadataPathToken},
		{"gen/echo/v1", packagePathToken},
		{"echov1", packageToken},
	}
	for _, replacement := range replacements {
		source = strings.ReplaceAll(source, replacement.old, replacement.token)
	}
	values := []struct{ token, value string }{
		{commentToken, "// Code generated by keelith add service. DO NOT EDIT."},
		{serviceToken, serviceName},
		{serviceGoToken, options.Service},
		{methodToken, options.Method},
		{httpMethodToken, "http." + httpMethodConstant(options.HTTPMethod)},
		{httpPathToken, strconv.Quote(options.HTTPPath)},
		{metadataPathToken, filepath.ToSlash(strings.TrimPrefix(sourceRelative, "api/"))},
		{packagePathToken, "gen/" + strings.ReplaceAll(options.Package, ".", "/")},
		{packageToken, goPackage},
	}
	for _, value := range values {
		source = strings.ReplaceAll(source, value.token, value.value)
	}
	formatted, err := format.Source([]byte(source))
	if err != nil {
		return []byte(source)
	}
	return formatted
}

func serviceGoPackage(packageID string) string {
	value := strings.ReplaceAll(packageID, ".", "")
	if value == "" || !token.IsIdentifier(value) || token.Lookup(value).IsKeyword() {
		return "serviceapi"
	}
	return value
}

func httpMethodConstant(method string) string {
	switch method {
	case "GET":
		return "MethodGet"
	case "POST":
		return "MethodPost"
	case "PUT":
		return "MethodPut"
	case "PATCH":
		return "MethodPatch"
	case "DELETE":
		return "MethodDelete"
	case "HEAD":
		return "MethodHead"
	default:
		return "MethodOptions"
	}
}
