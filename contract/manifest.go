// Package contract defines generated service and dependency manifests.
package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/token"
	"io"
	"path"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// ManifestSchemaVersion is the only generated contract schema accepted by
	// this preview release.
	ManifestSchemaVersion       = "v3"
	maxManifestBytes            = 4 * 1024 * 1024
	maxManifests                = 256
	maxServices                 = 1024
	maxMethods                  = 16 * 1024
	maxRoutes                   = 16 * 1024
	maxDependencies             = 4096
	maxProjections              = 1024
	maxIdentityBytes            = 4 * 1024
	maxAdditionalBindings       = 15
	minInlineBudgetMillis       = int64(1)
	maxInlineBudgetMillis       = int64(60 * 1000)
	minRetentionSeconds         = int64(60)
	maxRetentionSeconds         = int64(30 * 24 * 60 * 60)
	minContinuationPayloadBytes = int64(1)
	maxContinuationPayloadBytes = int64(1024 * 1024)
)

const (
	// DependencyDeclared identifies an application-declared outbound call.
	DependencyDeclared = "declared"
	// DependencyGeneratedAdapter identifies a generated adapter relationship
	// that is not automatically activated by an application profile.
	DependencyGeneratedAdapter = "generated-adapter"
	// DependencyBindingRemote requires the existing remote transport binding.
	DependencyBindingRemote = "REMOTE"
	// DependencyBindingAuto lets one topology plan choose local or remote.
	DependencyBindingAuto = "AUTO"
	// DependencyBindingLocal requires a colocated typed component binding.
	DependencyBindingLocal = "LOCAL"
)

var (
	// ErrInvalidManifest reports malformed or unsafe generated contract data.
	ErrInvalidManifest = errors.New("contract: invalid manifest")
)

// Manifest is one generated Proto file's static contract projection.
type Manifest struct {
	SchemaVersion     string       `json:"schemaVersion"`
	GeneratorProtocol string       `json:"generatorProtocol"`
	Source            string       `json:"source"`
	Package           string       `json:"package"`
	GoImportPath      string       `json:"goImportPath,omitempty"`
	GoPackage         string       `json:"goPackage,omitempty"`
	Services          []Service    `json:"services"`
	Listeners         []Listener   `json:"listeners"`
	Dependencies      []Dependency `json:"dependencies,omitempty"`
	Projections       []Projection `json:"projections,omitempty"`
}

// Service describes one provided Protobuf service.
type Service struct {
	Name    string   `json:"name"`
	GoName  string   `json:"goName,omitempty"`
	Methods []Method `json:"methods"`
}

// Method describes one transport-neutral RPC contract.
type Method struct {
	Name      string       `json:"name"`
	Operation string       `json:"operation"`
	Kind      string       `json:"kind"`
	Input     string       `json:"input"`
	Output    string       `json:"output"`
	HTTP      *HTTPBinding `json:"http,omitempty"`
	// Continuation is present only for unary methods declared as durable calls.
	Continuation *Continuation `json:"continuation,omitempty"`
}

// Continuation describes the bounded durable execution contract of one method.
type Continuation struct {
	MachineVersion     string `json:"machineVersion"`
	InlineBudgetMillis int64  `json:"inlineBudgetMs"`
	RetentionSeconds   int64  `json:"retentionSeconds"`
	MaxPayloadBytes    int64  `json:"maxPayloadBytes"`
}

// HTTPBinding describes the standard google.api.http projection used by the
// generated server, gateway, client, and OpenAPI document.
type HTTPBinding struct {
	Method       string        `json:"method"`
	Path         string        `json:"path"`
	Body         string        `json:"body,omitempty"`
	ResponseBody string        `json:"responseBody,omitempty"`
	Additional   *HTTPBindings `json:"additional,omitempty"`
}

// HTTPBindings is a bounded list kept behind a pointer so adding standard
// additional bindings does not remove HTTPBinding comparability.
type HTTPBindings []HTTPBinding

// Listener describes a generated inbound transport surface.
type Listener struct {
	Transport string  `json:"transport"`
	Service   string  `json:"service"`
	Routes    []Route `json:"routes,omitempty"`
}

// Route describes one static HTTP route.
type Route struct {
	Method    string `json:"method"`
	Path      string `json:"path"`
	Operation string `json:"operation"`
}

// Dependency describes one generated outbound service relationship.
type Dependency struct {
	Kind         string   `json:"kind"`
	Transport    string   `json:"transport"`
	Service      string   `json:"service"`
	GoImportPath string   `json:"goImportPath,omitempty"`
	GoPackage    string   `json:"goPackage,omitempty"`
	GoName       string   `json:"goName,omitempty"`
	Binding      string   `json:"binding,omitempty"`
	Reason       string   `json:"reason"`
	Operations   []string `json:"operations"`
}

// Projection declares one bounded replicated read model and its stable key.
type Projection struct {
	ID          string   `json:"id"`
	Message     string   `json:"message"`
	KeyFields   []string `json:"keyFields"`
	SchemaMajor uint32   `json:"schemaMajor"`
}

// Description is a bounded, low-detail diagnostic projection of a Catalog.
type Description struct {
	Sources      []string             `json:"sources"`
	Services     []ServiceDescription `json:"services"`
	Dependencies []Dependency         `json:"dependencies,omitempty"`
}

// ServiceDescription omits message schemas while retaining the static
// operations and transport surface.
type ServiceDescription struct {
	Name       string   `json:"name"`
	Operations []string `json:"operations"`
	Transports []string `json:"transports"`
	HTTPRoutes int      `json:"httpRoutes"`
}

// Parse strictly decodes and validates one generated manifest.
func Parse(payload []byte) (Manifest, error) {
	if len(payload) == 0 || len(payload) > maxManifestBytes {
		return Manifest{}, fmt.Errorf("%w: payload is empty or exceeds %d bytes", ErrInvalidManifest, maxManifestBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("%w: decode: %w", ErrInvalidManifest, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Manifest{}, fmt.Errorf("%w: payload has trailing content", ErrInvalidManifest)
	}
	if err := Validate(manifest); err != nil {
		return Manifest{}, err
	}
	return cloneManifest(manifest), nil
}

// Validate checks one Manifest's identity, cardinality, and references.
func Validate(manifest Manifest) error {
	if manifest.SchemaVersion != ManifestSchemaVersion {
		return invalid("schema version %q is unsupported", manifest.SchemaVersion)
	}
	if !validIdentity(manifest.GeneratorProtocol) || !validSource(manifest.Source) || !validIdentity(manifest.Package) {
		return invalid("protocol, source, or package is malformed")
	}
	hasGoMetadata := manifest.GoImportPath != "" || manifest.GoPackage != ""
	if hasGoMetadata && (!validGoImportPath(manifest.GoImportPath) || !token.IsIdentifier(manifest.GoPackage)) {
		return invalid("Go import path or package is malformed")
	}
	if len(manifest.Services) > maxServices || len(manifest.Services) == 0 && len(manifest.Projections) == 0 {
		return invalid("service count is outside the supported range")
	}
	serviceNames := make(map[string]struct{}, len(manifest.Services))
	operations := make(map[string]struct{})
	methodCount := 0
	for _, service := range manifest.Services {
		if !validIdentity(service.Name) {
			return invalid("service name %q is malformed", service.Name)
		}
		if service.GoName != "" && !token.IsIdentifier(service.GoName) {
			return invalid("service %q Go name is malformed", service.Name)
		}
		if hasGoMetadata && service.GoName == "" {
			return invalid("service %q has no Go name", service.Name)
		}
		if _, duplicate := serviceNames[service.Name]; duplicate {
			return invalid("service %q is duplicated", service.Name)
		}
		serviceNames[service.Name] = struct{}{}
		if len(service.Methods) == 0 {
			return invalid("service %q has no methods", service.Name)
		}
		methodNames := make(map[string]struct{}, len(service.Methods))
		for _, method := range service.Methods {
			methodCount++
			if methodCount > maxMethods {
				return invalid("method count exceeds %d", maxMethods)
			}
			if !validIdentity(method.Name) || !validOperation(method.Operation) || !validIdentity(method.Input) || !validIdentity(method.Output) || !validKind(method.Kind) {
				return invalid("service %q contains a malformed method", service.Name)
			}
			if _, duplicate := methodNames[method.Name]; duplicate {
				return invalid("service %q method %q is duplicated", service.Name, method.Name)
			}
			methodNames[method.Name] = struct{}{}
			operations[method.Operation] = struct{}{}
			if method.HTTP != nil {
				if err := validateHTTP(*method.HTTP); err != nil {
					return err
				}
			}
			if method.Continuation != nil {
				if method.Kind != "unary" {
					return invalid("continuation method %q is not unary", method.Operation)
				}
				if err := validateContinuation(*method.Continuation); err != nil {
					return err
				}
			}
		}
	}
	if len(manifest.Services) == 0 {
		if len(manifest.Listeners) != 0 {
			return invalid("projection-only manifest declares listeners")
		}
	} else {
		if err := validateListeners(manifest.Listeners, serviceNames, operations); err != nil {
			return err
		}
	}
	if err := validateDependencies(manifest.Dependencies, operations); err != nil {
		return err
	}
	if err := validateProjections(manifest.Projections); err != nil {
		return err
	}
	return nil
}

func validateContinuation(rule Continuation) error {
	if !validIdentity(rule.MachineVersion) || rule.InlineBudgetMillis < minInlineBudgetMillis || rule.InlineBudgetMillis > maxInlineBudgetMillis || rule.RetentionSeconds < minRetentionSeconds || rule.RetentionSeconds > maxRetentionSeconds || rule.MaxPayloadBytes < minContinuationPayloadBytes || rule.MaxPayloadBytes > maxContinuationPayloadBytes {
		return invalid("continuation identity or budget is malformed")
	}
	return nil
}

func validateProjections(projections []Projection) error {
	if len(projections) > maxProjections {
		return invalid("projection count exceeds %d", maxProjections)
	}
	identities := make(map[string]struct{}, len(projections))
	for _, projection := range projections {
		if !validIdentity(projection.ID) || !validIdentity(projection.Message) || projection.SchemaMajor == 0 || len(projection.KeyFields) == 0 || len(projection.KeyFields) > 8 {
			return invalid("projection is malformed")
		}
		if _, duplicate := identities[projection.ID]; duplicate {
			return invalid("projection %q is duplicated", projection.ID)
		}
		identities[projection.ID] = struct{}{}
		fields := make(map[string]struct{}, len(projection.KeyFields))
		for _, field := range projection.KeyFields {
			if !validProjectionField(field) {
				return invalid("projection key field is malformed")
			}
			if _, duplicate := fields[field]; duplicate {
				return invalid("projection %q key field %q is duplicated", projection.ID, field)
			}
			fields[field] = struct{}{}
		}
	}
	return nil
}

func validProjectionField(value string) bool {
	if value == "" || len(value) > 1024 || strings.TrimSpace(value) != value {
		return false
	}
	for index, character := range value {
		valid := character == '_' || character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || index > 0 && character >= '0' && character <= '9'
		if !valid {
			return false
		}
	}
	return true
}

// Catalog merges validated manifests and rejects duplicate service ownership.
type Catalog struct {
	manifests []Manifest
}

// NewCatalog creates one immutable manifest catalog.
func NewCatalog(manifests ...Manifest) (*Catalog, error) {
	if len(manifests) == 0 || len(manifests) > maxManifests {
		return nil, invalid("manifest count is outside the supported range")
	}
	sources := make(map[string]struct{}, len(manifests))
	services := make(map[string]string)
	snapshot := make([]Manifest, 0, len(manifests))
	for _, manifest := range manifests {
		if err := Validate(manifest); err != nil {
			return nil, err
		}
		if _, duplicate := sources[manifest.Source]; duplicate {
			return nil, invalid("source %q is duplicated", manifest.Source)
		}
		sources[manifest.Source] = struct{}{}
		for _, service := range manifest.Services {
			if owner, duplicate := services[service.Name]; duplicate {
				return nil, invalid("service %q is declared by %q and %q", service.Name, owner, manifest.Source)
			}
			services[service.Name] = manifest.Source
		}
		snapshot = append(snapshot, cloneManifest(manifest))
	}
	sort.Slice(snapshot, func(first, second int) bool {
		return snapshot[first].Source < snapshot[second].Source
	})
	return &Catalog{manifests: snapshot}, nil
}

// Manifests returns an independent copy of all source manifests.
func (catalog *Catalog) Manifests() []Manifest {
	if catalog == nil {
		return nil
	}
	result := make([]Manifest, len(catalog.manifests))
	for index, manifest := range catalog.manifests {
		result[index] = cloneManifest(manifest)
	}
	return result
}

// Describe returns deterministic static dependency diagnostics.
func (catalog *Catalog) Describe() Description {
	if catalog == nil {
		return Description{}
	}
	description := Description{}
	for _, manifest := range catalog.manifests {
		description.Sources = append(description.Sources, manifest.Source)
		transports := make(map[string]map[string]struct{})
		httpRoutes := make(map[string]int)
		for _, listener := range manifest.Listeners {
			serviceTransports := transports[listener.Service]
			if serviceTransports == nil {
				serviceTransports = make(map[string]struct{})
				transports[listener.Service] = serviceTransports
			}
			serviceTransports[listener.Transport] = struct{}{}
			httpRoutes[listener.Service] += len(listener.Routes)
		}
		for _, service := range manifest.Services {
			entry := ServiceDescription{
				Name:       service.Name,
				HTTPRoutes: httpRoutes[service.Name],
			}
			for _, method := range service.Methods {
				entry.Operations = append(entry.Operations, method.Operation)
			}
			for transport := range transports[service.Name] {
				entry.Transports = append(entry.Transports, transport)
			}
			sort.Strings(entry.Operations)
			sort.Strings(entry.Transports)
			description.Services = append(description.Services, entry)
		}
		for _, dependency := range manifest.Dependencies {
			description.Dependencies = append(
				description.Dependencies,
				cloneDependency(dependency),
			)
		}
	}
	sort.Strings(description.Sources)
	sort.Slice(description.Services, func(first, second int) bool {
		return description.Services[first].Name < description.Services[second].Name
	})
	sort.Slice(description.Dependencies, func(first, second int) bool {
		left := description.Dependencies[first]
		right := description.Dependencies[second]
		if left.Service == right.Service {
			if left.Transport == right.Transport {
				if left.Binding != right.Binding {
					return left.Binding < right.Binding
				}
				return left.Reason < right.Reason
			}
			return left.Transport < right.Transport
		}
		return left.Service < right.Service
	})
	return description
}

// ValidateDescription protects diagnostic providers that do not use Catalog.
func ValidateDescription(description Description) error {
	if len(description.Sources) > maxManifests ||
		len(description.Services) > maxServices ||
		len(description.Dependencies) > maxDependencies {
		return invalid("diagnostic cardinality exceeds limits")
	}
	for _, source := range description.Sources {
		if !validSource(source) {
			return invalid("diagnostic source is malformed")
		}
	}
	for _, service := range description.Services {
		if !validIdentity(service.Name) || service.HTTPRoutes < 0 || len(service.Operations) > maxMethods || len(service.Transports) > 16 {
			return invalid("diagnostic service is malformed")
		}
		for _, operation := range service.Operations {
			if !validOperation(operation) {
				return invalid("diagnostic operation is malformed")
			}
		}
		for _, transport := range service.Transports {
			if !validIdentity(transport) {
				return invalid("diagnostic transport is malformed")
			}
		}
	}
	return validateDependencies(description.Dependencies, nil)
}

func validateListeners(listeners []Listener, services map[string]struct{}, operations map[string]struct{}) error {
	if len(listeners) == 0 || len(listeners) > maxServices*2 {
		return invalid("listener count is outside the supported range")
	}
	routeCount := 0
	for _, listener := range listeners {
		if !validIdentity(listener.Transport) || !validIdentity(listener.Service) {
			return invalid("listener identity is malformed")
		}
		if _, exists := services[listener.Service]; !exists {
			return invalid("listener service %q is not declared", listener.Service)
		}
		for _, route := range listener.Routes {
			routeCount++
			if routeCount > maxRoutes {
				return invalid("route count exceeds %d", maxRoutes)
			}
			if !validHTTPMethod(route.Method) || !validHTTPPath(route.Path) || !validOperation(route.Operation) {
				return invalid("listener route is malformed")
			}
			if _, exists := operations[route.Operation]; !exists {
				return invalid("route operation %q is not declared", route.Operation)
			}
		}
	}
	return nil
}

func validateDependencies(dependencies []Dependency, knownOperations map[string]struct{}) error {
	if len(dependencies) > maxDependencies {
		return invalid("dependency count exceeds %d", maxDependencies)
	}
	for _, dependency := range dependencies {
		if !validDependencyKind(dependency.Kind) || !validDependencyBinding(dependency.Binding) || !validIdentity(dependency.Transport) || !validIdentity(dependency.Service) || !validIdentity(dependency.Reason) || len(dependency.Operations) == 0 || len(dependency.Operations) > maxMethods {
			return invalid("dependency is malformed")
		}
		hasGoMetadata := dependency.GoImportPath != "" || dependency.GoPackage != "" || dependency.GoName != ""
		if hasGoMetadata &&
			(!validGoImportPath(dependency.GoImportPath) || !token.IsIdentifier(dependency.GoPackage) || !token.IsIdentifier(dependency.GoName)) {
			return invalid("dependency Go binding metadata is malformed")
		}
		if dependency.Kind == DependencyDeclared && !hasGoMetadata {
			return invalid("declared dependency has no Go binding metadata")
		}
		if dependency.Kind == DependencyDeclared && dependency.Binding == "" {
			return invalid("declared dependency has no explicit binding")
		}
		seen := make(map[string]struct{}, len(dependency.Operations))
		for _, operation := range dependency.Operations {
			if !validOperation(operation) {
				return invalid("dependency operation is malformed")
			}
			if _, duplicate := seen[operation]; duplicate {
				return invalid("dependency operation %q is duplicated", operation)
			}
			seen[operation] = struct{}{}
			if knownOperations != nil {
				if _, exists := knownOperations[operation]; !exists {
					return invalid("dependency operation %q is not declared", operation)
				}
			}
		}
	}
	return nil
}

func validDependencyBinding(binding string) bool {
	switch binding {
	case "", DependencyBindingRemote, DependencyBindingAuto, DependencyBindingLocal:
		return true
	default:
		return false
	}
}

func validDependencyKind(kind string) bool {
	switch kind {
	case DependencyDeclared, DependencyGeneratedAdapter:
		return true
	default:
		return false
	}
}

func validateHTTP(binding HTTPBinding) error {
	additional := binding.Additional
	if additional != nil && len(*additional) > maxAdditionalBindings {
		return invalid("HTTP binding has too many additional routes")
	}
	if err := validateHTTPLeaf(binding); err != nil {
		return err
	}
	seen := map[string]struct{}{binding.Method + "\x00" + binding.Path: {}}
	if additional == nil {
		return nil
	}
	for _, additionalBinding := range *additional {
		if additionalBinding.Additional != nil {
			return invalid("nested additional HTTP bindings are unsupported")
		}
		if err := validateHTTPLeaf(additionalBinding); err != nil {
			return err
		}
		key := additionalBinding.Method + "\x00" + additionalBinding.Path
		if _, duplicate := seen[key]; duplicate {
			return invalid("HTTP binding route is duplicated")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateHTTPLeaf(binding HTTPBinding) error {
	if !validHTTPMethod(binding.Method) || !validHTTPPath(binding.Path) || !validHTTPBody(binding.Body) || binding.ResponseBody != "" &&
		!validHTTPResponseBodyPath(binding.ResponseBody) {
		return invalid("HTTP binding is malformed")
	}
	if (binding.Method == "GET" || binding.Method == "DELETE" || binding.Method == "HEAD") && binding.Body != "" {
		return invalid("HTTP GET/DELETE binding cannot have a body")
	}
	return nil
}

func validHTTPBody(body string) bool {
	if body == "" || body == "*" {
		return true
	}
	return validHTTPResponseBodyPath(body)
}

func validHTTPFieldName(value string) bool {
	if value == "" || len(value) > 1024 || strings.TrimSpace(value) != value {
		return false
	}
	if strings.Contains(value, ".") {
		return false
	}
	for index, character := range value {
		valid := character == '_' || character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || index > 0 && character >= '0' && character <= '9'
		if !valid {
			return false
		}
	}
	return true
}

func validHTTPResponseBodyPath(value string) bool {
	if value == "" || len(value) > 1024 || strings.TrimSpace(value) != value {
		return false
	}
	segments := strings.Split(value, ".")
	if len(segments) > 16 {
		return false
	}
	for _, segment := range segments {
		if !validHTTPFieldName(segment) {
			return false
		}
	}
	return true
}

func validHTTPMethod(method string) bool {
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
		return true
	default:
		return false
	}
}

func validHTTPPath(value string) bool {
	return strings.HasPrefix(value, "/") &&
		!strings.ContainsAny(value, "?#\r\n") &&
		validIdentity(value)
}

func validKind(kind string) bool {
	switch kind {
	case "unary", "client-stream", "server-stream", "bidi-stream":
		return true
	default:
		return false
	}
}

func validOperation(value string) bool {
	return strings.HasPrefix(value, "/") && validIdentity(value)
}

func validSource(value string) bool {
	return validIdentity(value) && !strings.HasPrefix(value, "/") && path.Clean(value) == value && value != "." && !strings.HasPrefix(value, "../")
}

func validGoImportPath(value string) bool {
	if !validSource(value) || !strings.Contains(value, "/") {
		return false
	}
	first, _, _ := strings.Cut(value, "/")
	return strings.Contains(first, ".") && !strings.HasPrefix(first, ".") && !strings.HasSuffix(first, ".")
}

func validIdentity(value string) bool {
	if value == "" || len(value) > maxIdentityBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func invalid(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidManifest, fmt.Sprintf(format, arguments...))
}

func cloneManifest(manifest Manifest) Manifest {
	result := manifest
	result.Services = make([]Service, len(manifest.Services))
	for serviceIndex, service := range manifest.Services {
		result.Services[serviceIndex] = service
		result.Services[serviceIndex].Methods = make([]Method, len(service.Methods))
		for methodIndex, method := range service.Methods {
			result.Services[serviceIndex].Methods[methodIndex] = method
			if method.HTTP != nil {
				result.Services[serviceIndex].Methods[methodIndex].HTTP =
					cloneHTTPBinding(method.HTTP)
			}
			if method.Continuation != nil {
				continuation := *method.Continuation
				result.Services[serviceIndex].Methods[methodIndex].Continuation =
					&continuation
			}
		}
	}
	result.Listeners = make([]Listener, len(manifest.Listeners))
	for index, listener := range manifest.Listeners {
		result.Listeners[index] = listener
		result.Listeners[index].Routes = append([]Route(nil), listener.Routes...)
	}
	result.Dependencies = make([]Dependency, len(manifest.Dependencies))
	for index, dependency := range manifest.Dependencies {
		result.Dependencies[index] = cloneDependency(dependency)
	}
	result.Projections = make([]Projection, len(manifest.Projections))
	for index, projection := range manifest.Projections {
		result.Projections[index] = projection
		result.Projections[index].KeyFields = append([]string(nil), projection.KeyFields...)
	}
	return result
}

func cloneHTTPBinding(binding *HTTPBinding) *HTTPBinding {
	if binding == nil {
		return nil
	}
	result := *binding
	if binding.Additional != nil {
		additional := append(HTTPBindings(nil), (*binding.Additional)...)
		for index := range additional {
			if additional[index].Additional != nil {
				nested := append(HTTPBindings(nil), (*additional[index].Additional)...)
				additional[index].Additional = &nested
			}
		}
		result.Additional = &additional
	}
	return &result
}

func cloneDependency(dependency Dependency) Dependency {
	dependency.Operations = append([]string(nil), dependency.Operations...)
	return dependency
}
