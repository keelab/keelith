package keelith

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"time"

	stdhttp "net/http"

	kapp "github.com/keelab/keelith/app"
	"github.com/keelab/keelith/config"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/server"
	"github.com/keelab/keelith/service"
	"go.opentelemetry.io/otel/propagation"
)

// Option configures an Application.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (fn optionFunc) apply(options *options) error {
	return fn(options)
}

type options struct {
	name           string
	httpAddress    string
	httpSet        bool
	httpDefault    bool
	grpcAddress    string
	grpcSet        bool
	grpcDefault    bool
	bindings       []service.Binding
	routes         []route
	servers        []server.Server
	components     []kapp.Component
	health         *health.Registry
	metadataPolicy *metadata.Policy
	propagator     propagation.TextMapPropagator
	serverBundle   *middleware.Bundle
	streamBundle   *middleware.StreamBundle
	output         io.Writer
	stopTimeout    time.Duration
	opsOptions     []ops.Option
	opsSet         bool
	profile        *service.Profile
	graph          Graph
	cleanups       []func(context.Context) error
	configFile     *configFileSettings
	configManager  *config.Manager
}

// Graph is the minimal dependency-graph contract accepted by the facade.
// *di.Graph satisfies this interface without making the facade depend on the
// DI implementation package.
type Graph interface {
	Close(context.Context) error
	Components() []kapp.Component
}

// ConfigFileOption customizes the standard file-backed configuration runtime.
// The options are intentionally separate from Application options so the
// minimal path remains a single WithConfigFile call.
type ConfigFileOption interface {
	applyConfigFile(*configFileSettings) error
}

type configFileOptionFunc func(*configFileSettings) error

func (fn configFileOptionFunc) applyConfigFile(settings *configFileSettings) error {
	return fn(settings)
}

type configFileSettings struct {
	path          string
	envPrefix     string
	knownFields   []string
	bindings      []config.Binding
	rejectUnknown bool
	pollInterval  time.Duration
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func normalizeListenerAddress(value string) (string, error) {
	normalized := strings.TrimSpace(value)
	if normalized == "" || normalized != value {
		return "", errors.New("address is empty or not normalized")
	}
	if strings.HasPrefix(normalized, ":") {
		normalized = "127.0.0.1" + normalized
	}
	host, port, err := net.SplitHostPort(normalized)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf("invalid listen address %q", value)
	}
	return normalized, nil
}

// WithConfigEnvPrefix adds an optional environment source above the file
// source. For example, "ORDERS" maps ORDERS_HTTP__ADDRESS to http.address.
func WithConfigEnvPrefix(prefix string) ConfigFileOption {
	return configFileOptionFunc(func(settings *configFileSettings) error {
		normalized := strings.TrimSpace(prefix)
		if normalized == "" || strings.Contains(normalized, "=") {
			return errors.New("config environment prefix is invalid")
		}
		settings.envPrefix = normalized
		return nil
	})
}

// WithConfigKnownFields declares paths checked when WithConfigStrict is used.
func WithConfigKnownFields(paths ...string) ConfigFileOption {
	snapshot := append([]string(nil), paths...)
	return configFileOptionFunc(func(settings *configFileSettings) error {
		for index, path := range snapshot {
			normalized := strings.TrimSpace(path)
			if normalized == "" {
				return fmt.Errorf("config known field %d is empty", index)
			}
			settings.knownFields = append(settings.knownFields, normalized)
		}
		return nil
	})
}

// WithConfigBindings adds typed configuration bindings to the file manager.
func WithConfigBindings(bindings ...config.Binding) ConfigFileOption {
	snapshot := append([]config.Binding(nil), bindings...)
	return configFileOptionFunc(func(settings *configFileSettings) error {
		for index, binding := range snapshot {
			if isNilValue(binding) {
				return fmt.Errorf("config binding %d is nil", index)
			}
		}
		settings.bindings = append(settings.bindings, snapshot...)
		return nil
	})
}

// WithConfigStrict rejects fields not listed by WithConfigKnownFields.
func WithConfigStrict() ConfigFileOption {
	return configFileOptionFunc(func(settings *configFileSettings) error {
		settings.rejectUnknown = true
		return nil
	})
}

// WithConfigPollInterval controls how often a file-backed runtime checks for
// a new revision. It does not cause a file read during New.
func WithConfigPollInterval(interval time.Duration) ConfigFileOption {
	return configFileOptionFunc(func(settings *configFileSettings) error {
		if interval <= 0 {
			return errors.New("config poll interval must be positive")
		}
		settings.pollInterval = interval
		return nil
	})
}

// WithConfigFile enables a standard file-backed configuration runtime. The
// file is loaded when Application.Run starts, not while New is constructing
// the object graph.
func WithConfigFile(path string, optionList ...ConfigFileOption) Option {
	return optionFunc(func(options *options) error {
		if options.configFile != nil || options.configManager != nil {
			return errors.New("config source is already configured")
		}
		normalized := strings.TrimSpace(path)
		if normalized == "" || normalized != path {
			return errors.New("config file path is empty or not normalized")
		}
		settings := configFileSettings{path: normalized}
		for index, option := range optionList {
			if isNilValue(option) {
				return fmt.Errorf("config file option %d is nil", index)
			}
			if err := option.applyConfigFile(&settings); err != nil {
				return fmt.Errorf("config file option %d: %w", index, err)
			}
		}
		if settings.rejectUnknown && len(settings.knownFields) == 0 {
			return errors.New("strict config requires at least one known field")
		}
		options.configFile = &settings
		return nil
	})
}

// WithConfigManager supplies an already configured Manager for remote or
// otherwise custom configuration sources. It is an advanced escape hatch;
// the facade still owns the Manager's Runtime lifecycle.
func WithConfigManager(manager *config.Manager) Option {
	return optionFunc(func(options *options) error {
		if manager == nil {
			return errors.New("config manager is nil")
		}
		if options.configFile != nil || options.configManager != nil {
			return errors.New("config source is already configured")
		}
		options.configManager = manager
		return nil
	})
}

// RouteHandler is the minimal HTTP handler used by the quick-start facade.
// Applications that need typed protocol contracts should use generated
// service bindings with WithServices instead.
type RouteHandler func(context.Context, *stdhttp.Request) (any, error)

type route struct {
	method  string
	path    string
	handler RouteHandler
}

// RouteDescription is the value-free diagnostic projection of a quick-start
// route.
type RouteDescription struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

// ConfigDescription is a secret-free configuration runtime projection.
type ConfigDescription struct {
	Enabled           bool                      `json:"enabled"`
	Path              string                    `json:"path,omitempty"`
	EnvironmentPrefix string                    `json:"environment_prefix,omitempty"`
	Strict            bool                      `json:"strict"`
	Runtime           config.RuntimeDescription `json:"runtime"`
}

// Description is a value-free snapshot of the facade's construction plan and
// lifecycle. It deliberately excludes secrets and concrete dependency values.
type Description struct {
	Name       string              `json:"name"`
	State      kapp.State          `json:"state"`
	HTTP       ListenerDescription `json:"http"`
	GRPC       ListenerDescription `json:"grpc"`
	Ops        ListenerDescription `json:"ops"`
	Profile    service.Description `json:"profile"`
	Routes     []RouteDescription  `json:"routes,omitempty"`
	Servers    []string            `json:"servers,omitempty"`
	Components []string            `json:"components,omitempty"`
	Config     *ConfigDescription  `json:"config,omitempty"`
	Graph      bool                `json:"graph"`
}

// ListenerDescription describes one generated listener without exposing
// sockets or credentials.
type ListenerDescription struct {
	Enabled bool   `json:"enabled"`
	Address string `json:"address,omitempty"`
}

// WithName sets the logical service name used by defaults and diagnostics.
func WithName(name string) Option {
	return optionFunc(func(options *options) error {
		normalized := strings.TrimSpace(name)
		if normalized == "" || normalized != name {
			return fmt.Errorf("name %q is empty or not normalized", name)
		}
		options.name = normalized
		return nil
	})
}

// WithHTTP enables the default HTTP listener at address.
func WithHTTP(address string) Option {
	return optionFunc(func(options *options) error {
		if options.httpSet && !options.httpDefault {
			return errors.New("http listener is already configured")
		}
		normalized, err := normalizeListenerAddress(address)
		if err != nil {
			return fmt.Errorf("http address: %w", err)
		}
		options.httpAddress = normalized
		options.httpSet = true
		options.httpDefault = false
		return nil
	})
}

// WithDefaultHTTP supplies a generated wiring default that an explicit
// WithHTTP option may override. It is intended for project composition roots,
// not for resolving two explicit application settings.
func WithDefaultHTTP(address string) Option {
	return optionFunc(func(options *options) error {
		if options.httpSet {
			if options.httpDefault {
				return errors.New("http listener default is already configured")
			}
			return nil
		}
		normalized, err := normalizeListenerAddress(address)
		if err != nil {
			return fmt.Errorf("http default address: %w", err)
		}
		options.httpAddress = normalized
		options.httpSet = true
		options.httpDefault = true
		return nil
	})
}

// WithGRPC enables the default gRPC listener at address.
func WithGRPC(address string) Option {
	return optionFunc(func(options *options) error {
		if options.grpcSet && !options.grpcDefault {
			return errors.New("grpc listener is already configured")
		}
		normalized, err := normalizeListenerAddress(address)
		if err != nil {
			return fmt.Errorf("grpc address: %w", err)
		}
		options.grpcAddress = normalized
		options.grpcSet = true
		options.grpcDefault = false
		return nil
	})
}

// WithDefaultGRPC supplies a generated wiring default that an explicit
// WithGRPC option may override. It is intended for project composition roots,
// not for resolving two explicit application settings.
func WithDefaultGRPC(address string) Option {
	return optionFunc(func(options *options) error {
		if options.grpcSet {
			if options.grpcDefault {
				return errors.New("grpc listener default is already configured")
			}
			return nil
		}
		normalized, err := normalizeListenerAddress(address)
		if err != nil {
			return fmt.Errorf("grpc default address: %w", err)
		}
		options.grpcAddress = normalized
		options.grpcSet = true
		options.grpcDefault = true
		return nil
	})
}

// WithoutDefaultHTTP removes the generated HTTP listener when it has not
// been explicitly configured by the caller.
func WithoutDefaultHTTP() Option {
	return optionFunc(func(options *options) error {
		if options.httpDefault {
			options.httpSet = false
			options.httpDefault = false
		}
		return nil
	})
}

// WithoutDefaultGRPC removes the generated gRPC listener when it has not
// been explicitly configured by the caller.
func WithoutDefaultGRPC() Option {
	return optionFunc(func(options *options) error {
		if options.grpcDefault {
			options.grpcSet = false
			options.grpcDefault = false
		}
		return nil
	})
}

// WithServices registers generated service bindings for the configured
// listeners. Bindings may support HTTP, gRPC, or both transports.
func WithServices(bindings ...service.Binding) Option {
	snapshot := append([]service.Binding(nil), bindings...)
	return optionFunc(func(options *options) error {
		for index, binding := range snapshot {
			if err := binding.Validate(); err != nil {
				return fmt.Errorf("service binding %d: %w", index, err)
			}
		}
		options.bindings = append(options.bindings, snapshot...)
		return nil
	})
}

// WithRoute registers a small JSON HTTP route for the quick-start path. It is
// intentionally limited to request/response handlers; generated service
// bindings remain the standard API for typed HTTP and gRPC contracts.
func WithRoute(method, path string, handler RouteHandler) Option {
	return optionFunc(func(options *options) error {
		trimmedMethod := strings.TrimSpace(method)
		normalizedMethod := strings.ToUpper(trimmedMethod)
		if normalizedMethod == "" || trimmedMethod != method {
			return errors.New("route method is empty or not normalized")
		}
		normalizedPath := strings.TrimSpace(path)
		if normalizedPath == "" || normalizedPath != path ||
			!strings.HasPrefix(normalizedPath, "/") ||
			strings.ContainsAny(normalizedPath, "\r\n") {
			return errors.New("route path is empty or invalid")
		}
		if handler == nil {
			return errors.New("route handler is nil")
		}
		switch normalizedMethod {
		case stdhttp.MethodGet, stdhttp.MethodPost, stdhttp.MethodPut,
			stdhttp.MethodPatch, stdhttp.MethodDelete, stdhttp.MethodHead,
			stdhttp.MethodOptions:
		default:
			return fmt.Errorf("route method %q is unsupported", method)
		}
		options.routes = append(options.routes, route{
			method:  normalizedMethod,
			path:    normalizedPath,
			handler: handler,
		})
		return nil
	})
}

// WithServer adds an application-owned server to the lifecycle. It is an
// escape hatch for transports or workers that are not represented by a
// generated service binding.
func WithServer(servers ...server.Server) Option {
	snapshot := append([]server.Server(nil), servers...)
	return optionFunc(func(options *options) error {
		for index, item := range snapshot {
			if isNilValue(item) {
				return fmt.Errorf("server %d is nil", index)
			}
		}
		options.servers = append(options.servers, snapshot...)
		return nil
	})
}

// WithOps enables the dedicated operational server. Only health endpoints are
// enabled by default; optional diagnostics still require explicit ops.Option
// values and retain the ops package's loopback/access-policy checks.
func WithOps(optionList ...ops.Option) Option {
	snapshot := append([]ops.Option(nil), optionList...)
	return optionFunc(func(options *options) error {
		if options.opsSet {
			return errors.New("ops server is already configured")
		}
		for index, option := range snapshot {
			if isNilValue(option) {
				return fmt.Errorf("ops option %d is nil", index)
			}
		}
		options.opsOptions = snapshot
		options.opsSet = true
		return nil
	})
}

// WithComponent adds an application component whose lifecycle is managed by
// the returned Application.
func WithComponent(components ...kapp.Component) Option {
	snapshot := append([]kapp.Component(nil), components...)
	return optionFunc(func(options *options) error {
		for index, component := range snapshot {
			if isNilValue(component) {
				return fmt.Errorf("component %d is nil", index)
			}
		}
		options.components = append(options.components, snapshot...)
		return nil
	})
}

// WithHealth uses an application-owned health registry.
func WithHealth(registry *health.Registry) Option {
	return optionFunc(func(options *options) error {
		if registry == nil {
			return errors.New("health registry is nil")
		}
		options.health = registry
		return nil
	})
}

// WithMetadataPolicy replaces the default inbound metadata allowlist.
func WithMetadataPolicy(policy metadata.Policy) Option {
	return optionFunc(func(options *options) error {
		snapshot := policy
		options.metadataPolicy = &snapshot
		return nil
	})
}

// WithPropagator replaces the default trace context propagator.
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return optionFunc(func(options *options) error {
		if isNilValue(propagator) {
			return errors.New("propagator is nil")
		}
		options.propagator = propagator
		return nil
	})
}

// WithMiddleware supplies one prebuilt inbound middleware bundle to both
// HTTP and gRPC listeners. It is intended for advanced applications that
// already own an auditable middleware chain.
func WithMiddleware(bundle *middleware.Bundle) Option {
	return optionFunc(func(options *options) error {
		if bundle == nil {
			return errors.New("middleware bundle is nil")
		}
		options.serverBundle = bundle
		return nil
	})
}

// WithStreamMiddleware supplies a prebuilt gRPC stream middleware bundle.
func WithStreamMiddleware(bundle *middleware.StreamBundle) Option {
	return optionFunc(func(options *options) error {
		if bundle == nil {
			return errors.New("stream middleware bundle is nil")
		}
		options.streamBundle = bundle
		return nil
	})
}

// WithOutput selects the destination for the default structured logger.
func WithOutput(output io.Writer) Option {
	return optionFunc(func(options *options) error {
		if isNilValue(output) {
			return errors.New("logging output is nil")
		}
		options.output = output
		return nil
	})
}

// WithStopTimeout sets the graceful shutdown deadline.
func WithStopTimeout(timeout time.Duration) Option {
	return optionFunc(func(options *options) error {
		if timeout <= 0 {
			return errors.New("stop timeout must be positive")
		}
		options.stopTimeout = timeout
		return nil
	})
}

// WithProfile supplies an already validated service profile. When set,
// WithServices cannot be used in the same Application.
func WithProfile(profile *service.Profile) Option {
	return optionFunc(func(options *options) error {
		if profile == nil {
			return errors.New("service profile is nil")
		}
		options.profile = profile
		return nil
	})
}

// WithGraph attaches a construction graph's cleanup to the Application. The
// graph's lifecycle components are also registered with app.App.
func WithGraph(graph Graph) Option {
	return optionFunc(func(options *options) error {
		if isNilValue(graph) {
			return errors.New("dependency graph is nil")
		}
		if options.graph != nil {
			return errors.New("dependency graph is already configured")
		}
		options.graph = graph
		options.components = append(options.components, graph.Components()...)
		return nil
	})
}

// WithCleanup transfers construction-owned cleanup functions to the
// Application. They run once, in reverse registration order, when the
// application stops or closes.
func WithCleanup(cleanups ...func(context.Context) error) Option {
	snapshot := append([]func(context.Context) error(nil), cleanups...)
	return optionFunc(func(options *options) error {
		for index, cleanup := range snapshot {
			if cleanup == nil {
				return fmt.Errorf("cleanup %d is nil", index)
			}
		}
		options.cleanups = append(options.cleanups, snapshot...)
		return nil
	})
}
