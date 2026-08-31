package keelith

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"

	kapp "github.com/keelab/keelith/app"
	"github.com/keelab/keelith/config"
	kenv "github.com/keelab/keelith/config/env"
	configfile "github.com/keelab/keelith/config/file"
	"github.com/keelab/keelith/correlation"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/observability"
	"github.com/keelab/keelith/observability/logging"
	kresource "github.com/keelab/keelith/observability/resource"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/server"
	"github.com/keelab/keelith/service"
	kgrpc "github.com/keelab/keelith/transport/grpc"
	khttp "github.com/keelab/keelith/transport/http"
	"go.opentelemetry.io/otel/propagation"
)

func buildDefaults(settings options) (*observability.Bundle, *middleware.Bundle, *middleware.StreamBundle, error) {
	telemetry, err := observability.New(observability.Config{
		Resource:  kresource.Config{ServiceName: settings.name},
		LogOutput: settings.output,
		Logging: &logging.Config{
			Level:  "info",
			Format: logging.FormatText,
		},
		SensitiveKeys: []string{
			"authorization",
			"cookie",
			"set-cookie",
			"x-api-key",
		},
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build observability: %w", err)
	}
	requestLogger, err := logging.NewRequestLogger(
		telemetry.Logger().Slog(),
		logging.RequestLogConfig{},
	)
	if err != nil {
		return nil, nil, nil, closeBuildFailure(telemetry, fmt.Errorf("build request logger: %w", err))
	}
	serverBundle, err := middleware.NewServerBundle(middleware.ServerBundleConfig{
		Source:        "keelith/default",
		Observability: telemetry.ServerMiddleware(),
		AdditionalEntries: []middleware.Entry{
			{
				Name:       "request-id",
				Source:     "keelith/default",
				Middleware: correlation.PropagateRequestID(),
			},
			{
				Name:       "request-log",
				Source:     "keelith/default",
				Middleware: requestLogger.Middleware(),
			},
		},
	})
	if err != nil {
		return nil, nil, nil, closeBuildFailure(telemetry, fmt.Errorf("build default middleware: %w", err))
	}
	streamBundle, err := middleware.NewStreamBundle(
		middleware.StreamEntry{
			Name:       "observability",
			Source:     "keelith/default",
			Middleware: telemetry.ServerStreamMiddleware(),
		},
		middleware.StreamEntry{
			Name:       "request-log",
			Source:     "keelith/default",
			Middleware: requestLogger.StreamMiddleware(),
		},
	)
	if err != nil {
		return nil, nil, nil, closeBuildFailure(telemetry, fmt.Errorf("build default stream middleware: %w", err))
	}
	return telemetry, serverBundle, streamBundle, nil
}

func buildConfigRuntime(settings options) (*config.Manager, *config.Runtime, error) {
	if settings.configManager != nil {
		runtime, err := config.NewRuntime(settings.configManager)
		if err != nil {
			return nil, nil, err
		}
		return settings.configManager, runtime, nil
	}
	if settings.configFile == nil {
		return nil, nil, nil
	}

	fileSettings := settings.configFile
	fileOptions := make([]configfile.Option, 0, 1)
	if fileSettings.pollInterval > 0 {
		fileOptions = append(
			fileOptions,
			configfile.WithPollInterval(fileSettings.pollInterval),
		)
	}
	fileSource, err := configfile.New(fileSettings.path, fileOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("create file source %q: %w", fileSettings.path, err)
	}
	sources := []config.Source{fileSource}
	if fileSettings.envPrefix != "" {
		envSource, envErr := kenv.New(
			fileSettings.envPrefix,
			kenv.WithParser(kenv.JSONValueParser),
		)
		if envErr != nil {
			return nil, nil, fmt.Errorf("create environment source: %w", envErr)
		}
		sources = append(sources, envSource)
	}
	managerOptions := []config.Option{
		config.WithSources(sources...),
		config.WithBindings(fileSettings.bindings...),
	}
	if fileSettings.rejectUnknown {
		managerOptions = append(
			managerOptions,
			config.WithUnknownFieldPolicy(config.UnknownReject),
			config.WithKnownFields(fileSettings.knownFields...),
		)
	}
	manager, err := config.New(managerOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("create config manager: %w", err)
	}
	runtime, err := config.NewRuntime(manager)
	if err != nil {
		return nil, nil, fmt.Errorf("create config runtime: %w", err)
	}
	return manager, runtime, nil
}

func newConfigDescription(
	settings options,
	runtime *config.Runtime,
) *ConfigDescription {
	if settings.configFile == nil && settings.configManager == nil {
		return nil
	}
	description := &ConfigDescription{Enabled: true}
	if settings.configFile != nil {
		description.Path = settings.configFile.path
		description.EnvironmentPrefix = settings.configFile.envPrefix
		description.Strict = settings.configFile.rejectUnknown
	}
	if runtime != nil {
		description.Runtime = runtime.Description()
	}
	return description
}

func routeDescriptions(routes []route) []RouteDescription {
	if len(routes) == 0 {
		return nil
	}
	result := make([]RouteDescription, len(routes))
	for index, item := range routes {
		result[index] = RouteDescription{Method: item.method, Path: item.path}
	}
	return result
}

func serverNames(servers []server.Server) []string {
	if len(servers) == 0 {
		return nil
	}
	result := make([]string, 0, len(servers))
	for _, item := range servers {
		if named, ok := item.(server.Named); ok && named.Name() != "" {
			result = append(result, named.Name())
			continue
		}
		result = append(result, fmt.Sprintf("%T", item))
	}
	return result
}

func componentNames(components []kapp.Component) []string {
	if len(components) == 0 {
		return nil
	}
	result := make([]string, 0, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		result = append(result, component.Name())
	}
	return result
}

func buildHTTPServer(
	profile *service.Profile,
	routes []route,
	listenerName string,
	address string,
	healthRegistry *health.Registry,
	policy *metadata.Policy,
	propagator propagation.TextMapPropagator,
	bundle *middleware.Bundle,
) (*khttp.Server, error) {
	var surface *service.Surface
	composed := bundle
	var err error
	if profile != nil {
		surface, err = profile.HTTP(listenerName)
		if err != nil {
			return nil, fmt.Errorf("build HTTP surface: %w", err)
		}
		composed, err = surface.Compose(bundle)
		if err != nil {
			return nil, fmt.Errorf("compose HTTP surface: %w", err)
		}
	}
	router, err := khttp.NewRouter(
		khttp.WithMiddleware(composed),
		khttp.WithMetadataPolicy(*policy),
		khttp.WithPropagator(propagator),
	)
	if err != nil {
		return nil, fmt.Errorf("build HTTP router: %w", err)
	}
	if surface != nil {
		if err := surface.RegisterHTTP(router); err != nil {
			return nil, fmt.Errorf("register HTTP services: %w", err)
		}
	}
	for index, item := range routes {
		target, operationErr := operation.New(
			"http",
			"keelith.quickstart",
			item.method+" "+item.path,
			operation.KindUnary,
		)
		if operationErr != nil {
			return nil, fmt.Errorf(
				"build route %d operation %s %s: %w",
				index,
				item.method,
				item.path,
				operationErr,
			)
		}
		routeHandler := item.handler
		if err := router.Handle(
			item.method,
			item.path,
			target,
			func(request *nethttp.Request) (any, error) { return request, nil },
			func(ctx context.Context, request any) (any, error) {
				httpRequest, ok := request.(*nethttp.Request)
				if !ok {
					return nil, fmt.Errorf("route %d: unexpected request type %T", index, request)
				}
				return routeHandler(ctx, httpRequest)
			},
			khttp.EncodeJSON,
		); err != nil {
			return nil, fmt.Errorf("register HTTP route %d: %w", index, err)
		}
	}
	server, err := khttp.NewServer(
		router,
		khttp.WithAddress(address),
		khttp.WithName(listenerName),
		khttp.WithHealth(healthRegistry),
	)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func buildGRPCServer(
	profile *service.Profile,
	listenerName string,
	address string,
	healthRegistry *health.Registry,
	policy *metadata.Policy,
	propagator propagation.TextMapPropagator,
	bundle *middleware.Bundle,
	streamBundle *middleware.StreamBundle,
) (*kgrpc.Server, error) {
	surface, err := profile.GRPC(listenerName)
	if err != nil {
		return nil, fmt.Errorf("build gRPC surface: %w", err)
	}
	composed, err := surface.Compose(bundle)
	if err != nil {
		return nil, fmt.Errorf("compose gRPC surface: %w", err)
	}
	server, err := kgrpc.NewServer(
		kgrpc.WithAddress(address),
		kgrpc.WithName(listenerName),
		kgrpc.WithMiddleware(composed),
		kgrpc.WithStreamMiddleware(streamBundle),
		kgrpc.WithMetadataPolicy(*policy),
		kgrpc.WithPropagator(propagator),
		kgrpc.WithHealth(healthRegistry),
	)
	if err != nil {
		return nil, err
	}
	if err := surface.RegisterGRPC(server.Registrar()); err != nil {
		return nil, fmt.Errorf("register gRPC services: %w", err)
	}
	return server, nil
}

func closeBuildFailure(telemetry *observability.Bundle, err error) error {
	if telemetry == nil {
		return err
	}
	return errors.Join(err, telemetry.Shutdown(context.Background()))
}
