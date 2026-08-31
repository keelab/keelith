package keelith

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	kapp "github.com/keelab/keelith/app"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/server"
	"github.com/keelab/keelith/service"
	kgrpc "github.com/keelab/keelith/transport/grpc"
	khttp "github.com/keelab/keelith/transport/http"
)

const (
	defaultName        = "keelith-app"
	defaultHTTPAddress = "127.0.0.1:8080"
	defaultStopTimeout = 30 * time.Second
)

var (
	// ErrInvalidOption reports an invalid high-level application option.
	ErrInvalidOption = errors.New("keelith: invalid option")
	// ErrNoListener reports an application without a configured listener.
	ErrNoListener = errors.New("keelith: no listener configured")
	// ErrNoService reports an application without a service binding or custom server.
	ErrNoService = errors.New("keelith: no service configured")
	// ErrClosed reports an application that was closed before it could run.
	ErrClosed = errors.New("keelith: application is closed")
)

// New builds a small, safe application from generated service bindings.
// External dependencies are never contacted by this constructor.
func New(optionList ...Option) (application *Application, err error) {
	settings := options{
		name:        defaultName,
		httpAddress: defaultHTTPAddress,
		output:      os.Stderr,
		stopTimeout: defaultStopTimeout,
	}
	buildComplete := false
	var graphCleanup *graphCloser
	var cleanupCleanup *cleanupCloser
	defer func() {
		if buildComplete {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			settings.stopTimeout,
		)
		var closeErr error
		if graphCleanup == nil {
			graphCleanup = newGraphCloser(settings.graph)
		}
		closeErr = graphCleanup.Close(cleanupCtx)
		if cleanupCleanup == nil {
			cleanupCleanup = newCleanupCloser(settings.cleanups)
		}
		closeErr = errors.Join(closeErr, cleanupCleanup.Close(cleanupCtx))
		cleanupCancel()
		if closeErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("close dependency graph after build failure: %w", closeErr),
			)
		}
	}()
	for index, option := range optionList {
		if isNilValue(option) {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	if !settings.httpSet && !settings.grpcSet && len(settings.servers) == 0 && !settings.opsSet {
		return nil, ErrNoListener
	}
	if settings.profile != nil && len(settings.bindings) > 0 {
		return nil, fmt.Errorf("%w: profile and services are mutually exclusive", ErrInvalidOption)
	}
	if settings.profile == nil && len(settings.bindings) == 0 &&
		len(settings.routes) == 0 && len(settings.servers) == 0 && !settings.opsSet {
		return nil, ErrNoService
	}
	graphCleanup = newGraphCloser(settings.graph)
	cleanupCleanup = newCleanupCloser(settings.cleanups)
	configManager, configRuntime, err := buildConfigRuntime(settings)
	if err != nil {
		return nil, fmt.Errorf("build config runtime: %w", err)
	}

	healthRegistry := settings.health
	if healthRegistry == nil {
		healthRegistry = health.NewRegistry()
	}
	policy := settings.metadataPolicy
	if policy == nil {
		defaultPolicy, err := metadata.NewPolicy(
			[]string{"x-request-id", "x-idempotency-key"},
		)
		if err != nil {
			return nil, fmt.Errorf("build default metadata policy: %w", err)
		}
		policy = &defaultPolicy
	}

	profile := settings.profile
	if profile == nil && len(settings.bindings) > 0 {
		var err error
		profile, err = service.NewProfileFromBindings(settings.name, settings.bindings...)
		if err != nil {
			return nil, fmt.Errorf("build default service profile: %w", err)
		}
	}

	telemetry, defaultBundle, defaultStreamBundle, err := buildDefaults(settings)
	if err != nil {
		return nil, err
	}
	serverBundle := defaultBundle
	if settings.serverBundle != nil {
		serverBundle = settings.serverBundle
	}
	streamBundle := settings.streamBundle
	if streamBundle == nil {
		streamBundle = defaultStreamBundle
	}
	propagator := settings.propagator
	if propagator == nil {
		propagator = telemetry.Propagator()
	}

	servers := make([]server.Server, 0, len(settings.servers)+3)
	if configRuntime != nil {
		servers = append(servers, configRuntime)
	}
	servers = append(servers, settings.servers...)
	var httpServer *khttp.Server
	var grpcServer *kgrpc.Server
	var opsServer *ops.Server
	if settings.opsSet {
		opsServer, err = ops.New(healthRegistry, settings.opsOptions...)
		if err != nil {
			return nil, closeBuildFailure(telemetry, fmt.Errorf("build ops server: %w", err))
		}
		servers = append(servers, opsServer)
	}
	if settings.httpSet {
		if profile == nil && len(settings.routes) == 0 {
			return nil, closeBuildFailure(
				telemetry,
				fmt.Errorf("%w: HTTP listener requires a service binding or route", ErrNoService),
			)
		}
		httpServer, err = buildHTTPServer(
			profile,
			settings.routes,
			settings.name+"-http",
			settings.httpAddress,
			healthRegistry,
			policy,
			propagator,
			serverBundle,
		)
		if err != nil {
			return nil, closeBuildFailure(telemetry, fmt.Errorf("build HTTP server: %w", err))
		}
		servers = append(servers, httpServer)
	}
	if settings.grpcSet {
		if profile == nil {
			return nil, closeBuildFailure(
				telemetry,
				fmt.Errorf("%w: gRPC listener requires generated service bindings", ErrNoService),
			)
		}
		grpcServer, err = buildGRPCServer(
			profile,
			settings.name+"-grpc",
			settings.grpcAddress,
			healthRegistry,
			policy,
			propagator,
			serverBundle,
			streamBundle,
		)
		if err != nil {
			return nil, closeBuildFailure(telemetry, fmt.Errorf("build gRPC server: %w", err))
		}
		servers = append(servers, grpcServer)
	}
	if len(servers) == 0 {
		return nil, closeBuildFailure(telemetry, ErrNoListener)
	}

	runStart := newStartGate()
	appOptions := []kapp.Option{
		kapp.WithHealth(healthRegistry),
		kapp.WithHooks(kapp.Hook{BeforeStart: runStart.signal}),
		kapp.WithLifecycles(telemetry),
	}
	if graphCleanup != nil {
		appOptions = append(
			appOptions,
			kapp.WithHooks(kapp.Hook{AfterStop: graphCleanup.Close}),
		)
	}
	if cleanupCleanup != nil {
		appOptions = append(appOptions, kapp.WithHooks(kapp.Hook{AfterStop: cleanupCleanup.Close}))
	}
	appOptions = append(
		appOptions,
		kapp.WithComponents(settings.components...),
		kapp.WithServers(servers...),
		kapp.WithStopTimeout(settings.stopTimeout),
	)
	runtimeApplication, err := kapp.New(appOptions...)
	if err != nil {
		return nil, closeBuildFailure(telemetry, fmt.Errorf("build application: %w", err))
	}
	application = &Application{
		app:         runtimeApplication,
		name:        settings.name,
		profile:     profile,
		health:      healthRegistry,
		telemetry:   telemetry,
		graph:       settings.graph,
		config:      configManager,
		configRun:   configRuntime,
		http:        httpServer,
		grpc:        grpcServer,
		ops:         opsServer,
		routes:      routeDescriptions(settings.routes),
		servers:     serverNames(servers),
		components:  componentNames(settings.components),
		configDesc:  newConfigDescription(settings, configRuntime),
		httpAddress: settings.httpAddress,
		grpcAddress: settings.grpcAddress,
		stopTimeout: settings.stopTimeout,
		runStart:    runStart,
		graphClose:  graphCleanup,
		cleanup:     cleanupCleanup,
	}
	buildComplete = true
	return application, nil
}
