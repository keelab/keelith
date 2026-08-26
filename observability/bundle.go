// Package observability composes one App-scoped log/trace/metric pipeline.
package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/observability/logging/audit"
	"github.com/keelab/keelith/observability/metrics"
	kresource "github.com/keelab/keelith/observability/resource"
	"github.com/keelab/keelith/observability/tracing"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// ErrInvalidOption reports an invalid Bundle dependency or configuration.
var ErrInvalidOption = errors.New("observability: invalid option")

const defaultMaxStreamSpanEvents = 64

// MetricReader is an OpenTelemetry SDK Reader.
type MetricReader = metrics.Reader

// Config constructs an isolated Bundle.
type Config struct {
	Resource                kresource.Config
	LogHandler              slog.Handler
	LoggingController       *logging.Controller
	LogOutput               io.Writer
	Logging                 *logging.Config
	AuditHandler            slog.Handler
	AuditPolicy             audit.Policy
	SensitiveKeys           []string
	SpanExporter            sdktrace.SpanExporter
	SpanProcessor           sdktrace.SpanProcessor
	MetricReaders           []MetricReader
	Propagator              propagation.TextMapPropagator
	MaxStreamSpanEvents     int
	DisableStreamSpanEvents bool

	// Initializer starts external exporters/connections. It runs inside the App
	// lifecycle so a failure participates in normal rollback.
	Initializer func(context.Context) error

	// Finalizer stops external exporter loops and connections before the SDK
	// providers are flushed and shut down. It must tolerate partial startup.
	Finalizer func(context.Context) error
}

// Bundle owns all providers for one App.
type Bundle struct {
	resource   *kresource.Resource
	logger     *logging.Logger
	logging    *logging.Controller
	audit      *audit.Logger
	tracing    *tracing.Provider
	metrics    *metrics.Provider
	propagator propagation.TextMapPropagator

	server       middleware.Middleware
	client       middleware.Middleware
	serverStream middleware.StreamMiddleware
	clientStream middleware.StreamMiddleware

	initializer func(context.Context) error
	finalizer   func(context.Context) error
	startOnce   sync.Once
	startErr    error
	stopOnce    sync.Once
	stopErr     error
}

// New constructs a Bundle without touching OTel or slog globals.
func New(config Config) (*Bundle, error) {
	if config.MaxStreamSpanEvents < 0 {
		return nil, fmt.Errorf(
			"%w: max stream span events is negative",
			ErrInvalidOption,
		)
	}
	if config.DisableStreamSpanEvents &&
		config.MaxStreamSpanEvents > 0 {
		return nil, fmt.Errorf(
			"%w: stream span events are both disabled and configured",
			ErrInvalidOption,
		)
	}
	maxStreamSpanEvents := config.MaxStreamSpanEvents
	if maxStreamSpanEvents == 0 && !config.DisableStreamSpanEvents {
		maxStreamSpanEvents = defaultMaxStreamSpanEvents
	}
	resource, err := kresource.New(config.Resource)
	if err != nil {
		return nil, err
	}
	redacter, err := logging.NewRedacter(config.SensitiveKeys...)
	if err != nil {
		return nil, err
	}
	handler := config.LogHandler
	loggingController := config.LoggingController
	switch {
	case config.Logging != nil:
		if handler != nil || loggingController != nil {
			return nil, fmt.Errorf("%w: log handler and logging policy are mutually exclusive", ErrInvalidOption)
		}
		handler, loggingController, err = logging.NewHandler(config.LogOutput, *config.Logging)
		if err != nil {
			return nil, err
		}
	case config.LogOutput != nil:
		return nil, fmt.Errorf("%w: log output requires a logging policy", ErrInvalidOption)
	case loggingController != nil && handler == nil:
		return nil, fmt.Errorf("%w: logging controller requires a log handler", ErrInvalidOption)
	case handler == nil:
		handler = slog.DiscardHandler
	}
	logger, err := logging.New(handler, resource, redacter)
	if err != nil {
		return nil, err
	}
	var auditLogger *audit.Logger
	if config.AuditHandler != nil {
		auditBase, auditErr := logging.New(config.AuditHandler, resource, redacter)
		if auditErr != nil {
			return nil, auditErr
		}
		auditLogger, auditErr = audit.NewWithPolicy(auditBase.Slog(), config.AuditPolicy)
		if auditErr != nil {
			return nil, auditErr
		}
	}
	traceProvider, err := tracing.New(
		resource,
		config.SpanExporter,
		config.SpanProcessor,
	)
	if err != nil {
		return nil, err
	}
	metricProvider, err := metrics.New(resource, config.MetricReaders...)
	if err != nil {
		_ = traceProvider.Shutdown(context.Background())
		return nil, err
	}
	propagator := config.Propagator
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)
	}
	return &Bundle{
		resource:   resource,
		logger:     logger,
		logging:    loggingController,
		audit:      auditLogger,
		tracing:    traceProvider,
		metrics:    metricProvider,
		propagator: propagator,
		server: middleware.Chain(
			traceProvider.Middleware(tracing.DirectionServer),
			metricProvider.Middleware(metrics.DirectionServer),
		),
		client: middleware.Chain(
			traceProvider.Middleware(tracing.DirectionClient),
			metricProvider.Middleware(metrics.DirectionClient),
		),
		serverStream: middleware.ChainStream(
			traceProvider.StreamMiddleware(
				tracing.DirectionServer,
				maxStreamSpanEvents,
			),
			metricProvider.StreamMiddleware(metrics.DirectionServer),
		),
		clientStream: middleware.ChainStream(
			traceProvider.StreamMiddleware(
				tracing.DirectionClient,
				maxStreamSpanEvents,
			),
			metricProvider.StreamMiddleware(metrics.DirectionClient),
		),
		initializer: config.Initializer,
		finalizer:   config.Finalizer,
	}, nil
}

// LoggingController returns the live logging policy controller when the Bundle
// constructed its Handler from Config.Logging. Caller-provided handlers have no
// framework-owned level controller and return nil.
func (bundle *Bundle) LoggingController() *logging.Controller {
	if bundle == nil {
		return nil
	}
	return bundle.logging
}

// AuditLogger returns the dedicated audit pipeline when AuditHandler was set.
func (bundle *Bundle) AuditLogger() *audit.Logger {
	if bundle == nil {
		return nil
	}
	return bundle.audit
}

// Start initializes external telemetry components inside App rollback.
func (bundle *Bundle) Start(ctx context.Context) error {
	if bundle == nil {
		return fmt.Errorf("%w: bundle is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	bundle.startOnce.Do(func() {
		if bundle.initializer != nil {
			bundle.startErr = bundle.initializer(ctx)
		}
		if bundle.startErr == nil && bundle.audit != nil {
			bundle.startErr = bundle.audit.Start(ctx)
		}
		if bundle.startErr != nil {
			bundle.startErr = errors.Join(
				bundle.startErr,
				bundle.Shutdown(context.WithoutCancel(ctx)),
			)
		}
	})
	return bundle.startErr
}

// Shutdown flushes and closes providers exactly once.
func (bundle *Bundle) Shutdown(ctx context.Context) error {
	if bundle == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	bundle.stopOnce.Do(func() {
		bundle.stopErr = errors.Join(
			callFinalizer(ctx, bundle.finalizer),
			stopAudit(ctx, bundle.audit),
			stopLogging(bundle.logging),
			bundle.tracing.ForceFlush(ctx),
			bundle.metrics.ForceFlush(ctx),
			bundle.metrics.Shutdown(ctx),
			bundle.tracing.Shutdown(ctx),
		)
	})
	return bundle.stopErr
}

func stopLogging(controller *logging.Controller) error {
	controller.Shutdown()
	return nil
}

func stopAudit(ctx context.Context, logger *audit.Logger) error {
	if logger == nil {
		return nil
	}
	return logger.Stop(ctx)
}

func callFinalizer(
	ctx context.Context,
	finalizer func(context.Context) error,
) error {
	if finalizer == nil {
		return nil
	}
	return finalizer(ctx)
}

// Resource returns the shared immutable telemetry identity.
func (bundle *Bundle) Resource() *kresource.Resource {
	return bundle.resource
}

// Logger returns the instance caller-aware logger.
func (bundle *Bundle) Logger() *logging.Logger {
	return bundle.logger
}

// TracerProvider returns the instance OTel provider.
func (bundle *Bundle) TracerProvider() trace.TracerProvider {
	return bundle.tracing.TracerProvider()
}

// MeterProvider returns the instance OTel provider.
func (bundle *Bundle) MeterProvider() otelmetric.MeterProvider {
	return bundle.metrics.MeterProvider()
}

// Propagator returns the instance transport propagation policy.
func (bundle *Bundle) Propagator() propagation.TextMapPropagator {
	return bundle.propagator
}

// ServerMiddleware returns inbound instrumentation.
func (bundle *Bundle) ServerMiddleware() middleware.Middleware {
	return bundle.server
}

// ClientMiddleware returns outbound instrumentation.
func (bundle *Bundle) ClientMiddleware() middleware.Middleware {
	return bundle.client
}

// ServerStreamMiddleware returns inbound stream lifecycle instrumentation.
func (bundle *Bundle) ServerStreamMiddleware() middleware.StreamMiddleware {
	return bundle.serverStream
}

// ClientStreamMiddleware returns outbound stream lifecycle instrumentation.
func (bundle *Bundle) ClientStreamMiddleware() middleware.StreamMiddleware {
	return bundle.clientStream
}
