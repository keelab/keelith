package otlpgrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	kresource "github.com/keelab/keelith/observability/resource"
	"github.com/keelab/keelith/secret"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
)

const (
	rotationRetryMinimum = 100 * time.Millisecond
	rotationRetryMaximum = 10 * time.Second
)

// State is a bounded lifecycle state suitable for diagnostics.
type State string

const (
	// StateCreated means the pipeline has not opened exporters.
	StateCreated State = "created"
	// StateStarting means startup is opening exporters.
	StateStarting State = "starting"
	// StateRunning means the pipeline is active.
	StateRunning State = "running"
	// StateStopping means shutdown has started.
	StateStopping State = "stopping"
	// StateStopped means shutdown completed normally.
	StateStopped State = "stopped"
	// StateFailed means startup or shutdown failed.
	StateFailed State = "failed"
)

// Description is a value-free operational snapshot. It intentionally excludes
// endpoint, headers, credentials, and exporter error text.
type Description struct {
	State                  State  `json:"state"`
	MetricExports          uint64 `json:"metric_exports"`
	MetricFailures         uint64 `json:"metric_failures"`
	LastMetricExportFailed bool   `json:"last_metric_export_failed"`
	LogExports             uint64 `json:"log_exports"`
	LogRecords             uint64 `json:"log_records"`
	LogFailures            uint64 `json:"log_failures"`
	LastLogExportFailed    bool   `json:"last_log_export_failed"`
	RotationEnabled        bool   `json:"rotation_enabled"`
	Rotating               bool   `json:"rotating"`
	Rotations              uint64 `json:"rotations"`
	RotationFailures       uint64 `json:"rotation_failures"`
	LastRotationFailed     bool   `json:"last_rotation_failed"`
}

// Pipeline owns one active OTLP exporter generation and one manual metric
// reader for an App instance. TLS update notifications build and probe a
// candidate generation before atomically replacing the active generation. The
// tracing and logging SDKs retain ownership of final exporter shutdown.
type Pipeline struct {
	config         normalizedConfig
	traceExporter  *swappableTraceExporter
	traceProcessor sdktrace.SpanProcessor
	processorOnce  sync.Once
	metricReader   *sdkmetric.ManualReader
	logExporter    *countingLogExporter

	logMu       sync.Mutex
	logProvider *sdklog.LoggerProvider
	logHandler  slog.Handler

	mu             sync.Mutex
	state          State
	startErr       error
	shutdownErr    error
	metricExporter *otlpmetricgrpc.Exporter
	metricExportMu sync.Mutex
	cancel         context.CancelFunc
	metricDone     chan struct{}
	rotationDone   chan struct{}
	subscriptions  []secret.UpdateSubscription
	shutdownDone   chan struct{}

	metricExports      atomic.Uint64
	metricFailures     atomic.Uint64
	lastFailed         atomic.Bool
	rotating           atomic.Bool
	rotations          atomic.Uint64
	rotationFailures   atomic.Uint64
	lastRotationFailed atomic.Bool
}

// New validates config and builds a disconnected pipeline. It does not dial the
// collector and does not mutate OpenTelemetry globals.
func New(config Config) (*Pipeline, error) {
	normalized, err := normalize(config)
	if err != nil {
		return nil, err
	}
	traceExporter := &swappableTraceExporter{}
	var logExporter *countingLogExporter
	if normalized.logsEnabled {
		logExporter = &countingLogExporter{}
	}
	return &Pipeline{
		config:        normalized,
		traceExporter: traceExporter,
		metricReader:  sdkmetric.NewManualReader(),
		logExporter:   logExporter,
		state:         StateCreated,
		shutdownDone:  make(chan struct{}),
	}, nil
}

// LogHandler creates the App-scoped OpenTelemetry slog bridge. The returned
// handler does not install a global provider and must be wrapped by Keelith's
// ContextHandler so redaction and trace correlation happen before export.
func (pipeline *Pipeline) LogHandler(resource *kresource.Resource) (slog.Handler, error) {
	if pipeline == nil {
		return nil, fmt.Errorf("%w: pipeline is nil", ErrInvalidState)
	}
	if resource == nil {
		return nil, fmt.Errorf("%w: log resource is nil", ErrInvalidConfig)
	}
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	if pipeline.state != StateCreated {
		return nil, fmt.Errorf(
			"%w: cannot attach logs from %s",
			ErrInvalidState,
			pipeline.state,
		)
	}
	if !pipeline.config.logsEnabled || pipeline.logExporter == nil {
		return nil, fmt.Errorf("%w: logs are not enabled", ErrInvalidConfig)
	}
	pipeline.logMu.Lock()
	defer pipeline.logMu.Unlock()
	if pipeline.logHandler != nil {
		return pipeline.logHandler, nil
	}
	processor := sdklog.NewBatchProcessor(
		pipeline.logExporter,
		sdklog.WithMaxQueueSize(pipeline.config.logQueueSize),
		sdklog.WithExportInterval(pipeline.config.logExportInterval),
		sdklog.WithExportTimeout(pipeline.config.timeout),
		sdklog.WithExportMaxBatchSize(pipeline.config.logBatchSize),
	)
	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(resource.OTel()),
		sdklog.WithProcessor(processor),
	)
	pipeline.logProvider = provider
	pipeline.logHandler = otelslog.NewHandler(
		"github.com/keelab/keelith",
		otelslog.WithLoggerProvider(provider),
	)
	return pipeline.logHandler, nil
}

// SpanExporter returns the exporter to pass to observability.Config.
func (pipeline *Pipeline) SpanExporter() sdktrace.SpanExporter {
	if pipeline == nil {
		return nil
	}
	return pipeline.traceExporter
}

// SpanProcessor returns the bounded production batch processor to pass to
// observability.Config. The tracing provider owns its flush and shutdown.
func (pipeline *Pipeline) SpanProcessor() sdktrace.SpanProcessor {
	if pipeline == nil {
		return nil
	}
	pipeline.processorOnce.Do(func() {
		pipeline.traceProcessor = sdktrace.NewBatchSpanProcessor(
			pipeline.traceExporter,
			sdktrace.WithBatchTimeout(pipeline.config.traceBatchTimeout),
			sdktrace.WithExportTimeout(pipeline.config.timeout),
			sdktrace.WithMaxQueueSize(pipeline.config.traceQueueSize),
			sdktrace.WithMaxExportBatchSize(pipeline.config.traceBatchSize),
		)
	})
	return pipeline.traceProcessor
}

// MetricReader returns the reader to pass to observability.Config.
func (pipeline *Pipeline) MetricReader() sdkmetric.Reader {
	if pipeline == nil {
		return nil
	}
	return pipeline.metricReader
}

// Start builds and verifies the initial OTLP exporter generation, subscribes to
// configured TLS update sources, and starts metric export and rotation loops.
// Use it as observability.Config.Initializer so readiness failure participates
// in App startup rollback.
func (pipeline *Pipeline) Start(ctx context.Context) error {
	if pipeline == nil {
		return fmt.Errorf("%w: pipeline is nil", ErrInvalidState)
	}
	if ctx == nil {
		return fmt.Errorf("%w: start context is nil", ErrInvalidState)
	}
	pipeline.mu.Lock()
	switch pipeline.state {
	case StateRunning:
		pipeline.mu.Unlock()
		return nil
	case StateFailed:
		err := pipeline.startErr
		pipeline.mu.Unlock()
		return err
	case StateCreated:
		pipeline.state = StateStarting
	default:
		state := pipeline.state
		pipeline.mu.Unlock()
		return fmt.Errorf("%w: cannot start from %s", ErrInvalidState, state)
	}
	pipeline.mu.Unlock()
	if pipeline.config.logsEnabled && !pipeline.logsReady() {
		return pipeline.failStart(fmt.Errorf(
			"%w: logs are enabled but LogHandler was not installed",
			ErrInvalidState,
		))
	}

	subscriptions, err := pipeline.subscribeUpdateSources()
	if err != nil {
		return pipeline.failStart(err)
	}
	startContext, startCancel := context.WithTimeout(ctx, pipeline.config.timeout)
	generation, err := pipeline.newGeneration(startContext)
	startCancel()
	if err != nil {
		closeSubscriptions(subscriptions)
		return pipeline.failStart(err)
	}

	loopContext, loopCancel := context.WithCancel(context.Background())
	metricDone := make(chan struct{})
	rotationDone := make(chan struct{})
	pipeline.installGeneration(generation)
	pipeline.mu.Lock()
	pipeline.cancel = loopCancel
	pipeline.metricDone = metricDone
	pipeline.rotationDone = rotationDone
	pipeline.subscriptions = subscriptions
	pipeline.state = StateRunning
	pipeline.mu.Unlock()
	go pipeline.run(loopContext, metricDone)
	if len(subscriptions) == 0 {
		close(rotationDone)
	} else {
		go pipeline.runRotation(loopContext, rotationDone, subscriptions)
	}
	return nil
}

// Shutdown stops rotation before final collection, closes subscriptions, and
// flushes the active metric and log exporters. The tracing SDK subsequently
// closes the active trace exporter through the swappable facade.
func (pipeline *Pipeline) Shutdown(ctx context.Context) error {
	if pipeline == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is nil", ErrInvalidState)
	}
	pipeline.mu.Lock()
	switch pipeline.state {
	case StateStopped:
		err := pipeline.shutdownErr
		pipeline.mu.Unlock()
		return err
	case StateCreated, StateFailed:
		pipeline.state = StateStopping
	case StateRunning:
		pipeline.state = StateStopping
	case StateStarting:
		pipeline.mu.Unlock()
		return fmt.Errorf(
			"%w: cannot shut down while starting",
			ErrInvalidState,
		)
	case StateStopping:
		done := pipeline.shutdownDone
		pipeline.mu.Unlock()
		<-done
		pipeline.mu.Lock()
		err := pipeline.shutdownErr
		pipeline.mu.Unlock()
		return err
	default:
		state := pipeline.state
		pipeline.mu.Unlock()
		return fmt.Errorf(
			"%w: cannot shut down from %s",
			ErrInvalidState,
			state,
		)
	}
	cancel := pipeline.cancel
	metricDone := pipeline.metricDone
	rotationDone := pipeline.rotationDone
	subscriptions := pipeline.subscriptions
	pipeline.mu.Unlock()

	closeSubscriptions(subscriptions)
	if cancel != nil {
		cancel()
	}
	if rotationDone != nil {
		<-rotationDone
	}
	if metricDone != nil {
		<-metricDone
	}
	var exportErr error
	if ctx.Err() == nil {
		exportErr = pipeline.exportOnce(ctx)
	}
	flushErr, shutdownErr := pipeline.shutdownMetric(ctx)
	logErr := pipeline.shutdownLogs(ctx)
	result := errors.Join(exportErr, flushErr, shutdownErr, logErr)
	pipeline.mu.Lock()
	pipeline.cancel = nil
	pipeline.metricDone = nil
	pipeline.rotationDone = nil
	pipeline.subscriptions = nil
	pipeline.state = StateStopped
	pipeline.shutdownErr = result
	close(pipeline.shutdownDone)
	pipeline.mu.Unlock()
	return result
}

// Description returns a secret-free lifecycle and export snapshot.
func (pipeline *Pipeline) Description() Description {
	if pipeline == nil {
		return Description{State: StateStopped}
	}
	pipeline.mu.Lock()
	state := pipeline.state
	pipeline.mu.Unlock()
	return Description{
		State:                  state,
		MetricExports:          pipeline.metricExports.Load(),
		MetricFailures:         pipeline.metricFailures.Load(),
		LastMetricExportFailed: pipeline.lastFailed.Load(),
		LogExports:             pipeline.logExports(),
		LogRecords:             pipeline.logRecords(),
		LogFailures:            pipeline.logFailures(),
		LastLogExportFailed:    pipeline.lastLogFailed(),
		RotationEnabled:        len(pipeline.config.updateSources) > 0,
		Rotating:               pipeline.rotating.Load(),
		Rotations:              pipeline.rotations.Load(),
		RotationFailures:       pipeline.rotationFailures.Load(),
		LastRotationFailed:     pipeline.lastRotationFailed.Load(),
	}
}

func (pipeline *Pipeline) logsReady() bool {
	pipeline.logMu.Lock()
	defer pipeline.logMu.Unlock()
	return pipeline.logProvider != nil && pipeline.logHandler != nil
}

func (pipeline *Pipeline) shutdownLogs(ctx context.Context) error {
	pipeline.logMu.Lock()
	provider := pipeline.logProvider
	pipeline.logProvider = nil
	pipeline.logHandler = nil
	pipeline.logMu.Unlock()
	if provider == nil {
		if pipeline.logExporter != nil {
			return pipeline.logExporter.Shutdown(ctx)
		}
		return nil
	}
	return errors.Join(provider.ForceFlush(ctx), provider.Shutdown(ctx))
}

func (pipeline *Pipeline) logExports() uint64 {
	if pipeline.logExporter == nil {
		return 0
	}
	return pipeline.logExporter.exports.Load()
}

func (pipeline *Pipeline) logRecords() uint64 {
	if pipeline.logExporter == nil {
		return 0
	}
	return pipeline.logExporter.records.Load()
}

func (pipeline *Pipeline) logFailures() uint64 {
	if pipeline.logExporter == nil {
		return 0
	}
	return pipeline.logExporter.failures.Load()
}

func (pipeline *Pipeline) lastLogFailed() bool {
	return pipeline.logExporter != nil && pipeline.logExporter.lastFailed.Load()
}

func (pipeline *Pipeline) run(
	ctx context.Context,
	done chan<- struct{},
) {
	defer close(done)
	ticker := time.NewTicker(pipeline.config.metricInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			exportContext, cancel := context.WithTimeout(
				ctx,
				pipeline.config.metricExportTimeout,
			)
			_ = pipeline.exportOnce(exportContext)
			cancel()
		}
	}
}

func (pipeline *Pipeline) exportOnce(ctx context.Context) error {
	pipeline.metricExportMu.Lock()
	defer pipeline.metricExportMu.Unlock()
	pipeline.mu.Lock()
	exporter := pipeline.metricExporter
	pipeline.mu.Unlock()
	if exporter == nil {
		return nil
	}
	var data metricdata.ResourceMetrics
	if err := pipeline.metricReader.Collect(ctx, &data); err != nil {
		pipeline.recordMetricFailure()
		return fmt.Errorf("otlpgrpc: collect metrics: %w", err)
	}
	if err := exporter.Export(ctx, &data); err != nil {
		pipeline.recordMetricFailure()
		return fmt.Errorf("otlpgrpc: export metrics: %w", err)
	}
	pipeline.metricExports.Add(1)
	pipeline.lastFailed.Store(false)
	return nil
}

func (pipeline *Pipeline) recordMetricFailure() {
	pipeline.metricFailures.Add(1)
	pipeline.lastFailed.Store(true)
}

func (pipeline *Pipeline) subscribeUpdateSources() (
	[]secret.UpdateSubscription,
	error,
) {
	if len(pipeline.config.updateSources) == 0 {
		return nil, nil
	}
	subscriptions := make(
		[]secret.UpdateSubscription,
		0,
		len(pipeline.config.updateSources),
	)
	for _, source := range pipeline.config.updateSources {
		if !source.Ready() {
			closeSubscriptions(subscriptions)
			return nil, fmt.Errorf(
				"%w: exporter update source is not ready",
				ErrInvalidState,
			)
		}
		subscription, err := source.SubscribeUpdates()
		if err != nil {
			closeSubscriptions(subscriptions)
			return nil, fmt.Errorf(
				"%w: subscribe to exporter updates",
				ErrInvalidState,
			)
		}
		if subscription == nil {
			closeSubscriptions(subscriptions)
			return nil, fmt.Errorf(
				"%w: update subscription is nil",
				ErrInvalidState,
			)
		}
		subscriptions = append(subscriptions, subscription)
	}
	return subscriptions, nil
}

func (pipeline *Pipeline) newGeneration(
	ctx context.Context,
) (*exporterGeneration, error) {
	generation := &exporterGeneration{}
	traceExporter := otlptracegrpc.NewUnstarted(
		traceOptions(pipeline.config)...,
	)
	if err := traceExporter.Start(ctx); err != nil {
		return nil, fmt.Errorf("otlpgrpc: start trace exporter: %w", err)
	}
	generation.trace = traceExporter

	metricExporter, err := otlpmetricgrpc.New(
		ctx,
		metricOptions(pipeline.config)...,
	)
	if err != nil {
		pipeline.discardGeneration(generation)
		return nil, fmt.Errorf("otlpgrpc: start metric exporter: %w", err)
	}
	generation.metric = metricExporter
	if pipeline.config.logsEnabled {
		logExporter, logErr := otlploggrpc.New(
			ctx,
			logOptions(pipeline.config)...,
		)
		if logErr != nil {
			pipeline.discardGeneration(generation)
			return nil, fmt.Errorf("otlpgrpc: start log exporter: %w", logErr)
		}
		generation.logs = logExporter
	}
	probe := metricdata.ResourceMetrics{Resource: sdkresource.Empty()}
	if err := metricExporter.Export(ctx, &probe); err != nil {
		pipeline.discardGeneration(generation)
		return nil, fmt.Errorf("otlpgrpc: verify collector: %w", err)
	}
	return generation, nil
}

func (pipeline *Pipeline) installGeneration(
	generation *exporterGeneration,
) *exporterGeneration {
	pipeline.metricExportMu.Lock()
	defer pipeline.metricExportMu.Unlock()
	retired := &exporterGeneration{}
	retired.trace = pipeline.traceExporter.swap(generation.trace)
	if pipeline.logExporter != nil {
		retired.logs = pipeline.logExporter.swap(generation.logs)
	}
	pipeline.mu.Lock()
	retired.metric = pipeline.metricExporter
	pipeline.metricExporter = generation.metric
	pipeline.mu.Unlock()
	return retired
}

func (pipeline *Pipeline) runRotation(
	ctx context.Context,
	done chan<- struct{},
	subscriptions []secret.UpdateSubscription,
) {
	defer close(done)
	signals := make(chan struct{}, 1)
	var forwarders sync.WaitGroup
	for _, subscription := range subscriptions {
		forwarders.Add(1)
		go forwardUpdateSignals(ctx, &forwarders, signals, subscription)
	}
	defer forwarders.Wait()
	for {
		select {
		case <-ctx.Done():
			return
		case <-signals:
			pipeline.rotateUntilReady(ctx, signals)
		}
	}
}

func (pipeline *Pipeline) rotateUntilReady(
	ctx context.Context,
	signals <-chan struct{},
) {
	pipeline.rotating.Store(true)
	defer pipeline.rotating.Store(false)
	retryDelay := rotationRetryMinimum
	for {
		rotationContext, cancel := context.WithTimeout(
			ctx,
			pipeline.config.rotationReadyTimeout,
		)
		generation, err := pipeline.newGeneration(rotationContext)
		cancel()
		if err == nil {
			retired := pipeline.installGeneration(generation)
			pipeline.rotations.Add(1)
			pipeline.lastRotationFailed.Store(false)
			pipeline.discardGeneration(retired)
			return
		}
		if context.Cause(ctx) != nil {
			return
		}
		pipeline.rotationFailures.Add(1)
		pipeline.lastRotationFailed.Store(true)
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-signals:
			stopTimer(timer)
		case <-timer.C:
		}
		if retryDelay < rotationRetryMaximum {
			retryDelay *= 2
			if retryDelay > rotationRetryMaximum {
				retryDelay = rotationRetryMaximum
			}
		}
	}
}

func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (pipeline *Pipeline) discardGeneration(generation *exporterGeneration) {
	if generation == nil {
		return
	}
	timeout := pipeline.config.rotationReadyTimeout
	if timeout == 0 {
		timeout = pipeline.config.timeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = shutdownGeneration(ctx, generation)
}

func (pipeline *Pipeline) shutdownMetric(
	ctx context.Context,
) (error, error) {
	pipeline.metricExportMu.Lock()
	defer pipeline.metricExportMu.Unlock()
	pipeline.mu.Lock()
	exporter := pipeline.metricExporter
	pipeline.metricExporter = nil
	pipeline.mu.Unlock()
	if exporter == nil {
		return nil, nil
	}
	return exporter.ForceFlush(ctx), exporter.Shutdown(ctx)
}

func (pipeline *Pipeline) failStart(err error) error {
	pipeline.mu.Lock()
	pipeline.startErr = err
	pipeline.state = StateFailed
	pipeline.mu.Unlock()
	return err
}

func traceOptions(config normalizedConfig) []otlptracegrpc.Option {
	options := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(config.endpoint),
		otlptracegrpc.WithTimeout(config.timeout),
	}
	if len(config.headers) > 0 {
		options = append(options, otlptracegrpc.WithHeaders(config.headers))
	}
	if config.perRPCCredentials != nil {
		options = append(options, otlptracegrpc.WithDialOption(
			grpc.WithPerRPCCredentials(config.perRPCCredentials),
		))
	}
	if config.insecure {
		options = append(options, otlptracegrpc.WithInsecure())
	} else {
		options = append(
			options,
			otlptracegrpc.WithTLSCredentials(credentials.NewTLS(
				cloneTLS(config.tlsConfig),
			)),
		)
	}
	if config.compression {
		options = append(
			options,
			otlptracegrpc.WithCompressor(grpcgzip.Name),
		)
	}
	return options
}

func metricOptions(config normalizedConfig) []otlpmetricgrpc.Option {
	options := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(config.endpoint),
		otlpmetricgrpc.WithTimeout(config.timeout),
	}
	if len(config.headers) > 0 {
		options = append(options, otlpmetricgrpc.WithHeaders(config.headers))
	}
	if config.perRPCCredentials != nil {
		options = append(options, otlpmetricgrpc.WithDialOption(
			grpc.WithPerRPCCredentials(config.perRPCCredentials),
		))
	}
	if config.insecure {
		options = append(options, otlpmetricgrpc.WithInsecure())
	} else {
		options = append(
			options,
			otlpmetricgrpc.WithTLSCredentials(credentials.NewTLS(
				cloneTLS(config.tlsConfig),
			)),
		)
	}
	if config.compression {
		options = append(
			options,
			otlpmetricgrpc.WithCompressor(grpcgzip.Name),
		)
	}
	return options
}

func logOptions(config normalizedConfig) []otlploggrpc.Option {
	options := []otlploggrpc.Option{
		otlploggrpc.WithEndpoint(config.endpoint),
		otlploggrpc.WithTimeout(config.timeout),
		otlploggrpc.WithMaxRequestSize(config.logMaxRequestBytes),
	}
	if len(config.headers) > 0 {
		options = append(options, otlploggrpc.WithHeaders(config.headers))
	}
	if config.perRPCCredentials != nil {
		options = append(options, otlploggrpc.WithDialOption(
			grpc.WithPerRPCCredentials(config.perRPCCredentials),
		))
	}
	if config.insecure {
		options = append(options, otlploggrpc.WithInsecure())
	} else {
		options = append(
			options,
			otlploggrpc.WithTLSCredentials(credentials.NewTLS(
				cloneTLS(config.tlsConfig),
			)),
		)
	}
	if config.compression {
		options = append(options, otlploggrpc.WithCompressor(grpcgzip.Name))
	}
	return options
}

type exporterGeneration struct {
	trace  sdktrace.SpanExporter
	metric *otlpmetricgrpc.Exporter
	logs   sdklog.Exporter
}

func shutdownGeneration(
	ctx context.Context,
	generation *exporterGeneration,
) error {
	if generation == nil {
		return nil
	}
	var metricErr error
	if generation.metric != nil {
		metricErr = generation.metric.Shutdown(ctx)
	}
	var logErr error
	if generation.logs != nil {
		logErr = generation.logs.Shutdown(ctx)
	}
	var traceErr error
	if generation.trace != nil {
		traceErr = generation.trace.Shutdown(ctx)
	}
	return errors.Join(metricErr, logErr, traceErr)
}

func closeSubscriptions(subscriptions []secret.UpdateSubscription) {
	for _, subscription := range subscriptions {
		if subscription != nil {
			subscription.Close()
		}
	}
}

func forwardUpdateSignals(
	ctx context.Context,
	forwarders *sync.WaitGroup,
	signals chan<- struct{},
	subscription secret.UpdateSubscription,
) {
	defer forwarders.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case _, open := <-subscription.Updates():
			if !open {
				return
			}
			select {
			case signals <- struct{}{}:
			default:
			}
		}
	}
}

type swappableTraceExporter struct {
	mu       sync.RWMutex
	exporter sdktrace.SpanExporter
}

func (exporter *swappableTraceExporter) ExportSpans(
	ctx context.Context,
	spans []sdktrace.ReadOnlySpan,
) error {
	exporter.mu.RLock()
	defer exporter.mu.RUnlock()
	if exporter.exporter == nil {
		return ErrInvalidState
	}
	return exporter.exporter.ExportSpans(ctx, spans)
}

func (exporter *swappableTraceExporter) Shutdown(ctx context.Context) error {
	exporter.mu.Lock()
	current := exporter.exporter
	exporter.exporter = nil
	exporter.mu.Unlock()
	if current == nil {
		return nil
	}
	return current.Shutdown(ctx)
}

func (exporter *swappableTraceExporter) swap(
	next sdktrace.SpanExporter,
) sdktrace.SpanExporter {
	exporter.mu.Lock()
	current := exporter.exporter
	exporter.exporter = next
	exporter.mu.Unlock()
	return current
}

type countingLogExporter struct {
	mu         sync.RWMutex
	exporter   sdklog.Exporter
	exports    atomic.Uint64
	records    atomic.Uint64
	failures   atomic.Uint64
	lastFailed atomic.Bool
}

func (exporter *countingLogExporter) Export(
	ctx context.Context,
	records []sdklog.Record,
) error {
	exporter.mu.RLock()
	defer exporter.mu.RUnlock()
	if exporter.exporter == nil {
		exporter.failures.Add(1)
		exporter.lastFailed.Store(true)
		return ErrInvalidState
	}
	if err := exporter.exporter.Export(ctx, records); err != nil {
		exporter.failures.Add(1)
		exporter.lastFailed.Store(true)
		return err
	}
	exporter.exports.Add(1)
	exporter.records.Add(uint64(len(records)))
	exporter.lastFailed.Store(false)
	return nil
}

func (exporter *countingLogExporter) ForceFlush(ctx context.Context) error {
	exporter.mu.RLock()
	defer exporter.mu.RUnlock()
	if exporter.exporter == nil {
		return nil
	}
	return exporter.exporter.ForceFlush(ctx)
}

func (exporter *countingLogExporter) Shutdown(ctx context.Context) error {
	exporter.mu.Lock()
	current := exporter.exporter
	exporter.exporter = nil
	exporter.mu.Unlock()
	if current == nil {
		return nil
	}
	return current.Shutdown(ctx)
}

func (exporter *countingLogExporter) swap(
	next sdklog.Exporter,
) sdklog.Exporter {
	exporter.mu.Lock()
	current := exporter.exporter
	exporter.exporter = next
	exporter.mu.Unlock()
	return current
}

func cloneTLS(config *tls.Config) *tls.Config {
	if config == nil {
		return &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return config.Clone()
}
