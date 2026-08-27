package gorm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	gormio "gorm.io/gorm"
)

const (
	instrumentationName = "github.com/keelab/contrib/data/gorm"
	pluginName          = "keelith:gorm:instrumentation"
	operationStateKey   = "keelith:gorm:operation-state"
)

var pluginInstallMu sync.Mutex

// Option configures optional instance telemetry.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (function optionFunc) apply(options *options) error {
	return function(options)
}

type options struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
}

func (settings options) enabled() bool {
	return settings.tracerProvider != nil || settings.meterProvider != nil
}

// WithTracerProvider enables fixed-name GORM client spans on an instance
// provider.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return optionFunc(func(options *options) error {
		if isNil(provider) {
			return errors.New("tracer provider is nil")
		}
		options.tracerProvider = provider
		return nil
	})
}

// WithMeterProvider enables fixed-name GORM operation and pool instruments.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return optionFunc(func(options *options) error {
		if isNil(provider) {
			return errors.New("meter provider is nil")
		}
		options.meterProvider = provider
		return nil
	})
}

func applyOptions(optionList []Option) (options, error) {
	settings := options{}
	for index, option := range optionList {
		if option == nil {
			return options{}, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		if err := option.apply(&settings); err != nil {
			return options{}, fmt.Errorf(
				"%w: option %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
	}
	return settings, nil
}

type instrumentation struct {
	pool   Pool
	system string
	name   string
	tracer trace.Tracer

	operations   metric.Int64Counter
	failures     metric.Int64Counter
	duration     metric.Float64Histogram
	registration metric.Registration

	active         atomic.Int64
	operationCount atomic.Uint64
	failureCount   atomic.Uint64
	closeOnce      sync.Once
	closeErr       error
}

func newInstrumentation(
	pool Pool,
	system string,
	name string,
	settings options,
) (*instrumentation, error) {
	result := &instrumentation{
		pool:   pool,
		system: system,
		name:   name,
	}
	if settings.tracerProvider != nil {
		result.tracer = settings.tracerProvider.Tracer(instrumentationName)
	}
	if settings.meterProvider != nil {
		if err := result.configureMetrics(settings.meterProvider); err != nil {
			return nil, fmt.Errorf("gorm data: configure telemetry: %w", err)
		}
	}
	return result, nil
}

func (instrumentation *instrumentation) begin(
	ctx context.Context,
	operation string,
) operationState {
	attributes := []attribute.KeyValue{
		attribute.String("db.system.name", instrumentation.system),
		attribute.String("db.namespace", instrumentation.name),
		attribute.String("db.operation.name", operation),
	}
	instrumentation.active.Add(1)
	state := operationState{
		instrumentation: instrumentation,
		context:         ctx,
		attributes:      attributes,
		startedAt:       time.Now(),
	}
	if instrumentation.tracer != nil {
		state.context, state.span = instrumentation.tracer.Start(
			ctx,
			instrumentation.system+"."+operation,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attributes...),
		)
	}
	return state
}

func (instrumentation *instrumentation) configureMetrics(
	provider metric.MeterProvider,
) error {
	meter := provider.Meter(instrumentationName)
	var err error
	instrumentation.operations, err = meter.Int64Counter(
		"keelith.gorm.operations",
	)
	if err != nil {
		return err
	}
	instrumentation.failures, err = meter.Int64Counter("keelith.gorm.errors")
	if err != nil {
		return err
	}
	instrumentation.duration, err = meter.Float64Histogram(
		"keelith.gorm.duration",
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}
	connections, err := meter.Int64ObservableGauge(
		"keelith.gorm.connections",
	)
	if err != nil {
		return err
	}
	waitCount, err := meter.Int64ObservableCounter(
		"keelith.gorm.wait.count",
	)
	if err != nil {
		return err
	}
	waitDuration, err := meter.Float64ObservableCounter(
		"keelith.gorm.wait.duration",
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}
	base := []attribute.KeyValue{
		attribute.String("db.system.name", instrumentation.system),
		attribute.String("db.namespace", instrumentation.name),
	}
	instrumentation.registration, err = meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			stats := instrumentation.pool.Stats()
			observer.ObserveInt64(
				connections,
				int64(stats.InUse),
				metric.WithAttributes(appendAttribute(
					base,
					attribute.String("state", "used"),
				)...),
			)
			observer.ObserveInt64(
				connections,
				int64(stats.Idle),
				metric.WithAttributes(appendAttribute(
					base,
					attribute.String("state", "idle"),
				)...),
			)
			observer.ObserveInt64(
				waitCount,
				stats.WaitCount,
				metric.WithAttributes(base...),
			)
			observer.ObserveFloat64(
				waitDuration,
				stats.WaitDuration.Seconds(),
				metric.WithAttributes(base...),
			)
			return nil
		},
		connections,
		waitCount,
		waitDuration,
	)
	return err
}

func appendAttribute(
	base []attribute.KeyValue,
	value attribute.KeyValue,
) []attribute.KeyValue {
	result := make([]attribute.KeyValue, 0, len(base)+1)
	result = append(result, base...)
	return append(result, value)
}

func (instrumentation *instrumentation) close() error {
	if instrumentation == nil {
		return nil
	}
	instrumentation.closeOnce.Do(func() {
		if instrumentation.registration != nil {
			instrumentation.closeErr = instrumentation.registration.Unregister()
		}
	})
	return instrumentation.closeErr
}

type operationState struct {
	instrumentation *instrumentation
	context         context.Context
	attributes      []attribute.KeyValue
	startedAt       time.Time
	span            trace.Span
}

func (state operationState) finish(err error) {
	if state.instrumentation == nil {
		return
	}
	failed := err != nil && !errors.Is(err, gormio.ErrRecordNotFound)
	options := metric.WithAttributes(state.attributes...)
	if state.instrumentation.operations != nil {
		state.instrumentation.operations.Add(state.context, 1, options)
		state.instrumentation.duration.Record(
			state.context,
			time.Since(state.startedAt).Seconds(),
			options,
		)
		if failed {
			state.instrumentation.failures.Add(state.context, 1, options)
		}
	}
	state.instrumentation.operationCount.Add(1)
	if failed {
		state.instrumentation.failureCount.Add(1)
	}
	if state.span != nil {
		if failed {
			state.span.RecordError(err)
			state.span.SetStatus(codes.Error, "database operation failed")
		}
		state.span.End()
	}
	state.instrumentation.active.Add(-1)
}

type instrumentationPlugin struct {
	current atomic.Pointer[instrumentation]
}

type processorSpec struct {
	operation string
	get       func(string) func(*gormio.DB)
	before    func(string, func(*gormio.DB)) error
	after     func(string, func(*gormio.DB)) error
	remove    func(string) error
}

func (*instrumentationPlugin) Name() string {
	return pluginName
}

func (plugin *instrumentationPlugin) Initialize(database *gormio.DB) error {
	create := database.Callback().Create()
	query := database.Callback().Query()
	update := database.Callback().Update()
	remove := database.Callback().Delete()
	row := database.Callback().Row()
	raw := database.Callback().Raw()
	processors := []processorSpec{
		newProcessorSpec("create", create.Get, create.Remove,
			func(name string, callback func(*gormio.DB)) error {
				return create.Before("*").Register(name, callback)
			},
			func(name string, callback func(*gormio.DB)) error {
				return create.After("*").Register(name, callback)
			},
		),
		newProcessorSpec("query", query.Get, query.Remove,
			func(name string, callback func(*gormio.DB)) error {
				return query.Before("*").Register(name, callback)
			},
			func(name string, callback func(*gormio.DB)) error {
				return query.After("*").Register(name, callback)
			},
		),
		newProcessorSpec("update", update.Get, update.Remove,
			func(name string, callback func(*gormio.DB)) error {
				return update.Before("*").Register(name, callback)
			},
			func(name string, callback func(*gormio.DB)) error {
				return update.After("*").Register(name, callback)
			},
		),
		newProcessorSpec("delete", remove.Get, remove.Remove,
			func(name string, callback func(*gormio.DB)) error {
				return remove.Before("*").Register(name, callback)
			},
			func(name string, callback func(*gormio.DB)) error {
				return remove.After("*").Register(name, callback)
			},
		),
		newProcessorSpec("row", row.Get, row.Remove,
			func(name string, callback func(*gormio.DB)) error {
				return row.Before("*").Register(name, callback)
			},
			func(name string, callback func(*gormio.DB)) error {
				return row.After("*").Register(name, callback)
			},
		),
		newProcessorSpec("raw", raw.Get, raw.Remove,
			func(name string, callback func(*gormio.DB)) error {
				return raw.Before("*").Register(name, callback)
			},
			func(name string, callback func(*gormio.DB)) error {
				return raw.After("*").Register(name, callback)
			},
		),
	}
	for _, specification := range processors {
		beforeName, afterName := callbackNames(specification.operation)
		if specification.get(beforeName) != nil ||
			specification.get(afterName) != nil {
			return fmt.Errorf(
				"%w: callback for %s already exists",
				ErrInvalidOption,
				specification.operation,
			)
		}
	}
	var registered []struct {
		remove func(string) error
		name   string
	}
	rollback := func() {
		for index := len(registered) - 1; index >= 0; index-- {
			_ = registered[index].remove(registered[index].name)
		}
	}
	for _, specification := range processors {
		beforeName, afterName := callbackNames(specification.operation)
		operation := specification.operation
		if err := specification.before(
			beforeName,
			func(database *gormio.DB) {
				plugin.before(database, operation)
			},
		); err != nil {
			rollback()
			return err
		}
		registered = append(registered, struct {
			remove func(string) error
			name   string
		}{remove: specification.remove, name: beforeName})
		if err := specification.after(
			afterName,
			plugin.after,
		); err != nil {
			rollback()
			return err
		}
		registered = append(registered, struct {
			remove func(string) error
			name   string
		}{remove: specification.remove, name: afterName})
	}
	return nil
}

func newProcessorSpec(
	operation string,
	get func(string) func(*gormio.DB),
	remove func(string) error,
	before func(string, func(*gormio.DB)) error,
	after func(string, func(*gormio.DB)) error,
) processorSpec {
	return processorSpec{
		operation: operation,
		get:       get,
		before:    before,
		after:     after,
		remove:    remove,
	}
}

func callbackNames(operation string) (string, string) {
	return pluginName + ":before:" + operation,
		pluginName + ":after:" + operation
}

func (plugin *instrumentationPlugin) acquire(
	instrumentation *instrumentation,
) error {
	if instrumentation == nil {
		return fmt.Errorf("%w: instrumentation is nil", ErrInvalidOption)
	}
	if !plugin.current.CompareAndSwap(nil, instrumentation) {
		return fmt.Errorf(
			"%w: GORM instance already has active telemetry",
			ErrInvalidOption,
		)
	}
	return nil
}

func (plugin *instrumentationPlugin) release(
	instrumentation *instrumentation,
) {
	if plugin == nil || instrumentation == nil {
		return
	}
	plugin.current.CompareAndSwap(instrumentation, nil)
}

func (plugin *instrumentationPlugin) before(
	database *gormio.DB,
	operation string,
) {
	if database == nil ||
		database.Statement == nil ||
		database.DryRun {
		return
	}
	instrumentation := plugin.current.Load()
	if instrumentation == nil {
		return
	}
	ctx := database.Statement.Context
	if ctx == nil {
		ctx = context.Background()
	}
	state := instrumentation.begin(ctx, operation)
	database.Statement.Context = state.context
	database.Statement.Settings.Store(operationStateKey, state)
}

func (*instrumentationPlugin) after(database *gormio.DB) {
	if database == nil || database.Statement == nil {
		return
	}
	value, ok := database.Statement.Settings.LoadAndDelete(operationStateKey)
	if !ok {
		return
	}
	state, ok := value.(operationState)
	if !ok {
		return
	}
	state.finish(database.Error)
}

func acquireInstrumentation(
	database *gormio.DB,
	instrumentation *instrumentation,
) (*instrumentationPlugin, error) {
	pluginInstallMu.Lock()
	defer pluginInstallMu.Unlock()

	if existing, ok := database.Plugins[pluginName]; ok {
		plugin, valid := existing.(*instrumentationPlugin)
		if !valid {
			return nil, fmt.Errorf(
				"%w: plugin name %q is already registered",
				ErrInvalidOption,
				pluginName,
			)
		}
		if err := plugin.acquire(instrumentation); err != nil {
			return nil, err
		}
		return plugin, nil
	}
	plugin := &instrumentationPlugin{}
	if err := plugin.acquire(instrumentation); err != nil {
		return nil, err
	}
	if err := database.Use(plugin); err != nil {
		plugin.release(instrumentation)
		return nil, fmt.Errorf("gorm data: install telemetry: %w", err)
	}
	return plugin, nil
}
