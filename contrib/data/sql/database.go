// Package sql provides lifecycle and low-cardinality instrumentation for
// database/sql without wrapping the complete standard API.
package sql

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"

	keelithconfig "github.com/keelab/keelith/config"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var (
	// ErrInvalidOption reports an invalid database, pool, or telemetry option.
	ErrInvalidOption    = errors.New("sql data: invalid option")
	errTransactionPanic = errors.New("sql data: transaction panicked")
)

const instrumentationName = "github.com/keelab/contrib/data/sql"

// Config controls pool tuning, identity, and lifecycle ownership.
type Config struct {
	Owns        bool          `config:"owns"`
	System      string        `config:"system"`
	Name        string        `config:"name"`
	MaxIdle     int           `config:"maxIdle" reload:"true"`
	MaxOpen     int           `config:"maxOpen" reload:"true"`
	MaxIdleTime time.Duration `config:"maxIdleTime" reload:"true"`
	MaxLifetime time.Duration `config:"maxLifetime" reload:"true"`
}

// Description is a query- and credential-free SQL pool snapshot.
type Description struct {
	Started        bool
	Closed         bool
	HealthChecks   uint64
	HealthFailures uint64
	Open           int
	InUse          int
	Idle           int
	WaitCount      int64
}

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

// WithTracerProvider enables client spans on an instance provider.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return optionFunc(func(options *options) error {
		if isNil(provider) {
			return fmt.Errorf("tracer provider is nil")
		}
		options.tracerProvider = provider
		return nil
	})
}

// WithMeterProvider enables operation and pool instruments.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return optionFunc(func(options *options) error {
		if isNil(provider) {
			return fmt.Errorf("meter provider is nil")
		}
		options.meterProvider = provider
		return nil
	})
}

// Database owns an explicit SQL pool boundary and optional instrumentation.
type Database struct {
	database     *stdsql.DB
	owns         bool
	system       string
	name         string
	driverName   string
	dsnReference string
	tracer       trace.Tracer

	operations   metric.Int64Counter
	failures     metric.Int64Counter
	duration     metric.Float64Histogram
	registration metric.Registration

	lifecycleMu    sync.Mutex
	started        bool
	closed         bool
	healthChecks   uint64
	healthFailures uint64

	closeOnce sync.Once
	closeErr  error
}

// Open creates and owns a standard SQL pool. Connectivity is checked in Start.
func Open(
	driverName string,
	dataSourceName string,
	config Config,
	optionList ...Option,
) (*Database, error) {
	if strings.TrimSpace(driverName) == "" {
		return nil, fmt.Errorf("%w: driver name is empty", ErrInvalidOption)
	}
	database, err := stdsql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("sql data: open: %w", err)
	}
	config.Owns = true
	result, err := Wrap(database, config, optionList...)
	if err != nil {
		_ = database.Close()
		return nil, err
	}
	return result, nil
}

// Wrap adopts a SQL pool with explicit ownership.
func Wrap(
	database *stdsql.DB,
	config Config,
	optionList ...Option,
) (*Database, error) {
	if database == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	settings := options{}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf(
				"%w: option %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
	}
	applyPoolConfig(database, config)
	result := &Database{
		database: database,
		owns:     config.Owns,
		system:   config.System,
		name:     config.Name,
	}
	if settings.tracerProvider != nil {
		result.tracer = settings.tracerProvider.Tracer(instrumentationName)
	}
	if settings.meterProvider != nil {
		if err := result.configureMetrics(settings.meterProvider); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Start verifies connectivity inside the App startup rollback boundary.
func (database *Database) Start(ctx context.Context) error {
	if database == nil || database.database == nil {
		return fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	database.lifecycleMu.Lock()
	database.healthChecks++
	database.lifecycleMu.Unlock()
	if err := database.database.PingContext(ctx); err != nil {
		database.lifecycleMu.Lock()
		database.healthFailures++
		database.lifecycleMu.Unlock()
		return fmt.Errorf("sql data: ping: %w", err)
	}
	database.lifecycleMu.Lock()
	if database.closed {
		database.lifecycleMu.Unlock()
		return fmt.Errorf("%w: database is closed", ErrInvalidOption)
	}
	database.started = true
	database.lifecycleMu.Unlock()
	return nil
}

// Shutdown unregisters telemetry and closes an owned pool exactly once.
func (database *Database) Shutdown(context.Context) error {
	if database == nil {
		return nil
	}
	database.closeOnce.Do(func() {
		var unregisterErr error
		if database.registration != nil {
			unregisterErr = database.registration.Unregister()
		}
		var closeErr error
		if database.owns && database.database != nil {
			closeErr = database.database.Close()
		}
		database.closeErr = errors.Join(unregisterErr, closeErr)
		database.lifecycleMu.Lock()
		database.started = false
		database.closed = true
		database.lifecycleMu.Unlock()
	})
	return database.closeErr
}

// ApplyConfig hot-applies connection pool limits. Ownership and telemetry
// identity are construction-time fields and require a component restart.
func (database *Database) ApplyConfig(ctx context.Context, config Config) error {
	if database == nil || database.database == nil {
		return fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := ValidateConfig(config); err != nil {
		return err
	}
	if config.Owns != database.owns ||
		config.System != database.system ||
		config.Name != database.name {
		return fmt.Errorf(
			"%w: SQL ownership or identity changed",
			keelithconfig.ErrRestartRequired,
		)
	}
	applyPoolConfig(database.database, config)
	return nil
}

// DB returns the underlying standard pool for APIs not wrapped by Keelith.
func (database *Database) DB() *stdsql.DB {
	if database == nil {
		return nil
	}
	return database.database
}

// Stats returns a point-in-time pool snapshot.
func (database *Database) Stats() stdsql.DBStats {
	if database == nil || database.database == nil {
		return stdsql.DBStats{}
	}
	return database.database.Stats()
}

// Description returns lifecycle, health, and aggregate pool occupancy.
func (database *Database) Description() Description {
	if database == nil {
		return Description{Closed: true}
	}
	database.lifecycleMu.Lock()
	description := Description{
		Started:        database.started,
		Closed:         database.closed,
		HealthChecks:   database.healthChecks,
		HealthFailures: database.healthFailures,
	}
	database.lifecycleMu.Unlock()
	stats := database.Stats()
	description.Open = stats.OpenConnections
	description.InUse = stats.InUse
	description.Idle = stats.Idle
	description.WaitCount = stats.WaitCount
	return description
}

// ExecContext instruments a stable operation name without recording raw SQL.
func (database *Database) ExecContext(
	ctx context.Context,
	operationName string,
	query string,
	arguments ...any,
) (stdsql.Result, error) {
	if err := database.validateCall(ctx, operationName); err != nil {
		return nil, err
	}
	finishCtx, finish := database.begin(ctx, operationName)
	result, err := database.database.ExecContext(
		finishCtx,
		query,
		arguments...,
	)
	finish(err)
	return result, err
}

// QueryContext instruments a stable operation name without recording raw SQL.
func (database *Database) QueryContext(
	ctx context.Context,
	operationName string,
	query string,
	arguments ...any,
) (*stdsql.Rows, error) {
	if err := database.validateCall(ctx, operationName); err != nil {
		return nil, err
	}
	finishCtx, finish := database.begin(ctx, operationName)
	rows, err := database.database.QueryContext(
		finishCtx,
		query,
		arguments...,
	)
	finish(err)
	return rows, err
}

// QueryRowContext defers telemetry completion until Scan observes the result.
func (database *Database) QueryRowContext(
	ctx context.Context,
	operationName string,
	query string,
	arguments ...any,
) (*Row, error) {
	if err := database.validateCall(ctx, operationName); err != nil {
		return nil, err
	}
	finishCtx, finish := database.begin(ctx, operationName)
	return &Row{
		row:    database.database.QueryRowContext(finishCtx, query, arguments...),
		finish: finish,
	}, nil
}

// Transaction instruments a transaction and preserves the original callback
// shape. New code that starts child operations should prefer
// TransactionContext so the transaction span context reaches the callback.
func (database *Database) Transaction(
	ctx context.Context,
	operationName string,
	options *stdsql.TxOptions,
	function func(*stdsql.Tx) error,
) error {
	if function == nil {
		return fmt.Errorf("%w: transaction function is nil", ErrInvalidOption)
	}
	return database.TransactionContext(
		ctx,
		operationName,
		options,
		func(_ context.Context, transaction *stdsql.Tx) error {
			return function(transaction)
		},
	)
}

// TransactionContext instruments begin, callback, rollback, and commit as one
// stable database operation. The callback receives the span-bearing context
// and the exact transaction used for all business, Outbox, and related writes.
func (database *Database) TransactionContext(
	ctx context.Context,
	operationName string,
	options *stdsql.TxOptions,
	function func(context.Context, *stdsql.Tx) error,
) (resultErr error) {
	if function == nil {
		return fmt.Errorf("%w: transaction function is nil", ErrInvalidOption)
	}
	if err := database.validateCall(ctx, operationName); err != nil {
		return err
	}
	finishCtx, finish := database.begin(ctx, operationName)
	var transaction *stdsql.Tx
	defer func() {
		if recovered := recover(); recovered != nil {
			var rollbackErr error
			if transaction != nil {
				rollbackErr = transaction.Rollback()
			}
			finish(errors.Join(errTransactionPanic, rollbackErr))
			panic(recovered)
		}
		finish(resultErr)
	}()

	var err error
	transaction, err = database.database.BeginTx(finishCtx, options)
	if err != nil {
		return err
	}
	if err := function(finishCtx, transaction); err != nil {
		return errors.Join(err, transaction.Rollback())
	}
	return transaction.Commit()
}

// Row delays QueryRow telemetry until Scan.
type Row struct {
	row    *stdsql.Row
	finish func(error)
	once   sync.Once
}

// Scan delegates to database/sql and completes telemetry once.
func (row *Row) Scan(destinations ...any) error {
	if row == nil || row.row == nil {
		return fmt.Errorf("%w: row is nil", ErrInvalidOption)
	}
	err := row.row.Scan(destinations...)
	row.once.Do(func() { row.finish(err) })
	return err
}

func (database *Database) validateCall(
	ctx context.Context,
	operationName string,
) error {
	if database == nil || database.database == nil {
		return fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if !validIdentity(operationName) {
		return fmt.Errorf(
			"%w: operation name %q is invalid",
			ErrInvalidOption,
			operationName,
		)
	}
	return nil
}

func (database *Database) begin(
	ctx context.Context,
	operationName string,
) (context.Context, func(error)) {
	attributes := []attribute.KeyValue{
		attribute.String("db.system.name", database.system),
		attribute.String("db.namespace", database.name),
		attribute.String("db.operation.name", operationName),
	}
	started := time.Now()
	span := trace.SpanFromContext(ctx)
	spanStarted := false
	if database.tracer != nil {
		ctx, span = database.tracer.Start(
			ctx,
			database.system+"."+operationName,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attributes...),
		)
		spanStarted = true
	}
	var once sync.Once
	return ctx, func(err error) {
		once.Do(func() {
			options := metric.WithAttributes(attributes...)
			if database.operations != nil {
				database.operations.Add(ctx, 1, options)
				database.duration.Record(
					ctx,
					time.Since(started).Seconds(),
					options,
				)
				if err != nil {
					database.failures.Add(ctx, 1, options)
				}
			}
			if spanStarted {
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, "database operation failed")
				}
				span.End()
			}
		})
	}
}

func (database *Database) configureMetrics(
	provider metric.MeterProvider,
) error {
	meter := provider.Meter(instrumentationName)
	var err error
	database.operations, err = meter.Int64Counter("keelith.db.operations")
	if err != nil {
		return err
	}
	database.failures, err = meter.Int64Counter("keelith.db.errors")
	if err != nil {
		return err
	}
	database.duration, err = meter.Float64Histogram(
		"keelith.db.duration",
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}
	connections, err := meter.Int64ObservableGauge(
		"keelith.db.connections",
	)
	if err != nil {
		return err
	}
	waitCount, err := meter.Int64ObservableCounter(
		"keelith.db.wait.count",
	)
	if err != nil {
		return err
	}
	waitDuration, err := meter.Float64ObservableCounter(
		"keelith.db.wait.duration",
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}
	base := []attribute.KeyValue{
		attribute.String("db.system.name", database.system),
		attribute.String("db.namespace", database.name),
	}
	database.registration, err = meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			stats := database.database.Stats()
			observer.ObserveInt64(
				connections,
				int64(stats.InUse),
				metric.WithAttributes(append(
					append([]attribute.KeyValue(nil), base...),
					attribute.String("state", "used"),
				)...),
			)
			observer.ObserveInt64(
				connections,
				int64(stats.Idle),
				metric.WithAttributes(append(
					append([]attribute.KeyValue(nil), base...),
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

// ValidateConfig validates SQL identity, pool limits, and hot-update inputs.
func ValidateConfig(config Config) error {
	if !validIdentity(config.System) || !validIdentity(config.Name) {
		return fmt.Errorf(
			"%w: database system and name are required",
			ErrInvalidOption,
		)
	}
	if config.MaxIdle < 0 ||
		config.MaxOpen < 0 ||
		config.MaxIdleTime < 0 ||
		config.MaxLifetime < 0 {
		return fmt.Errorf("%w: pool limit is negative", ErrInvalidOption)
	}
	if config.MaxOpen > 0 && config.MaxIdle > config.MaxOpen {
		return fmt.Errorf("%w: max idle exceeds max open", ErrInvalidOption)
	}
	return nil
}

func validateConfig(config Config) error {
	return ValidateConfig(config)
}

func applyPoolConfig(database *stdsql.DB, config Config) {
	database.SetMaxIdleConns(config.MaxIdle)
	database.SetMaxOpenConns(config.MaxOpen)
	database.SetConnMaxIdleTime(config.MaxIdleTime)
	database.SetConnMaxLifetime(config.MaxLifetime)
}

func validIdentity(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func isNil(value any) bool {
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
