// Package gorm provides lifecycle and transaction boundaries for GORM.
package gorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	keelithconfig "github.com/keelab/keelith/config"
	gormio "gorm.io/gorm"
)

var (
	// ErrInvalidOption reports an invalid database, pool, or configuration.
	ErrInvalidOption = errors.New("gorm data: invalid option")
	// ErrSQLTransactionUnavailable reports a GORM dialect that does not expose
	// its transaction as the standard database/sql transaction required by a
	// collaborating SQL adapter such as an Outbox repository.
	ErrSQLTransactionUnavailable = errors.New(
		"gorm data: SQL transaction unavailable",
	)
)

// Pool is the standard database/sql lifecycle and tuning surface used by
// Database.
type Pool interface {
	PingContext(context.Context) error
	Close() error
	SetMaxIdleConns(int)
	SetMaxOpenConns(int)
	SetConnMaxIdleTime(time.Duration)
	SetConnMaxLifetime(time.Duration)
	Stats() sql.DBStats
}

// Config controls pool limits, optional telemetry identity, and ownership.
type Config struct {
	Owns           bool          `config:"owns"`
	System         string        `config:"system"`
	Name           string        `config:"name"`
	MaxIdle        int           `config:"maxIdle" reload:"true"`
	MaxOpen        int           `config:"maxOpen" reload:"true"`
	MaxIdleTime    time.Duration `config:"maxIdleTime" reload:"true"`
	MaxLifetime    time.Duration `config:"maxLifetime" reload:"true"`
	DisablePrepare bool          `config:"disablePrepare"`
}

// Database owns one GORM handle and its underlying SQL pool.
type Database struct {
	database       *gormio.DB
	pool           Pool
	owns           bool
	system         string
	name           string
	disablePrepare bool
	telemetry      *instrumentation
	plugin         *instrumentationPlugin

	closeOnce sync.Once
	closeErr  error
	started   atomic.Bool
	closed    atomic.Bool
}

// Open creates and owns a GORM database and SQL pool.
func Open(
	dialector gormio.Dialector,
	gormConfig *gormio.Config,
	config Config,
	optionList ...Option,
) (*Database, error) {
	if isNil(dialector) {
		return nil, fmt.Errorf("%w: dialector is nil", ErrInvalidOption)
	}
	if gormConfig == nil {
		gormConfig = &gormio.Config{}
	}
	if config.DisablePrepare {
		cloned := *gormConfig
		cloned.PrepareStmt = false
		gormConfig = &cloned
	}
	database, err := gormio.Open(dialector, gormConfig)
	if err != nil {
		return nil, fmt.Errorf("gorm data: open: %w", err)
	}
	pool, err := database.DB()
	if err != nil {
		return nil, fmt.Errorf("gorm data: SQL pool: %w", err)
	}
	config.Owns = true
	result, err := Wrap(database, pool, config, optionList...)
	if err != nil {
		_ = pool.Close()
		return nil, err
	}
	return result, nil
}

// Wrap adopts a GORM handle and explicit pool with configurable ownership.
func Wrap(
	database *gormio.DB,
	pool Pool,
	config Config,
	optionList ...Option,
) (*Database, error) {
	if database == nil || isNil(pool) {
		return nil, fmt.Errorf("%w: database or pool is nil", ErrInvalidOption)
	}
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	settings, err := applyOptions(optionList)
	if err != nil {
		return nil, err
	}
	if settings.enabled() &&
		(!validIdentity(config.System) || !validIdentity(config.Name)) {
		return nil, fmt.Errorf(
			"%w: database system and name are required for telemetry",
			ErrInvalidOption,
		)
	}
	result := &Database{
		database:       database,
		pool:           pool,
		owns:           config.Owns,
		system:         config.System,
		name:           config.Name,
		disablePrepare: config.DisablePrepare,
	}
	if settings.enabled() {
		result.telemetry, err = newInstrumentation(
			pool,
			config.System,
			config.Name,
			settings,
		)
		if err != nil {
			return nil, err
		}
		result.plugin, err = acquireInstrumentation(
			database,
			result.telemetry,
		)
		if err != nil {
			_ = result.telemetry.close()
			return nil, err
		}
	}
	applyPoolConfig(pool, config)
	return result, nil
}

// Start verifies database connectivity inside App startup rollback.
func (database *Database) Start(ctx context.Context) error {
	if database == nil || isNil(database.pool) {
		return fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if database.closed.Load() {
		return fmt.Errorf("%w: database is closed", ErrInvalidOption)
	}
	if err := database.pool.PingContext(ctx); err != nil {
		return fmt.Errorf("gorm data: ping: %w", err)
	}
	database.started.Store(true)
	return nil
}

// Shutdown releases instance telemetry and closes an owned SQL pool exactly
// once. Instrumentation callbacks remain installed as no-ops because GORM
// sessions share one callback manager.
func (database *Database) Shutdown(context.Context) error {
	if database == nil {
		return nil
	}
	database.closeOnce.Do(func() {
		database.closed.Store(true)
		database.started.Store(false)
		if database.plugin != nil && database.telemetry != nil {
			database.plugin.release(database.telemetry)
		}
		var telemetryErr error
		if database.telemetry != nil {
			telemetryErr = database.telemetry.close()
		}
		var closeErr error
		if database.owns && !isNil(database.pool) {
			closeErr = database.pool.Close()
		}
		database.closeErr = errors.Join(telemetryErr, closeErr)
	})
	return database.closeErr
}

// DB returns the configured GORM handle.
func (database *Database) DB() *gormio.DB {
	if database == nil {
		return nil
	}
	return database.database
}

// Stats returns a point-in-time SQL pool snapshot.
func (database *Database) Stats() sql.DBStats {
	if database == nil || isNil(database.pool) {
		return sql.DBStats{}
	}
	return database.pool.Stats()
}

// ApplyConfig hot-applies SQL pool limits. Ownership, telemetry identity, and
// prepared-statement behavior are construction-time fields and require a
// component restart.
func (database *Database) ApplyConfig(ctx context.Context, config Config) error {
	if database == nil || isNil(database.pool) {
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
		config.Name != database.name ||
		config.DisablePrepare != database.disablePrepare {
		return fmt.Errorf(
			"%w: GORM ownership, identity, or prepared-statement behavior changed",
			keelithconfig.ErrRestartRequired,
		)
	}
	applyPoolConfig(database.pool, config)
	return nil
}

// Transaction runs function in a context-bound GORM transaction.
func (database *Database) Transaction(
	ctx context.Context,
	function func(*gormio.DB) error,
) error {
	if database == nil || database.database == nil {
		return fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	if ctx == nil || function == nil {
		return fmt.Errorf("%w: context or function is nil", ErrInvalidOption)
	}
	if err := database.database.WithContext(ctx).Transaction(function); err != nil {
		return fmt.Errorf("gorm data: transaction: %w", err)
	}
	return nil
}

// TransactionContext runs function in one context-bound GORM transaction and
// exposes the exact underlying database/sql transaction. It is intended for
// atomic collaboration with SQL-native adapters such as Outbox and Inbox.
func (database *Database) TransactionContext(
	ctx context.Context,
	function func(*gormio.DB, *sql.Tx) error,
) error {
	if database == nil || database.database == nil {
		return fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	if ctx == nil || function == nil {
		return fmt.Errorf("%w: context or function is nil", ErrInvalidOption)
	}
	err := database.database.WithContext(ctx).Transaction(
		func(transaction *gormio.DB) error {
			sqlTransaction, ok := transaction.Statement.ConnPool.(*sql.Tx)
			if !ok || sqlTransaction == nil {
				return ErrSQLTransactionUnavailable
			}
			return function(transaction, sqlTransaction)
		},
	)
	if err != nil {
		return fmt.Errorf("gorm data: transaction context: %w", err)
	}
	return nil
}

// ValidateConfig validates GORM telemetry identity and SQL pool limits. System
// and Name may both be empty when telemetry is disabled.
func ValidateConfig(config Config) error {
	if (config.System == "") != (config.Name == "") {
		return fmt.Errorf(
			"%w: database system and name must both be set",
			ErrInvalidOption,
		)
	}
	if config.System != "" &&
		(!validIdentity(config.System) || !validIdentity(config.Name)) {
		return fmt.Errorf(
			"%w: database system or name is invalid",
			ErrInvalidOption,
		)
	}
	if config.MaxIdle < 0 ||
		config.MaxOpen < 0 ||
		config.MaxIdleTime < 0 ||
		config.MaxLifetime < 0 {
		return fmt.Errorf("%w: pool limit is negative", ErrInvalidOption)
	}
	if config.MaxOpen > 0 &&
		config.MaxIdle > config.MaxOpen {
		return fmt.Errorf(
			"%w: max idle exceeds max open",
			ErrInvalidOption,
		)
	}
	return nil
}

func validIdentity(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func applyPoolConfig(pool Pool, config Config) {
	pool.SetMaxIdleConns(config.MaxIdle)
	pool.SetMaxOpenConns(config.MaxOpen)
	pool.SetConnMaxIdleTime(config.MaxIdleTime)
	pool.SetConnMaxLifetime(config.MaxLifetime)
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
