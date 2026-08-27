package inbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	keelithconfig "github.com/keelab/keelith/config"
	coreinbox "github.com/keelab/keelith/inbox"
)

// RuntimeConfig declares one PostgreSQL transaction-local Inbox.
type RuntimeConfig struct {
	Table      string        `config:"table" yaml:"table"`
	Isolation  string        `config:"isolation" yaml:"isolation"`
	Consumer   string        `config:"consumer" yaml:"consumer"`
	RetryAfter time.Duration `config:"retryAfter" yaml:"retryAfter"`
}

// ValidateRuntimeConfig rejects malformed identity or retry configuration.
func ValidateRuntimeConfig(config RuntimeConfig) error {
	if _, err := Schema(config.Table); err != nil {
		return fmt.Errorf("%w: table", ErrInvalidOption)
	}
	if _, err := runtimeIsolationLevel(config.Isolation); err != nil {
		return err
	}
	if err := (coreinbox.Key{
		Consumer: strings.TrimSpace(config.Consumer),
		Message:  "validation",
	}).Validate(); err != nil {
		return fmt.Errorf("%w: consumer", ErrInvalidOption)
	}
	if config.RetryAfter <= 0 {
		return fmt.Errorf("%w: retry delay", ErrInvalidOption)
	}
	return nil
}

// NewRuntimeConfigBinding creates a strict construction-time binding.
func NewRuntimeConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[RuntimeConfig],
) (*keelithconfig.Component[RuntimeConfig], error) {
	all := make(
		[]keelithconfig.ComponentOption[RuntimeConfig],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateRuntimeConfig),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[RuntimeConfig](name, path, all...)
}

// Runtime binds a PostgreSQL Executor to stable consumer and retry semantics.
type Runtime struct {
	executor   *Executor
	consumer   string
	retryAfter time.Duration
}

// NewRuntime constructs an Inbox runtime without background work.
func NewRuntime(
	config RuntimeConfig,
	database *sql.DB,
) (*Runtime, error) {
	if err := ValidateRuntimeConfig(config); err != nil {
		return nil, err
	}
	isolation, err := runtimeIsolationLevel(config.Isolation)
	if err != nil {
		return nil, err
	}
	executor, err := New(database, Options{
		Table:     config.Table,
		Isolation: isolation,
	})
	if err != nil {
		return nil, err
	}
	return &Runtime{
		executor:   executor,
		consumer:   strings.TrimSpace(config.Consumer),
		retryAfter: config.RetryAfter,
	}, nil
}

// Processor wraps an application-owned transactional Handler.
func (runtime *Runtime) Processor(
	handler coreinbox.Handler[*sql.Tx],
) (*coreinbox.Processor[*sql.Tx], error) {
	if runtime == nil || runtime.executor == nil {
		return nil, fmt.Errorf("%w: runtime is nil", ErrInvalidOption)
	}
	return coreinbox.New(coreinbox.Config[*sql.Tx]{
		Consumer:   runtime.consumer,
		Executor:   runtime.executor,
		Handler:    handler,
		RetryAfter: runtime.retryAfter,
	})
}

// Purge removes one bounded batch through the exact Inbox Executor.
func (runtime *Runtime) Purge(
	ctx context.Context,
	request PurgeRequest,
) (int64, error) {
	if runtime == nil || runtime.executor == nil {
		return 0, fmt.Errorf("%w: runtime is nil", ErrInvalidOption)
	}
	return runtime.executor.Purge(ctx, request)
}

func runtimeIsolationLevel(value string) (sql.IsolationLevel, error) {
	switch strings.TrimSpace(value) {
	case "", "default":
		return sql.LevelDefault, nil
	case "read-committed":
		return sql.LevelReadCommitted, nil
	case "repeatable-read":
		return sql.LevelRepeatableRead, nil
	case "serializable":
		return sql.LevelSerializable, nil
	default:
		return 0, fmt.Errorf("%w: isolation level", ErrInvalidOption)
	}
}
