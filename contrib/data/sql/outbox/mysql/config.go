package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	keelithconfig "github.com/keelab/keelith/config"
	coreoutbox "github.com/keelab/keelith/outbox"
)

var (
	_ coreoutbox.Enqueuer[*sql.Tx] = (*Runtime)(nil)
	_ coreoutbox.Maintenance       = (*Runtime)(nil)
)

// RuntimeConfig declares one MySQL-backed outbox Dispatcher.
//
// Every field is construction-time identity. Changing it requires replacing
// the Dispatcher so a single claim loop never mixes lease policies.
type RuntimeConfig struct {
	Table          string        `config:"table" yaml:"table"`
	Isolation      string        `config:"isolation" yaml:"isolation"`
	PollInterval   time.Duration `config:"pollInterval" yaml:"pollInterval"`
	ErrorDelay     time.Duration `config:"errorDelay" yaml:"errorDelay"`
	LeaseTTL       time.Duration `config:"leasettl" yaml:"leasettl"`
	PublishTimeout time.Duration `config:"publishTimeout" yaml:"publishTimeout"`
	BatchSize      int           `config:"batchSize" yaml:"batchSize"`
	MaxAttempts    int           `config:"maxAttempts" yaml:"maxAttempts"`
	RetryBase      time.Duration `config:"retryBase" yaml:"retryBase"`
	RetryMax       time.Duration `config:"retryMax" yaml:"retryMax"`
}

// ValidateRuntimeConfig rejects unbounded or internally inconsistent
// generated-project configuration.
func ValidateRuntimeConfig(config RuntimeConfig) error {
	if _, err := Schema(config.Table); err != nil {
		return fmt.Errorf("%w: table", ErrInvalidOption)
	}
	if _, err := isolationLevel(config.Isolation); err != nil {
		return err
	}
	if config.PollInterval <= 0 ||
		config.ErrorDelay <= 0 ||
		config.LeaseTTL <= 0 ||
		config.PublishTimeout <= 0 ||
		config.PublishTimeout >= config.LeaseTTL ||
		config.BatchSize <= 0 ||
		config.BatchSize > 10_000 ||
		config.MaxAttempts <= 0 ||
		config.RetryBase <= 0 ||
		config.RetryMax < config.RetryBase {
		return fmt.Errorf("%w: dispatcher budgets", ErrInvalidOption)
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

// Runtime owns the Repository used by business transactions and its
// corresponding Dispatcher.
type Runtime struct {
	repository *Repository
	dispatcher *coreoutbox.Dispatcher
}

// NewRuntime wires the dialect Repository into the shared broker-neutral
// Dispatcher while preserving the same Repository for business Enqueue calls.
func NewRuntime(
	config RuntimeConfig,
	name string,
	owner string,
	database *sql.DB,
	publisher coreoutbox.Publisher,
) (*Runtime, error) {
	if err := ValidateRuntimeConfig(config); err != nil {
		return nil, err
	}
	isolation, err := isolationLevel(config.Isolation)
	if err != nil {
		return nil, err
	}
	repository, err := New(database, Options{
		Table:     config.Table,
		Isolation: isolation,
	})
	if err != nil {
		return nil, err
	}
	dispatcher, err := coreoutbox.New(coreoutbox.Config{
		Name:           name,
		Owner:          owner,
		Repository:     repository,
		Publisher:      publisher,
		PollInterval:   config.PollInterval,
		ErrorDelay:     config.ErrorDelay,
		LeaseTTL:       config.LeaseTTL,
		PublishTimeout: config.PublishTimeout,
		BatchSize:      config.BatchSize,
		MaxAttempts:    config.MaxAttempts,
		RetryBase:      config.RetryBase,
		RetryMax:       config.RetryMax,
	})
	if err != nil {
		return nil, err
	}
	return &Runtime{
		repository: repository,
		dispatcher: dispatcher,
	}, nil
}

// NewConfiguredDispatcher constructs only the Dispatcher view for callers
// that enqueue through a separately owned Repository.
func NewConfiguredDispatcher(
	config RuntimeConfig,
	name string,
	owner string,
	database *sql.DB,
	publisher coreoutbox.Publisher,
) (*coreoutbox.Dispatcher, error) {
	runtime, err := NewRuntime(config, name, owner, database, publisher)
	if err != nil {
		return nil, err
	}
	return runtime.Dispatcher(), nil
}

// Enqueue writes through the exact Repository polled by Dispatcher.
func (runtime *Runtime) Enqueue(
	ctx context.Context,
	transaction *sql.Tx,
	message coreoutbox.Message,
	availableAt time.Time,
) error {
	if runtime == nil || runtime.repository == nil {
		return fmt.Errorf("%w: runtime is nil", ErrInvalidOption)
	}
	return runtime.repository.Enqueue(ctx, transaction, message, availableAt)
}

// Replay atomically requeues an exact terminal set.
func (runtime *Runtime) Replay(
	ctx context.Context,
	request coreoutbox.ReplayRequest,
) (coreoutbox.ReplayResult, error) {
	if runtime == nil || runtime.repository == nil {
		return coreoutbox.ReplayResult{}, fmt.Errorf(
			"%w: runtime is nil",
			ErrInvalidOption,
		)
	}
	return runtime.repository.Replay(ctx, request)
}

// Purge removes one bounded retention batch.
func (runtime *Runtime) Purge(
	ctx context.Context,
	request coreoutbox.RetentionRequest,
) (int64, error) {
	if runtime == nil || runtime.repository == nil {
		return 0, fmt.Errorf("%w: runtime is nil", ErrInvalidOption)
	}
	return runtime.repository.Purge(ctx, request)
}

// Dispatcher returns the App-owned claim and publish Server.
func (runtime *Runtime) Dispatcher() *coreoutbox.Dispatcher {
	if runtime == nil {
		return nil
	}
	return runtime.dispatcher
}

func isolationLevel(value string) (sql.IsolationLevel, error) {
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
