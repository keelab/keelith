package mysql

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	keelithconfig "github.com/keelab/keelith/config"
	coreoutbox "github.com/keelab/keelith/outbox"
)

const (
	maxRetentionBatchSize = 10_000
	maxRetentionBatches   = 100
)

// RetentionConfig bounds one scheduled Outbox retention execution.
//
// Published and terminal rows use independent retention windows so operators
// can preserve terminal diagnostics longer than successfully published data.
type RetentionConfig struct {
	PublishedRetention time.Duration `config:"publishedRetention" yaml:"publishedRetention"`
	TerminalRetention  time.Duration `config:"terminalRetention" yaml:"terminalRetention"`
	BatchSize          int           `config:"batchSize" yaml:"batchSize"`
	MaxBatches         int           `config:"maxBatches" yaml:"maxBatches"`
	RetryAfter         time.Duration `config:"retryAfter" yaml:"retryAfter"`
}

// ValidateRetentionConfig rejects unbounded maintenance work.
func ValidateRetentionConfig(config RetentionConfig) error {
	if config.PublishedRetention <= 0 ||
		config.TerminalRetention <= 0 ||
		config.BatchSize <= 0 ||
		config.BatchSize > maxRetentionBatchSize ||
		config.MaxBatches <= 0 ||
		config.MaxBatches > maxRetentionBatches ||
		config.RetryAfter <= 0 {
		return fmt.Errorf("%w: retention budgets", ErrInvalidOption)
	}
	return nil
}

// NewRetentionConfigBinding creates a strict construction-time binding.
func NewRetentionConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[RetentionConfig],
) (*keelithconfig.Component[RetentionConfig], error) {
	all := make(
		[]keelithconfig.ComponentOption[RetentionConfig],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateRetentionConfig),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[RetentionConfig](name, path, all...)
}

// RetentionPurger removes one bounded retention batch.
type RetentionPurger interface {
	Purge(context.Context, coreoutbox.RetentionRequest) (int64, error)
}

// RetentionResult reports whether the bounded run drained every eligible row.
type RetentionResult struct {
	Batches  int
	Purged   int64
	Complete bool
}

// RetentionRuntime runs bounded retention against an existing Outbox runtime.
// It does not own the database or start a background goroutine.
type RetentionRuntime struct {
	config RetentionConfig
	purger RetentionPurger

	active     atomic.Int64
	runs       atomic.Uint64
	batches    atomic.Uint64
	purged     atomic.Uint64
	incomplete atomic.Uint64
	failures   atomic.Uint64
}

// NewRetentionRuntime composes scheduled maintenance with the exact Outbox
// repository already used by business Enqueue and Dispatcher operations.
func NewRetentionRuntime(
	config RetentionConfig,
	purger RetentionPurger,
) (*RetentionRuntime, error) {
	if err := ValidateRetentionConfig(config); err != nil {
		return nil, err
	}
	if purger == nil {
		return nil, fmt.Errorf("%w: retention purger is nil", ErrInvalidOption)
	}
	return &RetentionRuntime{
		config: config,
		purger: purger,
	}, nil
}

// Run removes at most BatchSize*MaxBatches rows using the scheduled instant as
// a stable retention boundary for the whole execution.
func (runtime *RetentionRuntime) Run(
	ctx context.Context,
	scheduledAt time.Time,
) (result RetentionResult, err error) {
	if runtime == nil || runtime.purger == nil {
		return RetentionResult{}, fmt.Errorf(
			"%w: retention runtime is nil",
			ErrInvalidOption,
		)
	}
	if ctx == nil {
		return RetentionResult{}, fmt.Errorf(
			"%w: retention context is nil",
			ErrInvalidOption,
		)
	}
	if scheduledAt.IsZero() {
		return RetentionResult{}, fmt.Errorf(
			"%w: retention scheduled time is zero",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return RetentionResult{}, cause
	}
	runtime.active.Add(1)
	runtime.runs.Add(1)
	defer func() {
		runtime.active.Add(-1)
		runtime.batches.Add(uint64(result.Batches))
		runtime.purged.Add(uint64(result.Purged))
		if err != nil {
			runtime.failures.Add(1)
		} else if !result.Complete {
			runtime.incomplete.Add(1)
		}
	}()
	boundary := scheduledAt.UTC()
	request := coreoutbox.RetentionRequest{
		PublishedBefore: boundary.Add(-runtime.config.PublishedRetention),
		TerminalBefore:  boundary.Add(-runtime.config.TerminalRetention),
		Limit:           runtime.config.BatchSize,
	}
	for result.Batches < runtime.config.MaxBatches {
		if cause := context.Cause(ctx); cause != nil {
			return result, cause
		}
		deleted, err := runtime.purger.Purge(ctx, request)
		if err != nil {
			return result, err
		}
		if deleted < 0 || deleted > int64(runtime.config.BatchSize) {
			return result, fmt.Errorf(
				"%w: retention purger returned %d rows for limit %d",
				ErrInvalidOption,
				deleted,
				runtime.config.BatchSize,
			)
		}
		result.Batches++
		result.Purged += deleted
		if deleted < int64(runtime.config.BatchSize) {
			result.Complete = true
			return result, nil
		}
	}
	return result, nil
}

// Description returns aggregate bounded-run progress.
func (runtime *RetentionRuntime) Description() coreoutbox.RetentionDescription {
	if runtime == nil {
		return coreoutbox.RetentionDescription{}
	}
	return coreoutbox.RetentionDescription{
		Active:     runtime.active.Load(),
		Runs:       runtime.runs.Load(),
		Batches:    runtime.batches.Load(),
		Purged:     runtime.purged.Load(),
		Incomplete: runtime.incomplete.Load(),
		Failures:   runtime.failures.Load(),
	}
}

// RetryAfter returns the configured Job retry delay.
func (runtime *RetentionRuntime) RetryAfter() time.Duration {
	if runtime == nil {
		return 0
	}
	return runtime.config.RetryAfter
}
