package inbox

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	keelithconfig "github.com/keelab/keelith/config"
	coreinbox "github.com/keelab/keelith/inbox"
)

const (
	maxRetentionBatchSize = 10_000
	maxRetentionBatches   = 100
)

// RetentionConfig bounds one scheduled PostgreSQL Inbox cleanup.
type RetentionConfig struct {
	ProcessedRetention time.Duration `config:"processedRetention" yaml:"processedRetention"`
	BatchSize          int           `config:"batchSize" yaml:"batchSize"`
	MaxBatches         int           `config:"maxBatches" yaml:"maxBatches"`
	RetryAfter         time.Duration `config:"retryAfter" yaml:"retryAfter"`
}

// ValidateRetentionConfig rejects unsafe horizons and unbounded work.
func ValidateRetentionConfig(config RetentionConfig) error {
	if config.ProcessedRetention <= 0 ||
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

// RetentionPurger removes one bounded Inbox key batch.
type RetentionPurger interface {
	Purge(context.Context, PurgeRequest) (int64, error)
}

// RetentionResult reports whether one bounded run drained eligible keys.
type RetentionResult struct {
	Batches  int
	Purged   int64
	Complete bool
}

// RetentionRuntime runs bounded cleanup against an existing Inbox Runtime.
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

// NewRetentionRuntime composes cleanup with the exact Inbox Executor.
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
	return &RetentionRuntime{config: config, purger: purger}, nil
}

// Run removes at most BatchSize*MaxBatches keys using one stable cutoff.
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
	if ctx == nil || scheduledAt.IsZero() {
		return RetentionResult{}, fmt.Errorf(
			"%w: retention execution input",
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
	request := PurgeRequest{
		ProcessedBefore: scheduledAt.UTC().
			Add(-runtime.config.ProcessedRetention),
		Limit: runtime.config.BatchSize,
	}
	for result.Batches < runtime.config.MaxBatches {
		if cause := context.Cause(ctx); cause != nil {
			return result, cause
		}
		deleted, purgeErr := runtime.purger.Purge(ctx, request)
		if purgeErr != nil {
			return result, purgeErr
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
func (runtime *RetentionRuntime) Description() coreinbox.RetentionDescription {
	if runtime == nil {
		return coreinbox.RetentionDescription{}
	}
	return coreinbox.RetentionDescription{
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
