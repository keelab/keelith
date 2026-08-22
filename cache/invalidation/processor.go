package invalidation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/keelab/keelith/cache"
	"github.com/keelab/keelith/worker"
)

const defaultRetryAfter = time.Second

var (
	// ErrInvalidOption reports an incomplete Processor configuration.
	ErrInvalidOption = errors.New("cache invalidation: invalid option")
	errTargetPanic   = errors.New("cache invalidation: target panicked")
)

// Target atomically applies versioned invalidation entries.
type Target interface {
	InvalidateVersion(context.Context, string, uint64) (cache.InvalidationState, error)
}

// Config constructs one namespace-bound Processor.
type Config struct {
	Namespace  string
	Target     Target
	RetryAfter time.Duration
}

// Description is a key- and payload-free processing snapshot.
type Description struct {
	Applied  uint64
	Current  uint64
	Stale    uint64
	Failures uint64
	Invalid  uint64
	Panics   uint64
}

// Processor decodes invalidation events into a worker disposition.
type Processor struct {
	namespace  string
	target     Target
	retryAfter time.Duration

	applied  atomic.Uint64
	current  atomic.Uint64
	stale    atomic.Uint64
	failures atomic.Uint64
	invalid  atomic.Uint64
	panics   atomic.Uint64
}

// New constructs a Processor for exactly one cache namespace.
func New(config Config) (*Processor, error) {
	if !validIdentity(config.Namespace, maxNamespaceBytes) || isNil(config.Target) {
		return nil, fmt.Errorf("%w: namespace or target", ErrInvalidOption)
	}
	if config.RetryAfter == 0 {
		config.RetryAfter = defaultRetryAfter
	}
	if config.RetryAfter < 0 {
		return nil, fmt.Errorf("%w: retry delay", ErrInvalidOption)
	}
	return &Processor{
		namespace:  config.Namespace,
		target:     config.Target,
		retryAfter: config.RetryAfter,
	}, nil
}

// Handle applies every entry. A partial backend failure is safe to retry:
// already-applied versions become Current on the next delivery.
func (processor *Processor) Handle(ctx context.Context, message worker.Message) worker.Result {
	if processor == nil || ctx == nil {
		return worker.Nack(fmt.Errorf("%w: processor or context", ErrInvalidOption))
	}
	event, err := Decode(message.Payload())
	if err != nil || event.Namespace != processor.namespace {
		processor.invalid.Add(1)
		if err == nil {
			err = fmt.Errorf("%w: namespace mismatch", ErrInvalidEvent)
		}
		return worker.DeadLetter(err)
	}
	for _, entry := range event.Entries {
		state, applyErr := processor.apply(
			ctx,
			entry.Key,
			entry.Version,
		)
		if applyErr != nil {
			processor.failures.Add(1)
			return worker.Retry(fmt.Errorf("cache invalidation: apply: %w", applyErr), processor.retryAfter)
		}
		switch state {
		case cache.InvalidationApplied:
			processor.applied.Add(1)
		case cache.InvalidationCurrent:
			processor.current.Add(1)
		case cache.InvalidationStale:
			processor.stale.Add(1)
		default:
			processor.failures.Add(1)
			return worker.Nack(fmt.Errorf("%w: target state", ErrInvalidOption))
		}
	}
	return worker.Ack()
}

// Description returns bounded result counters.
func (processor *Processor) Description() Description {
	if processor == nil {
		return Description{}
	}
	return Description{
		Applied:  processor.applied.Load(),
		Current:  processor.current.Load(),
		Stale:    processor.stale.Load(),
		Failures: processor.failures.Load(),
		Invalid:  processor.invalid.Load(),
		Panics:   processor.panics.Load(),
	}
}

func (processor *Processor) apply(ctx context.Context, key string, version uint64) (state cache.InvalidationState, err error) {
	defer func() {
		if recover() != nil {
			processor.panics.Add(1)
			state = 0
			err = errTargetPanic
		}
	}()
	return processor.target.InvalidateVersion(ctx, key, version)
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
