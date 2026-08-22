// Package inbox provides broker-neutral transactional consumer idempotency.
package inbox

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/worker"
)

const (
	// OutboxIDHeader is the stable metadata key emitted by Keelith publishers.
	OutboxIDHeader = "keelith-outbox-id"

	defaultRetryAfter = time.Second
	maxKeyBytes       = 512
)

var (
	// ErrInvalidOption reports an incomplete processor or malformed key.
	ErrInvalidOption = errors.New("inbox: invalid option")
	// ErrInvalidDecision reports a broken Executor/Processor contract.
	ErrInvalidDecision = errors.New("inbox: invalid transaction decision")
	errHandlerPanic    = errors.New("inbox: handler panicked")
)

// Decision tells an Executor whether to commit or roll back the transaction.
type Decision uint8

const (
	// DecisionCommit persists the business effects and inbox record together.
	DecisionCommit Decision = iota + 1
	// DecisionRollback discards both the business effects and inbox record.
	DecisionRollback
)

// Outcome reports how an Executor settled one idempotency key.
type Outcome uint8

const (
	// OutcomeApplied means the callback and inbox record committed atomically.
	OutcomeApplied Outcome = iota + 1
	// OutcomeDuplicate means the key had already committed.
	OutcomeDuplicate
	// OutcomeRolledBack means the callback requested rollback.
	OutcomeRolledBack
)

// Key isolates one message identity inside a stable consumer projection.
type Key struct {
	Consumer string
	Message  string
}

// Validate rejects ambiguous or unbounded idempotency keys.
func (key Key) Validate() error {
	if !validIdentity(key.Consumer, 256) || !validIdentity(key.Message, maxKeyBytes) {
		return fmt.Errorf("%w: idempotency key", ErrInvalidOption)
	}
	return nil
}

// TransactionFunc runs business work inside an Executor-owned transaction.
type TransactionFunc[Transaction any] func(context.Context, Transaction) Decision

// Executor atomically records a key and commits callback business effects.
//
// Concurrent executions of the same key must not both call the callback.
// A rolled-back callback must not leave a durable idempotency record.
type Executor[Transaction any] interface {
	Execute(context.Context, Key, TransactionFunc[Transaction]) (Outcome, error)
}

// KeyResolver derives one stable idempotency key from a delivery.
type KeyResolver func(context.Context, worker.Message) (string, error)

// Handler performs business work using the Executor-owned transaction.
type Handler[Transaction any] func(context.Context, Transaction, worker.Message) worker.Result

// Config constructs one Processor.
type Config[Transaction any] struct {
	Consumer   string
	Executor   Executor[Transaction]
	Handler    Handler[Transaction]
	ResolveKey KeyResolver
	RetryAfter time.Duration
}

// Description is a payload- and key-free Processor snapshot.
type Description struct {
	Applied    uint64
	Duplicates uint64
	RolledBack uint64
	Failures   uint64
	Panics     uint64
}

// RetentionDescription is a datastore-, consumer-, and schedule-free
// aggregate snapshot for bounded Inbox cleanup.
type RetentionDescription struct {
	Active     int64
	Runs       uint64
	Batches    uint64
	Purged     uint64
	Incomplete uint64
	Failures   uint64
}

// Processor wraps a transactional business Handler as worker.ConsumerHandler.
type Processor[Transaction any] struct {
	consumer   string
	executor   Executor[Transaction]
	handler    Handler[Transaction]
	resolveKey KeyResolver
	retryAfter time.Duration

	applied    atomic.Uint64
	duplicates atomic.Uint64
	rolledBack atomic.Uint64
	failures   atomic.Uint64
	panics     atomic.Uint64
}

// New constructs a transactional inbox Processor.
func New[Transaction any](config Config[Transaction]) (*Processor[Transaction], error) {
	if !validIdentity(config.Consumer, 256) ||
		isNil(config.Executor) ||
		config.Handler == nil {
		return nil, fmt.Errorf("%w: consumer, executor, or handler", ErrInvalidOption)
	}
	if config.ResolveKey == nil {
		config.ResolveKey = DefaultKey
	}
	if config.RetryAfter == 0 {
		config.RetryAfter = defaultRetryAfter
	}
	if config.RetryAfter < 0 {
		return nil, fmt.Errorf("%w: retry delay", ErrInvalidOption)
	}
	return &Processor[Transaction]{
		consumer:   config.Consumer,
		executor:   config.Executor,
		handler:    config.Handler,
		resolveKey: config.ResolveKey,
		retryAfter: config.RetryAfter,
	}, nil
}

// Handle executes one message once relative to transaction-local side effects.
func (p *Processor[Transaction]) Handle(ctx context.Context, message worker.Message) worker.Result {
	if p == nil || ctx == nil {
		return worker.Nack(fmt.Errorf("%w: processor or context", ErrInvalidOption))
	}
	messageKey, err := p.resolveKey(ctx, message)
	key := Key{
		Consumer: p.consumer,
		Message:  messageKey,
	}
	if err != nil || key.Validate() != nil {
		if err == nil {
			err = ErrInvalidOption
		}
		p.failures.Add(1)
		return worker.DeadLetter(fmt.Errorf("inbox: resolve key: %w", err))
	}

	businessResult := worker.Nack(ErrInvalidDecision)
	outcome, err := p.executor.Execute(ctx, key, func(transactionCtx context.Context, transaction Transaction) Decision {
		businessResult = p.invoke(
			transactionCtx,
			transaction,
			message,
		)
		switch businessResult.Action() {
		case worker.ActionAck:
			return DecisionCommit
		case worker.ActionNack,
			worker.ActionRetry,
			worker.ActionDeadLetter:
			return DecisionRollback
		default:
			businessResult = worker.Nack(ErrInvalidDecision)
			return DecisionRollback
		}
	},
	)
	if err != nil {
		p.failures.Add(1)
		return worker.Retry(
			fmt.Errorf("inbox: execute: %w", err),
			p.retryAfter,
		)
	}

	switch outcome {
	case OutcomeApplied:
		if businessResult.Action() != worker.ActionAck {
			p.failures.Add(1)
			return worker.Nack(ErrInvalidDecision)
		}
		p.applied.Add(1)
		return businessResult
	case OutcomeDuplicate:
		p.duplicates.Add(1)
		return worker.Ack()
	case OutcomeRolledBack:
		if businessResult.Action() == worker.ActionAck {
			p.failures.Add(1)
			return worker.Nack(ErrInvalidDecision)
		}
		p.rolledBack.Add(1)
		return businessResult
	default:
		p.failures.Add(1)
		return worker.Nack(ErrInvalidDecision)
	}
}

// Description returns bounded outcome counters without message identities.
func (p *Processor[Transaction]) Description() Description {
	if p == nil {
		return Description{}
	}
	return Description{
		Applied:    p.applied.Load(),
		Duplicates: p.duplicates.Load(),
		RolledBack: p.rolledBack.Load(),
		Failures:   p.failures.Load(),
		Panics:     p.panics.Load(),
	}
}

// DefaultKey prefers a globally unique Keelith outbox ID and otherwise uses
// the adapter delivery identity.
func DefaultKey(_ context.Context, message worker.Message) (string, error) {
	values := message.Metadata().Values(OutboxIDHeader)
	switch len(values) {
	case 0:
		if !validIdentity(message.ID(), maxKeyBytes-len("delivery/")) {
			return "", fmt.Errorf("%w: delivery ID", ErrInvalidOption)
		}
		return "delivery/" + message.ID(), nil
	case 1:
		if !validIdentity(values[0], maxKeyBytes-len("outbox/")) {
			return "", fmt.Errorf("%w: outbox ID", ErrInvalidOption)
		}
		return "outbox/" + values[0], nil
	default:
		return "", fmt.Errorf("%w: multiple outbox IDs", ErrInvalidOption)
	}
}

func (p *Processor[Transaction]) invoke(ctx context.Context, transaction Transaction, message worker.Message) (result worker.Result) {
	defer func() {
		if recover() != nil {
			p.panics.Add(1)
			result = worker.Nack(errHandlerPanic)
		}
	}()
	return p.handler(ctx, transaction, message)
}

func validIdentity(value string, maxBytes int) bool {
	if value == "" ||
		len(value) > maxBytes ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) {
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
