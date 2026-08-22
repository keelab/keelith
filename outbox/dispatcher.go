package outbox

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultPollInterval   = 250 * time.Millisecond
	defaultErrorDelay     = time.Second
	defaultLeaseTTL       = 30 * time.Second
	defaultPublishTimeout = 10 * time.Second
	defaultBatchSize      = 100
	defaultMaxAttempts    = 20
)

// Config controls one durable outbox Dispatcher.
type Config struct {
	Name            string
	Owner           string
	Repository      Repository
	Publisher       Publisher
	PollInterval    time.Duration
	ErrorDelay      time.Duration
	LeaseTTL        time.Duration
	PublishTimeout  time.Duration
	BatchSize       int
	MaxAttempts     int
	RetryBase       time.Duration
	RetryMax        time.Duration
	ClassifyFailure FailureClassifier
}

// Description is a payload-free Dispatcher snapshot.
type Description struct {
	Name                          string
	Running                       bool
	StopRequested                 bool
	Finished                      bool
	Failed                        bool
	InFlight                      int
	Claimed                       uint64
	Published                     uint64
	Rescheduled                   uint64
	Terminal                      uint64
	RepositoryFailures            uint64
	PublisherFailures             uint64
	ConsecutiveRepositoryFailures uint64
	ConsecutivePublisherFailures  uint64
}

// Dispatcher claims, publishes, and settles durable outbox rows.
type Dispatcher struct {
	config Config

	mu             sync.Mutex
	started        bool
	stopRequested  bool
	finished       bool
	inflight       int
	pollCancel     context.CancelCauseFunc
	deliveryCancel context.CancelCauseFunc
	runErr         error

	done     chan struct{}
	stopOnce sync.Once

	claimed                       atomic.Uint64
	published                     atomic.Uint64
	rescheduled                   atomic.Uint64
	terminal                      atomic.Uint64
	repositoryFailures            atomic.Uint64
	publisherFailures             atomic.Uint64
	consecutiveRepositoryFailures atomic.Uint64
	consecutivePublisherFailures  atomic.Uint64
}

// New validates and constructs a Dispatcher without starting goroutines.
func New(config Config) (*Dispatcher, error) {
	if !validIdentity(config.Name, maxIDBytes) ||
		!validIdentity(config.Owner, maxIDBytes) ||
		isNil(config.Repository) ||
		isNil(config.Publisher) {
		return nil, fmt.Errorf(
			"%w: name, owner, repository, or publisher",
			ErrInvalidOption,
		)
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.ErrorDelay == 0 {
		config.ErrorDelay = defaultErrorDelay
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = defaultLeaseTTL
	}
	if config.PublishTimeout == 0 {
		config.PublishTimeout = defaultPublishTimeout
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = defaultMaxAttempts
	}
	if config.RetryBase == 0 {
		config.RetryBase = time.Second
	}
	if config.RetryMax == 0 {
		config.RetryMax = time.Minute
	}
	if config.PollInterval < 0 ||
		config.ErrorDelay < 0 ||
		config.LeaseTTL <= 0 ||
		config.PublishTimeout <= 0 ||
		config.PublishTimeout >= config.LeaseTTL ||
		config.BatchSize <= 0 ||
		config.BatchSize > 10_000 ||
		config.MaxAttempts <= 0 ||
		config.RetryBase <= 0 ||
		config.RetryMax < config.RetryBase {
		return nil, fmt.Errorf("%w: dispatcher budgets", ErrInvalidOption)
	}
	if config.ClassifyFailure == nil {
		config.ClassifyFailure = func(error) string {
			return "publish_failed"
		}
	}
	return &Dispatcher{
		config: config,
		done:   make(chan struct{}),
	}, nil
}

// Name returns the stable App server identity.
func (dispatcher *Dispatcher) Name() string {
	if dispatcher == nil {
		return ""
	}
	return dispatcher.config.Name
}

// Start begins polling and returns after the runtime is ready.
func (dispatcher *Dispatcher) Start(ctx context.Context) error {
	if dispatcher == nil {
		return fmt.Errorf("%w: dispatcher is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	dispatcher.mu.Lock()
	if dispatcher.started {
		dispatcher.mu.Unlock()
		return fmt.Errorf("%w: already started", ErrInvalidOption)
	}
	pollCtx, pollCancel := context.WithCancelCause(context.Background())
	deliveryCtx, deliveryCancel := context.WithCancelCause(context.Background())
	dispatcher.pollCancel = pollCancel
	dispatcher.deliveryCancel = deliveryCancel
	dispatcher.started = true
	dispatcher.mu.Unlock()
	go dispatcher.run(pollCtx, deliveryCtx)
	return nil
}

// Stop prevents new claims and drains the current bounded batch.
func (dispatcher *Dispatcher) Stop(ctx context.Context) error {
	if dispatcher == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	dispatcher.mu.Lock()
	if !dispatcher.started {
		dispatcher.mu.Unlock()
		return nil
	}
	dispatcher.stopRequested = true
	pollCancel := dispatcher.pollCancel
	deliveryCancel := dispatcher.deliveryCancel
	dispatcher.mu.Unlock()
	dispatcher.stopOnce.Do(func() {
		if pollCancel != nil {
			pollCancel(context.Canceled)
		}
	})
	select {
	case <-dispatcher.done:
		return dispatcher.waitError()
	case <-ctx.Done():
		if deliveryCancel != nil {
			deliveryCancel(context.Cause(ctx))
		}
		return context.Cause(ctx)
	}
}

// Wait reports terminal runtime failure after Start.
func (dispatcher *Dispatcher) Wait() error {
	if dispatcher == nil {
		return nil
	}
	dispatcher.mu.Lock()
	started := dispatcher.started
	dispatcher.mu.Unlock()
	if !started {
		return ErrNotStarted
	}
	<-dispatcher.done
	return dispatcher.waitError()
}

// Description returns lifecycle and low-cardinality settlement counters.
func (dispatcher *Dispatcher) Description() Description {
	if dispatcher == nil {
		return Description{}
	}
	dispatcher.mu.Lock()
	description := Description{
		Name:          dispatcher.config.Name,
		Running:       dispatcher.started && !dispatcher.finished,
		StopRequested: dispatcher.stopRequested,
		Finished:      dispatcher.finished,
		Failed:        dispatcher.runErr != nil,
		InFlight:      dispatcher.inflight,
	}
	dispatcher.mu.Unlock()
	description.Claimed = dispatcher.claimed.Load()
	description.Published = dispatcher.published.Load()
	description.Rescheduled = dispatcher.rescheduled.Load()
	description.Terminal = dispatcher.terminal.Load()
	description.RepositoryFailures = dispatcher.repositoryFailures.Load()
	description.PublisherFailures = dispatcher.publisherFailures.Load()
	description.ConsecutiveRepositoryFailures =
		dispatcher.consecutiveRepositoryFailures.Load()
	description.ConsecutivePublisherFailures =
		dispatcher.consecutivePublisherFailures.Load()
	return description
}

func (dispatcher *Dispatcher) run(
	pollCtx context.Context,
	deliveryCtx context.Context,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			dispatcher.setRunError(fmt.Errorf("outbox: dispatcher panic"))
		}
		dispatcher.mu.Lock()
		dispatcher.finished = true
		dispatcher.inflight = 0
		dispatcher.mu.Unlock()
		close(dispatcher.done)
	}()
	for {
		if context.Cause(pollCtx) != nil {
			return
		}
		messages, err := dispatcher.config.Repository.Claim(
			pollCtx,
			ClaimRequest{
				Owner:      dispatcher.config.Owner,
				Limit:      dispatcher.config.BatchSize,
				LeaseUntil: time.Now().UTC().Add(dispatcher.config.LeaseTTL),
			},
		)
		if err != nil {
			if context.Cause(pollCtx) != nil {
				return
			}
			dispatcher.repositoryFailures.Add(1)
			dispatcher.consecutiveRepositoryFailures.Add(1)
			if !wait(pollCtx, dispatcher.config.ErrorDelay) {
				return
			}
			continue
		}
		dispatcher.consecutiveRepositoryFailures.Store(0)
		if len(messages) > dispatcher.config.BatchSize {
			dispatcher.repositoryFailures.Add(1)
			dispatcher.consecutiveRepositoryFailures.Add(1)
			dispatcher.setRunError(fmt.Errorf(
				"outbox: repository returned %d messages for limit %d",
				len(messages),
				dispatcher.config.BatchSize,
			))
			return
		}
		if len(messages) == 0 {
			if !wait(pollCtx, dispatcher.config.PollInterval) {
				return
			}
			continue
		}
		dispatcher.claimed.Add(uint64(len(messages)))
		for _, message := range messages {
			dispatcher.process(deliveryCtx, message)
		}
	}
}

func (dispatcher *Dispatcher) process(
	deliveryCtx context.Context,
	message Message,
) {
	if err := message.Validate(); err != nil || message.Attempts <= 0 {
		dispatcher.settleFailure(message, true, "invalid_message")
		return
	}
	dispatcher.mu.Lock()
	dispatcher.inflight++
	dispatcher.mu.Unlock()
	defer func() {
		dispatcher.mu.Lock()
		dispatcher.inflight--
		dispatcher.mu.Unlock()
	}()

	publishCtx, cancelPublish := context.WithTimeout(
		deliveryCtx,
		dispatcher.config.PublishTimeout,
	)
	err := dispatcher.publish(
		publishCtx,
		message.Clone(),
	)
	cancelPublish()
	if err == nil {
		dispatcher.consecutivePublisherFailures.Store(0)
		settlementCtx, cancelSettlement := dispatcher.settlementContext()
		err = dispatcher.config.Repository.Complete(
			settlementCtx,
			dispatcher.config.Owner,
			message.ID,
		)
		cancelSettlement()
		if err == nil {
			dispatcher.consecutiveRepositoryFailures.Store(0)
			dispatcher.published.Add(1)
			return
		}
		dispatcher.repositoryFailures.Add(1)
		dispatcher.consecutiveRepositoryFailures.Add(1)
		return
	}
	dispatcher.publisherFailures.Add(1)
	dispatcher.consecutivePublisherFailures.Add(1)
	terminal := message.Attempts >= dispatcher.config.MaxAttempts
	reason := dispatcher.classifyFailure(err)
	if !validIdentity(reason, 64) {
		reason = "publish_failed"
	}
	dispatcher.settleFailure(message, terminal, reason)
}

func (dispatcher *Dispatcher) publish(
	ctx context.Context,
	message Message,
) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("outbox: publisher panic")
		}
	}()
	return dispatcher.config.Publisher.Publish(ctx, message)
}

func (dispatcher *Dispatcher) classifyFailure(err error) (reason string) {
	defer func() {
		if recover() != nil {
			reason = "publish_failed"
		}
	}()
	return dispatcher.config.ClassifyFailure(err)
}

func (dispatcher *Dispatcher) settleFailure(
	message Message,
	terminal bool,
	reason string,
) {
	next := time.Now().UTC()
	if !terminal {
		next = next.Add(dispatcher.retryDelay(message.Attempts))
	}
	settlementCtx, cancel := dispatcher.settlementContext()
	err := dispatcher.config.Repository.Reschedule(
		settlementCtx,
		dispatcher.config.Owner,
		message.ID,
		next,
		terminal,
		reason,
	)
	cancel()
	if err != nil {
		dispatcher.repositoryFailures.Add(1)
		dispatcher.consecutiveRepositoryFailures.Add(1)
		return
	}
	dispatcher.consecutiveRepositoryFailures.Store(0)
	if terminal {
		dispatcher.terminal.Add(1)
	} else {
		dispatcher.rescheduled.Add(1)
	}
}

func (dispatcher *Dispatcher) retryDelay(attempts int) time.Duration {
	delay := dispatcher.config.RetryBase
	for attempt := 1; attempt < attempts; attempt++ {
		if delay >= dispatcher.config.RetryMax/2 {
			return dispatcher.config.RetryMax
		}
		delay *= 2
	}
	if delay > dispatcher.config.RetryMax {
		return dispatcher.config.RetryMax
	}
	return delay
}

func (dispatcher *Dispatcher) settlementContext() (
	context.Context,
	context.CancelFunc,
) {
	timeout := min(
		dispatcher.config.PublishTimeout,
		dispatcher.config.LeaseTTL/2,
	)
	return context.WithTimeout(context.Background(), timeout)
}

func (dispatcher *Dispatcher) setRunError(err error) {
	dispatcher.mu.Lock()
	dispatcher.runErr = err
	dispatcher.mu.Unlock()
}

func (dispatcher *Dispatcher) waitError() error {
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	return dispatcher.runErr
}

func wait(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return context.Cause(ctx) == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
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
