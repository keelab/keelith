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
func (d *Dispatcher) Name() string {
	if d == nil {
		return ""
	}
	return d.config.Name
}

// Start begins polling and returns after the runtime is ready.
func (d *Dispatcher) Start(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("%w: dispatcher is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return fmt.Errorf("%w: already started", ErrInvalidOption)
	}
	pollCtx, pollCancel := context.WithCancelCause(context.Background())
	deliveryCtx, deliveryCancel := context.WithCancelCause(context.Background())
	d.pollCancel = pollCancel
	d.deliveryCancel = deliveryCancel
	d.started = true
	d.mu.Unlock()
	go d.run(pollCtx, deliveryCtx)
	return nil
}

// Stop prevents new claims and drains the current bounded batch.
func (d *Dispatcher) Stop(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	d.mu.Lock()
	if !d.started {
		d.mu.Unlock()
		return nil
	}
	d.stopRequested = true
	pollCancel := d.pollCancel
	deliveryCancel := d.deliveryCancel
	d.mu.Unlock()
	d.stopOnce.Do(func() {
		if pollCancel != nil {
			pollCancel(context.Canceled)
		}
	})
	select {
	case <-d.done:
		return d.waitError()
	case <-ctx.Done():
		if deliveryCancel != nil {
			deliveryCancel(context.Cause(ctx))
		}
		return context.Cause(ctx)
	}
}

// Wait reports terminal runtime failure after Start.
func (d *Dispatcher) Wait() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	started := d.started
	d.mu.Unlock()
	if !started {
		return ErrNotStarted
	}
	<-d.done
	return d.waitError()
}

// Description returns lifecycle and low-cardinality settlement counters.
func (d *Dispatcher) Description() Description {
	if d == nil {
		return Description{}
	}
	d.mu.Lock()
	description := Description{
		Name:          d.config.Name,
		Running:       d.started && !d.finished,
		StopRequested: d.stopRequested,
		Finished:      d.finished,
		Failed:        d.runErr != nil,
		InFlight:      d.inflight,
	}
	d.mu.Unlock()
	description.Claimed = d.claimed.Load()
	description.Published = d.published.Load()
	description.Rescheduled = d.rescheduled.Load()
	description.Terminal = d.terminal.Load()
	description.RepositoryFailures = d.repositoryFailures.Load()
	description.PublisherFailures = d.publisherFailures.Load()
	description.ConsecutiveRepositoryFailures =
		d.consecutiveRepositoryFailures.Load()
	description.ConsecutivePublisherFailures =
		d.consecutivePublisherFailures.Load()
	return description
}

func (d *Dispatcher) run(
	pollCtx context.Context,
	deliveryCtx context.Context,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			d.setRunError(fmt.Errorf("outbox: dispatcher panic"))
		}
		d.mu.Lock()
		d.finished = true
		d.inflight = 0
		d.mu.Unlock()
		close(d.done)
	}()
	for {
		if context.Cause(pollCtx) != nil {
			return
		}
		messages, err := d.config.Repository.Claim(
			pollCtx,
			ClaimRequest{
				Owner:      d.config.Owner,
				Limit:      d.config.BatchSize,
				LeaseUntil: time.Now().UTC().Add(d.config.LeaseTTL),
			},
		)
		if err != nil {
			if context.Cause(pollCtx) != nil {
				return
			}
			d.repositoryFailures.Add(1)
			d.consecutiveRepositoryFailures.Add(1)
			if !wait(pollCtx, d.config.ErrorDelay) {
				return
			}
			continue
		}
		d.consecutiveRepositoryFailures.Store(0)
		if len(messages) > d.config.BatchSize {
			d.repositoryFailures.Add(1)
			d.consecutiveRepositoryFailures.Add(1)
			d.setRunError(fmt.Errorf(
				"outbox: repository returned %d messages for limit %d",
				len(messages),
				d.config.BatchSize,
			))
			return
		}
		if len(messages) == 0 {
			if !wait(pollCtx, d.config.PollInterval) {
				return
			}
			continue
		}
		d.claimed.Add(uint64(len(messages)))
		for _, message := range messages {
			d.process(deliveryCtx, message)
		}
	}
}

func (d *Dispatcher) process(
	deliveryCtx context.Context,
	message Message,
) {
	if err := message.Validate(); err != nil || message.Attempts <= 0 {
		d.settleFailure(message, true, "invalid_message")
		return
	}
	d.mu.Lock()
	d.inflight++
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.inflight--
		d.mu.Unlock()
	}()

	publishCtx, cancelPublish := context.WithTimeout(
		deliveryCtx,
		d.config.PublishTimeout,
	)
	err := d.publish(
		publishCtx,
		message.Clone(),
	)
	cancelPublish()
	if err == nil {
		d.consecutivePublisherFailures.Store(0)
		settlementCtx, cancelSettlement := d.settlementContext()
		err = d.config.Repository.Complete(
			settlementCtx,
			d.config.Owner,
			message.ID,
		)
		cancelSettlement()
		if err == nil {
			d.consecutiveRepositoryFailures.Store(0)
			d.published.Add(1)
			return
		}
		d.repositoryFailures.Add(1)
		d.consecutiveRepositoryFailures.Add(1)
		return
	}
	d.publisherFailures.Add(1)
	d.consecutivePublisherFailures.Add(1)
	terminal := message.Attempts >= d.config.MaxAttempts
	reason := d.classifyFailure(err)
	if !validIdentity(reason, 64) {
		reason = "publish_failed"
	}
	d.settleFailure(message, terminal, reason)
}

func (d *Dispatcher) publish(
	ctx context.Context,
	message Message,
) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("outbox: publisher panic")
		}
	}()
	return d.config.Publisher.Publish(ctx, message)
}

func (d *Dispatcher) classifyFailure(err error) (reason string) {
	defer func() {
		if recover() != nil {
			reason = "publish_failed"
		}
	}()
	return d.config.ClassifyFailure(err)
}

func (d *Dispatcher) settleFailure(
	message Message,
	terminal bool,
	reason string,
) {
	next := time.Now().UTC()
	if !terminal {
		next = next.Add(d.retryDelay(message.Attempts))
	}
	settlementCtx, cancel := d.settlementContext()
	err := d.config.Repository.Reschedule(
		settlementCtx,
		d.config.Owner,
		message.ID,
		next,
		terminal,
		reason,
	)
	cancel()
	if err != nil {
		d.repositoryFailures.Add(1)
		d.consecutiveRepositoryFailures.Add(1)
		return
	}
	d.consecutiveRepositoryFailures.Store(0)
	if terminal {
		d.terminal.Add(1)
	} else {
		d.rescheduled.Add(1)
	}
}

func (d *Dispatcher) retryDelay(attempts int) time.Duration {
	delay := d.config.RetryBase
	for attempt := 1; attempt < attempts; attempt++ {
		if delay >= d.config.RetryMax/2 {
			return d.config.RetryMax
		}
		delay *= 2
	}
	if delay > d.config.RetryMax {
		return d.config.RetryMax
	}
	return delay
}

func (d *Dispatcher) settlementContext() (
	context.Context,
	context.CancelFunc,
) {
	timeout := min(
		d.config.PublishTimeout,
		d.config.LeaseTTL/2,
	)
	return context.WithTimeout(context.Background(), timeout)
}

func (d *Dispatcher) setRunError(err error) {
	d.mu.Lock()
	d.runErr = err
	d.mu.Unlock()
}

func (d *Dispatcher) waitError() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.runErr
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
