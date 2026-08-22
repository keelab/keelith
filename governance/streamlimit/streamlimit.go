// Package streamlimit provides per-stream quotas and shared message limits.
package streamlimit

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/governance/ratelimit"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

var (
	// ErrLimited reports a per-stream or shared message limit rejection.
	ErrLimited = kerrors.New(
		429,
		"STREAM_MESSAGE_LIMITED",
		"stream message limit is exceeded",
	)
	// ErrInvalidOption reports a missing dependency or invalid event sequence.
	ErrInvalidOption = errors.New("streamlimit: invalid option")
)

// Description is a bounded aggregate stream limiter snapshot.
type Description struct {
	ActiveStreams    int64
	StartedStreams   uint64
	FinishedStreams  uint64
	SentMessages     uint64
	ReceivedMessages uint64
	RejectedMessages uint64
}

// Limiter creates per-stream state while sharing rate/concurrency pools.
type Limiter struct {
	resolver policy.Resolver
	pool     *ratelimit.Pool

	activeStreams    atomic.Int64
	startedStreams   atomic.Uint64
	finishedStreams  atomic.Uint64
	sentMessages     atomic.Uint64
	receivedMessages atomic.Uint64
	rejectedMessages atomic.Uint64
}

// New creates an isolated stream message Limiter.
func New(resolver policy.Resolver, pool *ratelimit.Pool) (*Limiter, error) {
	if isNil(resolver) || pool == nil {
		return nil, fmt.Errorf("%w: resolver or rate-limit pool is nil", ErrInvalidOption)
	}
	return &Limiter{resolver: resolver, pool: pool}, nil
}

// Middleware returns a factory whose mutable state is scoped to one stream.
func (l *Limiter) Middleware() middleware.StreamMiddleware {
	return func(next middleware.StreamHandler) middleware.StreamHandler {
		if next == nil {
			return func(context.Context, middleware.StreamEvent) error {
				return fmt.Errorf("%w: next stream handler is nil", ErrInvalidOption)
			}
		}

		var (
			mu        sync.Mutex
			created   bool
			finished  bool
			config    policy.StreamPolicy
			sharedKey string
			sent      int
			received  int
		)
		return func(ctx context.Context, event middleware.StreamEvent) error {
			if l == nil {
				return fmt.Errorf("%w: limiter is nil", ErrInvalidOption)
			}
			if ctx == nil {
				return fmt.Errorf("%w: context is nil", ErrInvalidOption)
			}
			switch event.Phase {
			case middleware.StreamPhaseCreate:
				target, ok := operation.FromContext(ctx)
				if !ok {
					return fmt.Errorf("%w: operation is missing", ErrInvalidOption)
				}
				resolved := policy.Resolve(
					ctx,
					l.resolver,
					target,
				).Stream
				if err := policy.ValidateStream(resolved); err != nil {
					return err
				}
				mu.Lock()
				if created {
					mu.Unlock()
					return fmt.Errorf("%w: duplicate create event", ErrInvalidOption)
				}
				config = resolved
				sharedKey = target.PolicyKey() + "/stream-messages"
				mu.Unlock()
				if err := next(ctx, event); err != nil {
					return err
				}
				mu.Lock()
				created = true
				enabled := config.Enabled
				mu.Unlock()
				if enabled {
					l.activeStreams.Add(1)
					l.startedStreams.Add(1)
				}
				return nil

			case middleware.StreamPhaseSend,
				middleware.StreamPhaseReceive:
				mu.Lock()
				if !created || finished {
					mu.Unlock()
					return fmt.Errorf("%w: message outside active stream", ErrInvalidOption)
				}
				current := config
				key := sharedKey
				mu.Unlock()
				if !current.Enabled {
					return next(ctx, event)
				}
				if event.Phase == middleware.StreamPhaseSend {
					mu.Lock()
					if current.MaxSendMessages > 0 &&
						sent >= current.MaxSendMessages {
						mu.Unlock()
						l.rejectedMessages.Add(1)
						return limitError(nil, event.Phase)
					}
					sent++
					mu.Unlock()
					permits, err := l.acquireBeforeMessage(
						key,
						current,
					)
					if err != nil {
						mu.Lock()
						sent--
						mu.Unlock()
						l.rejectedMessages.Add(1)
						return limitError(err, event.Phase)
					}
					defer releasePermits(permits)
					if err := next(ctx, event); err != nil {
						mu.Lock()
						sent--
						mu.Unlock()
						return err
					}
					l.sentMessages.Add(1)
					return nil
				}

				concurrencyPermit, err := l.acquireConcurrency(
					key,
					current.MaxConcurrency,
				)
				if err != nil {
					l.rejectedMessages.Add(1)
					return limitError(err, event.Phase)
				}
				if err := next(ctx, event); err != nil {
					concurrencyPermit.Release()
					return err
				}
				concurrencyPermit.Release()
				mu.Lock()
				if current.MaxReceiveMessages > 0 &&
					received >= current.MaxReceiveMessages {
					mu.Unlock()
					l.rejectedMessages.Add(1)
					return limitError(nil, event.Phase)
				}
				received++
				mu.Unlock()
				ratePermit, err := l.acquireRate(key, current)
				if err != nil {
					mu.Lock()
					received--
					mu.Unlock()
					l.rejectedMessages.Add(1)
					return limitError(err, event.Phase)
				}
				ratePermit.Release()
				l.receivedMessages.Add(1)
				return nil

			case middleware.StreamPhaseFinish:
				mu.Lock()
				if !created || finished {
					mu.Unlock()
					return next(ctx, event)
				}
				finished = true
				enabled := config.Enabled
				mu.Unlock()
				if enabled {
					defer func() {
						l.activeStreams.Add(-1)
						l.finishedStreams.Add(1)
					}()
				}
				return next(ctx, event)

			default:
				return fmt.Errorf("%w: unsupported phase %q", ErrInvalidOption, event.Phase)
			}
		}
	}
}

// Describe returns low-cardinality aggregate counters.
func (l *Limiter) Describe() Description {
	if l == nil {
		return Description{}
	}
	return Description{
		ActiveStreams:    l.activeStreams.Load(),
		StartedStreams:   l.startedStreams.Load(),
		FinishedStreams:  l.finishedStreams.Load(),
		SentMessages:     l.sentMessages.Load(),
		ReceivedMessages: l.receivedMessages.Load(),
		RejectedMessages: l.rejectedMessages.Load(),
	}
}

func (l *Limiter) acquireBeforeMessage(key string, config policy.StreamPolicy) ([]*ratelimit.Permit, error) {
	permits := make([]*ratelimit.Permit, 0, 2)
	concurrencyPermit, err := l.acquireConcurrency(
		key,
		config.MaxConcurrency,
	)
	if err != nil {
		return nil, err
	}
	permits = append(permits, concurrencyPermit)
	ratePermit, err := l.acquireRate(key, config)
	if err != nil {
		releasePermits(permits)
		return nil, err
	}
	permits = append(permits, ratePermit)
	return permits, nil
}

func (l *Limiter) acquireConcurrency(key string, maximum int) (*ratelimit.Permit, error) {
	if maximum <= 0 {
		return &ratelimit.Permit{}, nil
	}
	return l.pool.Get(key + "/concurrency").Acquire(
		policy.RateLimitPolicy{
			Enabled:        true,
			MaxConcurrency: maximum,
		},
	)
}

func (l *Limiter) acquireRate(key string, config policy.StreamPolicy) (*ratelimit.Permit, error) {
	if config.MessagesPerSecond <= 0 {
		return &ratelimit.Permit{}, nil
	}
	return l.pool.Get(key + "/rate").Acquire(policy.RateLimitPolicy{
		Enabled:           true,
		RequestsPerSecond: config.MessagesPerSecond,
		Burst:             config.Burst,
	})
}

func releasePermits(permits []*ratelimit.Permit) {
	for index := len(permits) - 1; index >= 0; index-- {
		permits[index].Release()
	}
}

func limitError(cause error, phase middleware.StreamPhase) error {
	message := "stream " + string(phase) + " message limit is exceeded"
	if cause == nil {
		return ErrLimited.Clone(kerrors.WithMessage(message))
	}
	return kerrors.Wrap(
		cause,
		ErrLimited.Code(),
		ErrLimited.Reason(),
		message,
	)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
