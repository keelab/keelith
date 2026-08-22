// Package dependency assembles coherent outbound dependency governance.
package dependency

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/keelab/keelith/governance/admission"
	"github.com/keelab/keelith/governance/attempt"
	"github.com/keelab/keelith/governance/breaker"
	"github.com/keelab/keelith/governance/bulkhead"
	"github.com/keelab/keelith/governance/hedging"
	"github.com/keelab/keelith/governance/outlier"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/governance/retry"
	ktimeout "github.com/keelab/keelith/governance/timeout"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/selector"
)

const (
	defaultOutlierFailures = 5
	defaultOutlierEjection = 30 * time.Second
	defaultRetryBurst      = 1
	defaultSource          = "dependency-runtime"
)

var (
	// ErrInvalidConfig means a dependency Runtime cannot be assembled safely.
	ErrInvalidConfig = errors.New("dependency: invalid config")
	// ErrMissingOperation means outbound middleware did not receive an
	// Operation from its transport adapter.
	ErrMissingOperation = errors.New("dependency: operation is missing")
)

// Config defines one coherent outbound dependency-governance runtime.
//
// The Runtime owns its service-breaker pool and retry budget unless explicit
// shared instances are supplied. InstanceOutlier is defaulted when omitted.
type Config struct {
	Resolver               policy.Resolver
	Admission              admission.Resolver
	AdmissionOptions       []admission.Option
	Breakers               *breaker.Pool
	Bulkheads              *bulkhead.Pool
	RetryBudget            *retry.Budget
	RetryOptions           []retry.Option
	HedgingOptions         []hedging.Option
	InstanceOutlier        outlier.Config
	DisableInstanceOutlier bool
	Source                 string
}

// Description is an immutable operational snapshot.
type Description struct {
	Middleware []middleware.Description
	Admission  admission.Description
	Breakers   []breaker.Description
	Bulkheads  []bulkhead.Description
	Retry      []retry.BudgetDescription
	Instances  []outlier.Status
}

// Runtime coordinates aggregate service health, attempt amplification, and
// per-instance passive health.
//
// Its middleware order is fixed:
//
//	policy snapshot -> optional dynamic admission -> logical-call timeout -> dependency bulkhead
//	-> service breaker -> retry/hedging dispatcher
//
// The timeout and service breaker therefore observe one final logical call
// while the selector Observer sees every selected transport attempt.
type Runtime struct {
	bundle       *middleware.Bundle
	admission    *admission.Controller
	breakers     *breaker.Pool
	bulkheads    *bulkhead.Pool
	retryBudget  *retry.Budget
	instanceTier *outlier.Detector
}

// New assembles an outbound dependency Runtime.
func New(config Config) (*Runtime, error) {
	if isNil(config.Resolver) {
		return nil, fmt.Errorf("%w: resolver is nil", ErrInvalidConfig)
	}

	breakers := config.Breakers
	if breakers == nil {
		breakers = breaker.NewPool(nil)
	}
	bulkheads := config.Bulkheads
	if bulkheads == nil {
		bulkheads = bulkhead.NewPool(nil)
	}
	budget := config.RetryBudget
	if budget == nil {
		budget = retry.NewBudget(defaultRetryBurst)
	}
	instanceTier, err := newInstanceTier(config)
	if err != nil {
		return nil, err
	}
	var admissionController *admission.Controller
	if !isNil(config.Admission) {
		admissionController, err = admission.New(
			config.Admission,
			config.AdmissionOptions...,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: admission: %w",
				ErrInvalidConfig,
				err,
			)
		}
	}

	serviceBreaker, err := breaker.NewService(config.Resolver, breakers)
	if err != nil {
		return nil, fmt.Errorf("%w: service breaker: %w", ErrInvalidConfig, err)
	}
	callTimeout, err := ktimeout.New(config.Resolver)
	if err != nil {
		return nil, fmt.Errorf("%w: timeout: %w", ErrInvalidConfig, err)
	}
	dependencyBulkhead, err := bulkhead.New(config.Resolver, bulkheads)
	if err != nil {
		return nil, fmt.Errorf("%w: bulkhead: %w", ErrInvalidConfig, err)
	}
	retryOptions := append([]retry.Option(nil), config.RetryOptions...)
	retryOptions = append(retryOptions, retry.WithBudget(budget))
	retryMiddleware, err := retry.New(config.Resolver, retryOptions...)
	if err != nil {
		return nil, fmt.Errorf("%w: retry: %w", ErrInvalidConfig, err)
	}
	hedgingMiddleware, err := hedging.New(
		config.Resolver,
		config.HedgingOptions...,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: hedging: %w", ErrInvalidConfig, err)
	}
	attempts := dispatchAttempts(
		config.Resolver,
		retryMiddleware,
		hedgingMiddleware,
	)
	source := strings.TrimSpace(config.Source)
	if source == "" {
		source = defaultSource
	}
	entries := []middleware.Entry{
		{
			Name:       "policy-snapshot",
			Source:     source,
			Middleware: snapshotPolicy(config.Resolver),
		},
	}
	if admissionController != nil {
		entries = append(entries, middleware.Entry{
			Name:       "admission",
			Source:     source,
			Middleware: admissionController.Middleware(),
		})
	}
	entries = append(entries,
		middleware.Entry{
			Name:       "timeout",
			Source:     source,
			Middleware: callTimeout,
		},
		middleware.Entry{
			Name:       "dependency-bulkhead",
			Source:     source,
			Middleware: dependencyBulkhead,
		},
		middleware.Entry{
			Name:       "service-breaker",
			Source:     source,
			Middleware: serviceBreaker,
		},
		middleware.Entry{
			Name:       "attempts",
			Source:     source,
			Middleware: attempts,
		},
	)
	bundle, err := middleware.NewBundle(entries...)
	if err != nil {
		return nil, fmt.Errorf("%w: middleware bundle: %w", ErrInvalidConfig, err)
	}
	return &Runtime{
		bundle:       bundle,
		admission:    admissionController,
		breakers:     breakers,
		bulkheads:    bulkheads,
		retryBudget:  budget,
		instanceTier: instanceTier,
	}, nil
}

// Middleware returns the immutable client middleware bundle.
func (r *Runtime) Middleware() *middleware.Bundle {
	if r == nil {
		return nil
	}
	return r.bundle
}

// InstanceObserver returns the passive per-instance health observer.
//
// It returns nil only when DisableInstanceOutlier was explicitly configured.
func (r *Runtime) InstanceObserver() selector.Observer {
	if r == nil || r.instanceTier == nil {
		return nil
	}
	return r.instanceTier
}

// SelectorOptions returns the options needed to attach instance health to a
// Keelith Selector. The result is empty when the instance tier is disabled.
func (r *Runtime) SelectorOptions() []selector.Option {
	observer := r.InstanceObserver()
	if observer == nil {
		return nil
	}
	return []selector.Option{selector.WithObserver(observer)}
}

// Describe returns bounded state for middleware order, service breakers,
// retry amplification, and instance health.
func (r *Runtime) Describe() Description {
	if r == nil {
		return Description{}
	}
	description := Description{
		Middleware: r.bundle.Describe(),
		Breakers:   r.breakers.Describe(),
		Bulkheads:  r.bulkheads.Describe(),
		Retry:      r.retryBudget.Describe(),
	}
	if r.admission != nil {
		description.Admission = r.admission.Describe()
	}
	if r.instanceTier != nil {
		description.Instances = r.instanceTier.Status()
	}
	return description
}

func newInstanceTier(config Config) (*outlier.Detector, error) {
	if config.DisableInstanceOutlier {
		return nil, nil
	}
	instanceConfig := config.InstanceOutlier
	if instanceConfig.ConsecutiveFailures == 0 {
		instanceConfig.ConsecutiveFailures = defaultOutlierFailures
	}
	if instanceConfig.EjectionTime == 0 {
		instanceConfig.EjectionTime = defaultOutlierEjection
	}
	detector, err := outlier.New(instanceConfig)
	if err != nil {
		return nil, fmt.Errorf("%w: instance outlier: %w", ErrInvalidConfig, err)
	}
	return detector, nil
}

func snapshotPolicy(resolver policy.Resolver) middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		if next == nil {
			return invalidHandler("policy snapshot next handler is nil")
		}
		return func(ctx context.Context, request any) (any, error) {
			if ctx == nil {
				return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
			}
			target, ok := operation.FromContext(ctx)
			if !ok {
				return nil, ErrMissingOperation
			}
			resolved := resolver.Resolve(target)
			if err := policy.Validate(resolved); err != nil {
				return nil, err
			}
			return next(policy.WithResolved(ctx, target, resolved), request)
		}
	}
}

func dispatchAttempts(resolver policy.Resolver, retryMiddleware middleware.Middleware, hedgingMiddleware middleware.Middleware) middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		if next == nil {
			return invalidHandler("attempt dispatcher next handler is nil")
		}
		retryHandler := retryMiddleware(next)
		hedgingHandler := hedgingMiddleware(next)
		return func(ctx context.Context, request any) (any, error) {
			if ctx == nil {
				return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
			}
			target, ok := operation.FromContext(ctx)
			if !ok {
				return nil, ErrMissingOperation
			}
			resolved := policy.Resolve(ctx, resolver, target)
			switch {
			case resolved.Retry.Enabled && resolved.Hedging.Enabled:
				return nil, fmt.Errorf("%w: retry and hedging are both enabled", policy.ErrInvalidPolicy)
			case resolved.Retry.Enabled:
				return retryHandler(ctx, request)
			case resolved.Hedging.Enabled:
				return hedgingHandler(ctx, request)
			default:
				return next(attempt.WithContext(ctx, 1), request)
			}
		}
	}
}

func invalidHandler(message string) middleware.Handler {
	return func(context.Context, any) (any, error) {
		return nil, fmt.Errorf("%w: %s", ErrInvalidConfig, message)
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
