// Package inbound assembles coherent server-side request governance.
package inbound

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/keelab/keelith/governance/loadshed"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/governance/ratelimit"
	ktimeout "github.com/keelab/keelith/governance/timeout"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

const defaultSource = "inbound-runtime"

var (
	// ErrInvalidConfig reports an incomplete or unsafe inbound runtime.
	ErrInvalidConfig = errors.New("inbound: invalid config")
	// ErrMissingOperation reports a server invocation without a stable
	// transport-neutral Operation.
	ErrMissingOperation = errors.New("inbound: operation is missing")
)

// Config defines one application-instance server governance runtime.
type Config struct {
	Resolver     policy.Resolver
	RateLimits   *ratelimit.Pool
	LoadShedders *loadshed.Pool
	CPU          loadshed.CPU
	Source       string
}

// Stages are the fixed server-governance insertion points.
//
// Policy must execute before the other three stages. ServerBundle places
// authentication and authorization between Policy and RateLimit.
type Stages struct {
	Policy       middleware.Middleware
	RateLimit    middleware.Middleware
	LoadShedding middleware.Middleware
	Timeout      middleware.Middleware
}

// Description is a bounded runtime snapshot. Pool entries include Operation
// keys for application-owned diagnostics; use the Ops adapter for an
// HTTP-safe aggregate projection.
type Description struct {
	Middleware   []middleware.Description
	RateLimits   []ratelimit.Description
	LoadShedders []loadshed.Description
}

// Runtime coordinates one policy snapshot with local admission, overload
// protection, and request deadlines.
//
// Its standalone middleware order is:
//
//	policy snapshot -> rate limit -> load shedding -> timeout
type Runtime struct {
	stages       Stages
	bundle       *middleware.Bundle
	rateLimits   *ratelimit.Pool
	loadShedders *loadshed.Pool
}

// New assembles an inbound Runtime without starting goroutines.
func New(config Config) (*Runtime, error) {
	if isNil(config.Resolver) {
		return nil, fmt.Errorf("%w: resolver is nil", ErrInvalidConfig)
	}
	rateLimits := config.RateLimits
	if rateLimits == nil {
		rateLimits = ratelimit.NewPool(nil)
	}
	loadShedders := config.LoadShedders
	if loadShedders == nil {
		loadShedders = loadshed.NewPool(nil, config.CPU)
	}
	snapshot := snapshotPolicy(config.Resolver)
	rateLimit, err := ratelimit.New(config.Resolver, rateLimits)
	if err != nil {
		return nil, fmt.Errorf("%w: rate limit: %w", ErrInvalidConfig, err)
	}
	loadShedding, err := loadshed.New(config.Resolver, loadShedders)
	if err != nil {
		return nil, fmt.Errorf("%w: load shedding: %w", ErrInvalidConfig, err)
	}
	timeout, err := ktimeout.New(config.Resolver)
	if err != nil {
		return nil, fmt.Errorf("%w: timeout: %w", ErrInvalidConfig, err)
	}
	source := strings.TrimSpace(config.Source)
	if source == "" {
		source = defaultSource
	}
	bundle, err := middleware.NewBundle(middleware.Entry{Name: "policy-snapshot", Source: source, Middleware: snapshot},
		middleware.Entry{
			Name:       "rate-limit",
			Source:     source,
			Middleware: rateLimit,
		},
		middleware.Entry{
			Name:       "load-shedding",
			Source:     source,
			Middleware: loadShedding,
		},
		middleware.Entry{
			Name:       "timeout",
			Source:     source,
			Middleware: timeout,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("%w: middleware bundle: %w", ErrInvalidConfig, err)
	}
	return &Runtime{
		stages: Stages{
			Policy:       snapshot,
			RateLimit:    rateLimit,
			LoadShedding: loadShedding,
			Timeout:      timeout,
		},
		bundle:       bundle,
		rateLimits:   rateLimits,
		loadShedders: loadShedders,
	}, nil
}

// Stages returns immutable middleware insertion points.
func (r *Runtime) Stages() Stages {
	if r == nil {
		return Stages{}
	}
	return r.stages
}

// Middleware returns the standalone fixed-order governance bundle.
func (r *Runtime) Middleware() *middleware.Bundle {
	if r == nil {
		return nil
	}
	return r.bundle
}

// Describe returns current admission and overload state.
func (r *Runtime) Describe() Description {
	if r == nil {
		return Description{}
	}
	return Description{
		Middleware:   r.bundle.Describe(),
		RateLimits:   r.rateLimits.Describe(),
		LoadShedders: r.loadShedders.Describe(),
	}
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
			return next(
				policy.WithResolved(ctx, target, resolved),
				request,
			)
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
