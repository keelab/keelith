// Package admission applies dynamic, provider-neutral outbound drop policies.
package admission

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

const (
	maximumCategories  = 16
	maximumIdentityLen = 1_024
	maximumDenominator = 1_000_000
)

var (
	// ErrDropped reports that a dynamic admission policy rejected a logical
	// outbound call before any transport attempt was started.
	ErrDropped = kerrors.New(
		503,
		"ADMISSION_DROPPED",
		"request was dropped by an admission policy",
	)
	// ErrInvalidPolicy reports an unsafe drop category or percentage.
	ErrInvalidPolicy = errors.New("admission: invalid policy")
	// ErrInvalidUpdate reports an invalid service or revision identity.
	ErrInvalidUpdate = errors.New("admission: invalid update")
	// ErrInvalidOption reports a nil resolver, random source, or handler.
	ErrInvalidOption = errors.New("admission: invalid option")
	// ErrMissingOperation reports an invocation without a stable Operation.
	ErrMissingOperation = errors.New("admission: operation is missing")
)

// Category is one ordered drop decision. Categories are evaluated in order;
// each percentage applies to traffic remaining after earlier categories.
type Category struct {
	Name        string
	Numerator   uint32
	Denominator uint32
}

// Policy is an immutable-by-convention ordered admission policy.
type Policy struct {
	Categories []Category
}

// Clone returns an independent Policy value.
func (policy Policy) Clone() Policy {
	return Policy{Categories: append([]Category(nil), policy.Categories...)}
}

// Validate verifies bounded category identities and fractional percentages.
func Validate(policy Policy) error {
	if len(policy.Categories) > maximumCategories {
		return fmt.Errorf("%w: category count exceeds %d", ErrInvalidPolicy, maximumCategories)
	}
	seen := make(map[string]struct{}, len(policy.Categories))
	for _, category := range policy.Categories {
		if !validIdentity(category.Name) {
			return fmt.Errorf("%w: category name is invalid", ErrInvalidPolicy)
		}
		if category.Denominator == 0 ||
			category.Denominator > maximumDenominator ||
			category.Numerator > category.Denominator {
			return fmt.Errorf("%w: category percentage is invalid", ErrInvalidPolicy)
		}
		if _, duplicate := seen[category.Name]; duplicate {
			return fmt.Errorf("%w: category name is duplicated", ErrInvalidPolicy)
		}
		seen[category.Name] = struct{}{}
	}
	return nil
}

// Resolver returns the current complete Policy for one logical service.
type Resolver interface {
	Resolve(string) Policy
}

// Sink atomically accepts a complete Policy for one logical service.
type Sink interface {
	Update(string, string, Policy) (bool, error)
}

type storeEntry struct {
	revision string
	policy   Policy
}

// Store publishes immutable per-service admission policies.
type Store struct {
	mu       sync.RWMutex
	services map[string]storeEntry
	updates  uint64
}

// StoreDescription is a value-free aggregate of current policy state.
type StoreDescription struct {
	Services   int
	Categories int
	Updates    uint64
}

// NewStore creates an empty Store. An absent service policy allows traffic.
func NewStore() *Store {
	return &Store{services: make(map[string]storeEntry)}
}

// Update validates and atomically replaces one service policy. A repeated
// revision is ignored so duplicate xDS responses do not perturb state.
func (s *Store) Update(service string, revision string, policy Policy) (bool, error) {
	if s == nil {
		return false, fmt.Errorf("%w: store is nil", ErrInvalidUpdate)
	}
	if !validIdentity(service) || !validIdentity(revision) {
		return false, fmt.Errorf("%w: service or revision is invalid", ErrInvalidUpdate)
	}
	if err := Validate(policy); err != nil {
		return false, err
	}
	next := policy.Clone()
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, exists := s.services[service]; exists && current.revision == revision {
		return false, nil
	}
	if s.services == nil {
		s.services = make(map[string]storeEntry)
	}
	s.services[service] = storeEntry{revision: revision, policy: next}
	s.updates++
	return true, nil
}

// Resolve returns an independent current Policy. Unknown services resolve to
// an empty allow-all policy.
func (s *Store) Resolve(service string) Policy {
	if s == nil {
		return Policy{}
	}
	s.mu.RLock()
	entry, exists := s.services[service]
	s.mu.RUnlock()
	if !exists {
		return Policy{}
	}
	return entry.policy.Clone()
}

// Describe returns aggregate cardinality without service, revision, or
// category values.
func (s *Store) Describe() StoreDescription {
	if s == nil {
		return StoreDescription{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	description := StoreDescription{
		Services: len(s.services),
		Updates:  s.updates,
	}
	for _, entry := range s.services {
		description.Categories += len(entry.policy.Categories)
	}
	return description
}

// Random supplies unbiased values in [0, limit).
type Random interface {
	Uint64N(uint64) uint64
}

// Option configures a Controller.
type Option interface {
	apply(*controllerOptions) error
}

type optionFunc func(*controllerOptions) error

func (f optionFunc) apply(options *controllerOptions) error {
	return f(options)
}

type controllerOptions struct {
	random Random
}

// WithRandom replaces the production sampler for deterministic tests.
func WithRandom(random Random) Option {
	return optionFunc(func(options *controllerOptions) error {
		if isNil(random) {
			return fmt.Errorf("random source is nil")
		}
		options.random = random
		return nil
	})
}

// Description is a bounded Controller snapshot.
type Description struct {
	Enabled    bool
	Services   int
	Categories int
	Updates    uint64
	Accepted   uint64
	Dropped    uint64
}

// Controller resolves and samples one admission policy per logical call.
type Controller struct {
	resolver Resolver
	random   Random
	accepted atomic.Uint64
	dropped  atomic.Uint64
}

// New creates a Controller without starting goroutines.
func New(resolver Resolver, optionList ...Option) (*Controller, error) {
	if isNil(resolver) {
		return nil, fmt.Errorf("%w: resolver is nil", ErrInvalidOption)
	}
	settings := controllerOptions{random: packageRandom{}}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	return &Controller{resolver: resolver, random: settings.random}, nil
}

// Middleware rejects a dropped logical call before invoking next.
func (c *Controller) Middleware() middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		if next == nil {
			return invalidHandler("next handler is nil")
		}
		return func(ctx context.Context, request any) (any, error) {
			if c == nil || ctx == nil {
				return nil, fmt.Errorf("%w: controller or context is nil", ErrInvalidOption)
			}
			target, ok := operation.FromContext(ctx)
			if !ok {
				return nil, ErrMissingOperation
			}
			policy := c.resolver.Resolve(target.Service())
			if err := Validate(policy); err != nil {
				return nil, err
			}
			for _, category := range policy.Categories {
				if category.Numerator == category.Denominator || c.random.Uint64N(uint64(category.Denominator)) < uint64(category.Numerator) {
					c.dropped.Add(1)
					return nil, ErrDropped
				}
			}
			c.accepted.Add(1)
			return next(ctx, request)
		}
	}
}

// Describe returns aggregate policy and decision state.
func (c *Controller) Describe() Description {
	if c == nil {
		return Description{}
	}
	description := Description{
		Enabled:  true,
		Accepted: c.accepted.Load(),
		Dropped:  c.dropped.Load(),
	}
	if describer, ok := c.resolver.(interface {
		Describe() StoreDescription
	}); ok {
		store := describer.Describe()
		description.Services = store.Services
		description.Categories = store.Categories
		description.Updates = store.Updates
	}
	return description
}

type packageRandom struct{}

func (packageRandom) Uint64N(limit uint64) uint64 {
	return rand.Uint64N(limit)
}

func invalidHandler(message string) middleware.Handler {
	return func(context.Context, any) (any, error) {
		return nil, fmt.Errorf("%w: %s", ErrInvalidOption, message)
	}
}

func validIdentity(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > maximumIdentityLen || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
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
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var (
	_ Resolver = (*Store)(nil)
	_ Sink     = (*Store)(nil)
)
