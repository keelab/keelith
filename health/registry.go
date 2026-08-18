package health

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"
)

const applicationCheckName = "app"

// Registry combines application phase with named health contributors.
type Registry struct {
	mu              sync.RWMutex                // mu is the read-write mutex for the registry.
	phase           Phase                       // phase is the current application phase.
	startupComplete bool                        // startupComplete is whether the application startup is complete.
	contributors    map[Kind]map[string]Checker // contributors is the map of health contributors by kind and name.
	checkTimeout    time.Duration               // checkTimeout is the timeout for health checks.
	cacheTTL        time.Duration               // cacheTTL is the time-to-live for cached reports.
	generation      uint64                      // generation is the cache generation number.
	cache           map[Kind]cachedReport       // cache is the map of cached reports by kind.
}

// NewRegistry creates an isolated Registry in PhaseNew.
func NewRegistry() *Registry {
	registry, _ := NewRegistryWithOptions()
	return registry
}

// NewRegistryWithOptions creates a validated bounded Registry.
func NewRegistryWithOptions(optionList ...RegistryOption) (*Registry, error) {
	options := registryOptions{checkTimeout: defaultCheckTimeout}

	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: registry option %d is nil", ErrInvalid, index)
		}
		if err := option.applyRegistry(&options); err != nil {
			return nil, fmt.Errorf("%w: registry option %d: %w", ErrInvalid, index, err)
		}
	}

	return &Registry{
		contributors: make(map[Kind]map[string]Checker),
		checkTimeout: options.checkTimeout,
		cacheTTL:     options.cacheTTL,
		cache:        make(map[Kind]cachedReport),
	}, nil
}

// Register adds a named Checker to one health Kind.
func (r *Registry) Register(kind Kind, name string, checker Checker) error {
	if r == nil || !validKind(kind) {
		return fmt.Errorf("%w: kind %q", ErrInvalid, kind)
	}
	normalizedName := strings.TrimSpace(name)
	if normalizedName == "" {
		return fmt.Errorf("%w: contributor name is empty", ErrInvalid)
	}
	if checker == nil {
		return fmt.Errorf("%w: checker %q is nil", ErrInvalid, normalizedName)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.contributors == nil {
		r.contributors = make(map[Kind]map[string]Checker)
	}
	checkers := r.contributors[kind]
	if checkers == nil {
		checkers = make(map[string]Checker)
		r.contributors[kind] = checkers
	}
	if _, duplicate := checkers[normalizedName]; duplicate {
		return fmt.Errorf("%w: %s/%s", ErrDuplicate, kind, normalizedName)
	}
	if len(checkers) >= maxContributors {
		return fmt.Errorf("%w: kind %q", ErrLimit, kind)
	}
	checkers[normalizedName] = checker
	r.invalidateLocked()

	return nil
}

// Unregister removes a named Checker if it exists.
func (r *Registry) Unregister(kind Kind, name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.contributors[kind], strings.TrimSpace(name))
	r.invalidateLocked()
}

// Check evaluates a Kind without holding the Registry lock while running user
// Checkers.
func (r *Registry) Check(ctx context.Context, kind Kind) Report {
	now := time.Now().UTC()
	if r == nil {
		return Report{Kind: kind, Status: StatusFail, Reason: "registry is nil", CheckedAt: now}
	}
	if !validKind(kind) {
		return Report{Kind: kind, Status: StatusFail, Reason: "invalid health kind", CheckedAt: now}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	output := r.checkSnapshot(kind, now)
	if output.cacheHit {
		return cloneReport(output.cached)
	}
	results := make([]CheckResult, 0, len(output.checkers)+1)
	if kind != KindDependency {
		results = append(
			results,
			namedResult(applicationCheckName, phaseResult(kind, output.phase, output.startupComplete), now),
		)
	}

	names := make([]string, 0, len(output.checkers))
	for name := range output.checkers {
		names = append(names, name)
	}
	sort.Strings(names)
	results = append(
		results,
		runCheckers(ctx, output.checkTimeout, names, output.checkers, now)...,
	)
	sort.Slice(results, func(first, second int) bool {
		return results[first].Name < results[second].Name
	})

	status, reason := aggregate(results)
	report := Report{
		Kind:      kind,
		Status:    status,
		Reason:    reason,
		CheckedAt: now,
		Checks:    results,
	}
	if output.cacheTTL > 0 && context.Cause(ctx) == nil {
		r.storeCached(kind, output.generation, output.cacheTTL, report)
	}
	return report
}

// Starting advances the Registry to PhaseStarting.
func (r *Registry) Starting() {
	r.advance(PhaseStarting)
}

// Ready advances the Registry to PhaseReady and permanently marks startup
// complete.
func (r *Registry) Ready() {
	r.advance(PhaseReady)
}

// Draining advances the Registry to PhaseDraining, immediately disabling
// readiness.
func (r *Registry) Draining() {
	r.advance(PhaseDraining)
}

// Stopped advances the Registry to PhaseStopped.
func (r *Registry) Stopped() {
	r.advance(PhaseStopped)
}

// Failed advances the Registry to PhaseFailed.
func (r *Registry) Failed() {
	r.advance(PhaseFailed)
}

// Phase returns the current application phase.
func (r *Registry) Phase() Phase {
	if r == nil {
		return PhaseStopped
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.phase
}

func (r *Registry) advance(next Phase) {

	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if next <= r.phase {
		return
	}
	r.phase = next
	if next == PhaseReady {
		r.startupComplete = true
	}
	r.invalidateLocked()
}

func (r *Registry) storeCached(kind Kind, generation uint64, ttl time.Duration, report Report) {

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.generation != generation || r.cacheTTL != ttl {
		return
	}
	if r.cache == nil {
		r.cache = make(map[Kind]cachedReport)
	}
	r.cache[kind] = cachedReport{
		report:     cloneReport(report),
		expiresAt:  time.Now().UTC().Add(ttl),
		generation: generation,
	}
}

func (r *Registry) checkSnapshot(kind Kind, now time.Time) checkerOutput {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cached, ok := r.cache[kind]; ok &&
		cached.generation == r.generation &&
		now.Before(cached.expiresAt) {

		return checkerOutput{
			phase:           r.phase,
			startupComplete: r.startupComplete,
			checkers:        nil,
			checkTimeout:    0,
			cacheTTL:        0,
			generation:      0,
			cached:          cached.report,
			cacheHit:        true,
		}
	}
	checkers := r.contributors[kind]
	cloned := make(map[string]Checker, len(checkers))
	maps.Copy(cloned, checkers)
	timeout := r.checkTimeout
	if timeout <= 0 {
		timeout = defaultCheckTimeout
	}

	return checkerOutput{
		phase:           r.phase,
		startupComplete: r.startupComplete,
		checkers:        cloned,
		checkTimeout:    timeout,
		cacheTTL:        r.cacheTTL,
		generation:      r.generation,
		cached:          Report{},
		cacheHit:        false,
	}
}

func (r *Registry) invalidateLocked() {
	r.generation++
	clear(r.cache)
}

func phaseResult(kind Kind, phase Phase, startupComplete bool) Result {
	switch kind {
	case KindStartup:
		if startupComplete {
			return Pass("startup completed")
		}
		return Fail("startup has not completed")
	case KindLiveness:
		switch phase {
		case PhaseStarting, PhaseReady, PhaseDraining:
			return Pass("application is alive")
		default:
			return Fail("application is " + phase.String())
		}
	case KindReadiness:
		if phase == PhaseReady {
			return Pass("application is ready")
		}
		return Fail("application is " + phase.String())
	default:
		return Unknown("no application phase rule")
	}
}

func runChecker(ctx context.Context, checker Checker, now time.Time) (result Result) {
	if cause := context.Cause(ctx); cause != nil {
		result = Fail(cause.Error())
		result.CheckedAt = now
		return result
	}

	defer func() {
		if recover() != nil {
			result = Fail("checker panicked")
			result.CheckedAt = now
		}
	}()
	result = checker(ctx)
	if cause := context.Cause(ctx); cause != nil {
		result = Fail(cause.Error())
	}
	if !validStatus(result.Status) {
		result = Fail("checker returned invalid status")
	}
	if result.Reason == "" {
		result.Reason = "checker returned no reason"
	}
	if result.CheckedAt.IsZero() {
		result.CheckedAt = now
	}
	return result
}

func namedResult(name string, result Result, now time.Time) CheckResult {
	if result.CheckedAt.IsZero() {
		result.CheckedAt = now
	}
	return CheckResult{
		Name:      name,
		Status:    result.Status,
		Reason:    result.Reason,
		CheckedAt: result.CheckedAt,
	}
}

func aggregate(results []CheckResult) (Status, string) {
	if len(results) == 0 {
		return StatusPass, "no checks registered"
	}

	status := StatusPass
	reasons := make([]string, 0)
	for _, result := range results {
		switch result.Status {
		case StatusFail:
			status = StatusFail
			reasons = append(reasons, result.Name+": "+result.Reason)
		case StatusUnknown:
			if status != StatusFail {
				status = StatusUnknown
			}
			reasons = append(reasons, result.Name+": "+result.Reason)
		}
	}
	if len(reasons) == 0 {
		return StatusPass, "all checks passed"
	}
	return status, strings.Join(reasons, "; ")
}

func validKind(kind Kind) bool {
	switch kind {
	case KindStartup, KindLiveness, KindReadiness, KindDependency:
		return true
	default:
		return false
	}
}

func validStatus(status Status) bool {
	switch status {
	case StatusPass, StatusFail, StatusUnknown:
		return true
	default:
		return false
	}
}

// String returns a stable diagnostic phase.
func (phase Phase) String() string {
	switch phase {
	case PhaseNew:
		return "new"
	case PhaseStarting:
		return "starting"
	case PhaseReady:
		return "ready"
	case PhaseDraining:
		return "draining"
	case PhaseStopped:
		return "stopped"
	case PhaseFailed:
		return "failed"
	default:
		return fmt.Sprintf("Phase(%d)", phase)
	}
}
