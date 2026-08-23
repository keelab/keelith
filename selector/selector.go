// Package selector provides feedback-aware service node selection.
package selector

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/registry"
)

const (
	maximumPreferenceTiers   = 8
	maximumPreferencesInTier = 8

	// MetadataRegion is the provider-neutral service region metadata key.
	MetadataRegion = "cloud.region"
	// MetadataZone is the provider-neutral service availability-zone key.
	MetadataZone = "cloud.availability_zone"
)

var (
	// ErrInvalidOption means a selector option is malformed.
	ErrInvalidOption = errors.New("selector: invalid option")
	// ErrInvalidWeight means a node advertises an unusable selection weight.
	ErrInvalidWeight = errors.New("selector: invalid weight")
	// ErrHashKey means a rendezvous selector cannot obtain a routing key.
	ErrHashKey = errors.New("selector: invalid hash key")
	// ErrNoNodes means no endpoint satisfies required constraints.
	ErrNoNodes = errors.New("selector: no eligible nodes")
	// ErrServiceMismatch means an Operation targets another service.
	ErrServiceMismatch = errors.New("selector: service mismatch")
)

// Result reports one completed node invocation.
type Result struct {
	Latency  time.Duration
	Error    error
	Canceled bool
	Retried  bool
}

// Done records a Result. Implementations must make Done idempotent.
type Done func(Result)

// Selector consumes full discovery Snapshots and selects Nodes.
type Selector interface {
	Update(registry.Snapshot) error
	Select(context.Context, operation.Operation) (Node, Done, error)
}

// PreferenceDescriber is implemented by built-in selectors that expose only
// the number of configured preference tiers. Metadata values remain private.
type PreferenceDescriber interface {
	PreferenceTierCount() int
}

// Filter is a mandatory node constraint.
type Filter func(context.Context, operation.Operation, Node) bool

// Observer can exclude unhealthy nodes and observe completed invocations.
//
// Implementations must be safe for concurrent use. Allow should be cheap and
// must not retain ctx.
type Observer interface {
	Allow(context.Context, operation.Operation, Node) bool
	Done(operation.Operation, Node, Result)
}

// Preference is one exact metadata match inside a preference tier.
type Preference struct {
	Key   string
	Value string
}

// PreferenceTier is an AND set of preferred metadata values. Tiers are tried
// in caller order after mandatory filters and health observers have run.
type PreferenceTier []Preference

// Locality is the local service topology used by WithLocality.
type Locality struct {
	Region string
	Zone   string
}

// Option configures selectors.
type Option interface {
	apply(*settings) error
}

type optionFunc func(*settings) error

func (f optionFunc) apply(settings *settings) error {
	return f(settings)
}

type settings struct {
	filters         []Filter
	preferenceTiers []map[string]string
	observers       []Observer
	seed            uint64
}

// WithFilter adds a mandatory node Filter.
func WithFilter(filter Filter) Option {
	return optionFunc(func(settings *settings) error {
		if filter == nil {
			return fmt.Errorf("filter is nil")
		}
		settings.filters = append(settings.filters, filter)
		return nil
	})
}

// WithObserver adds a selection health/result observer.
func WithObserver(observer Observer) Option {
	return optionFunc(func(settings *settings) error {
		if observer == nil {
			return fmt.Errorf("observer is nil")
		}
		settings.observers = append(settings.observers, observer)
		return nil
	})
}

// MatchMetadata creates a mandatory metadata Filter.
func MatchMetadata(key, value string) Filter {
	return func(_ context.Context, _ operation.Operation, node Node) bool {
		return node.metadata[key] == value
	}
}

// WithPreferenceTiers adds ordered best-effort metadata preferences.
//
// Every matcher inside a tier must match. The first non-empty tier wins; if
// every tier is empty, selection falls back to all otherwise eligible nodes.
func WithPreferenceTiers(tiers ...PreferenceTier) Option {
	copied := make([]PreferenceTier, len(tiers))
	for index, tier := range tiers {
		copied[index] = append(PreferenceTier(nil), tier...)
	}
	return optionFunc(func(settings *settings) error {
		if len(copied) == 0 ||
			len(settings.preferenceTiers)+len(copied) > maximumPreferenceTiers {
			return fmt.Errorf("preference tier count must be within 1..%d", maximumPreferenceTiers)
		}
		for tierIndex, tier := range copied {
			if len(tier) == 0 || len(tier) > maximumPreferencesInTier {
				return fmt.Errorf("preference tier %d size must be within 1..%d", tierIndex, maximumPreferencesInTier)
			}
			matches := make(map[string]string, len(tier))
			for matchIndex, match := range tier {
				if !validPreferenceKey(match.Key) || !validPreferenceValue(match.Value) {
					return fmt.Errorf("preference tier %d matcher %d is malformed", tierIndex, matchIndex)
				}
				if _, duplicate := matches[match.Key]; duplicate {
					return fmt.Errorf("preference tier %d key %q is duplicated", tierIndex, match.Key)
				}
				matches[match.Key] = match.Value
			}
			settings.preferenceTiers = append(settings.preferenceTiers, matches)
		}
		return nil
	})
}

// WithLocality prefers the local availability zone, then the local region,
// then any otherwise eligible node. Empty topology levels are omitted.
func WithLocality(locality Locality) Option {
	tiers := make([]PreferenceTier, 0, 2)
	if locality.Zone != "" {
		tiers = append(tiers, PreferenceTier{{
			Key:   MetadataZone,
			Value: locality.Zone,
		}})
	}
	if locality.Region != "" {
		tiers = append(tiers, PreferenceTier{{
			Key:   MetadataRegion,
			Value: locality.Region,
		}})
	}
	return WithPreferenceTiers(tiers...)
}

// WithSeed sets deterministic P2C sampling state.
func WithSeed(seed uint64) Option {
	return optionFunc(func(settings *settings) error {
		settings.seed = seed
		return nil
	})
}

func newSettings(options []Option) (settings, error) {
	result := settings{seed: 0x9e3779b97f4a7c15}
	for index, option := range options {
		if option == nil {
			return settings{}, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&result); err != nil {
			return settings{}, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	if result.seed == 0 {
		result.seed = 0x9e3779b97f4a7c15
	}
	return result, nil
}

func validateScheme(scheme string) (string, error) {
	normalized := strings.ToLower(scheme)
	parsed, err := url.Parse(normalized + "://endpoint")
	if err != nil || parsed.Scheme != normalized || normalized == "" {
		return "", fmt.Errorf("%w: endpoint scheme %q", ErrInvalidOption, scheme)
	}
	return normalized, nil
}

func nodesFromSnapshot(snapshot registry.Snapshot, scheme string) ([]Node, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	nodes := make([]Node, 0)
	for _, instance := range snapshot.Instances() {
		for _, endpoint := range instance.Endpoints() {
			parsed, err := url.Parse(endpoint)
			if err == nil && parsed.Scheme == scheme {
				nodes = append(nodes, newNode(instance, endpoint))
			}
		}
	}
	sort.Slice(nodes, func(first, second int) bool {
		return nodes[first].key() < nodes[second].key()
	})
	return nodes, nil
}

func eligibleNodes(ctx context.Context, operationID operation.Operation, nodes []Node, settings settings) ([]Node, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	filtered := make([]Node, 0, len(nodes))
	for _, node := range nodes {
		eligible := true
		for _, filter := range settings.filters {
			if !filter(ctx, operationID, node) {
				eligible = false
				break
			}
		}
		if eligible {
			for _, observer := range settings.observers {
				if !observer.Allow(ctx, operationID, node) {
					eligible = false
					break
				}
			}
		}
		if eligible {
			filtered = append(filtered, node)
		}
	}
	if len(filtered) == 0 {
		return nil, ErrNoNodes
	}
	return preferredNodes(filtered, settings.preferenceTiers), nil
}

func preferredNodes(nodes []Node, tiers []map[string]string) []Node {
	for _, tier := range tiers {
		preferred := make([]Node, 0, len(nodes))
		for _, node := range nodes {
			matches := true
			for key, value := range tier {
				if node.metadata[key] != value {
					matches = false
					break
				}
			}
			if matches {
				preferred = append(preferred, node)
			}
		}
		if len(preferred) != 0 {
			return preferred
		}
	}
	return nodes
}

func (s settings) preferenceTierCount() int {
	return len(s.preferenceTiers)
}

func validPreferenceKey(value string) bool {
	if !validPreferenceValue(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func validPreferenceValue(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func observeResult(settings settings, operationID operation.Operation, node Node, result Result) {
	for _, observer := range settings.observers {
		observer.Done(operationID, node, result)
	}
}

func idempotentDone(record func(Result)) Done {
	var once sync.Once
	return func(result Result) {
		once.Do(func() {
			record(result)
		})
	}
}
