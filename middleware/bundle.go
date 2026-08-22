package middleware

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/keelab/keelith/operation"
)

const explicitSource = "explicit"

var (
	// ErrInvalidEntry means a Bundle entry is incomplete or ambiguous.
	ErrInvalidEntry = errors.New("middleware: invalid bundle entry")
)

// Entry names a Middleware and records the configuration source that enabled
// it.
type Entry struct {
	Name       string
	Source     string
	Middleware Middleware
}

// ScopeToService returns a namespaced Bundle whose entries execute only when
// the current Operation belongs to service. The original Bundle is unchanged
// and can be reused by multiple generated service bindings.
func ScopeToService(service string, bundle *Bundle) (*Bundle, error) {
	return ScopeToServiceWithNamespace(service, service, bundle)
}

// ScopeToServiceWithNamespace behaves like ScopeToService and uses namespace
// as the diagnostic Entry prefix. It allows higher-level immutable groups to
// distinguish inherited policy from Binding-specific policy.
func ScopeToServiceWithNamespace(namespace string, service string, bundle *Bundle) (*Bundle, error) {
	if strings.TrimSpace(namespace) != namespace || namespace == "" {
		return nil, fmt.Errorf("%w: namespace is empty or not normalized", ErrInvalidEntry)
	}
	if strings.TrimSpace(service) != service || service == "" {
		return nil, fmt.Errorf("%w: service scope is empty or not normalized", ErrInvalidEntry)
	}
	if bundle == nil {
		return nil, nil
	}
	entries := make([]Entry, len(bundle.entries))

	for index, entry := range bundle.entries {
		entries[index] = Entry{
			Name:       namespace + "/" + entry.Name,
			Source:     entry.Source,
			Middleware: scopeMiddlewareToService(service, entry.Middleware),
		}
	}
	return NewBundle(entries...)
}

func scopeMiddlewareToService(service string, scoped Middleware) Middleware {
	return func(next Handler) Handler {
		handler := scoped(next)
		return func(ctx context.Context, request any) (any, error) {
			target, ok := operation.FromContext(ctx)
			if !ok || target.Service() != service {
				return next(ctx, request)
			}
			return handler(ctx, request)
		}
	}
}

// Description is an immutable diagnostic projection of one Bundle entry.
type Description struct {
	Position int
	Name     string
	Source   string
}

// Bundle is an immutable, inspectable middleware chain.
type Bundle struct {
	entries []Entry
}

// NewBundle validates and copies entries.
func NewBundle(entries ...Entry) (*Bundle, error) {
	snapshot := make([]Entry, 0, len(entries))
	names := make(map[string]struct{}, len(entries))

	for index, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			return nil, fmt.Errorf("%w: entry %d has an empty name", ErrInvalidEntry, index)
		}
		if entry.Middleware == nil {
			return nil, fmt.Errorf("%w: entry %q has a nil middleware", ErrInvalidEntry, name)
		}
		if _, duplicate := names[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate name %q", ErrInvalidEntry, name)
		}
		names[name] = struct{}{}

		source := strings.TrimSpace(entry.Source)
		if source == "" {
			source = explicitSource
		}
		snapshot = append(snapshot, Entry{
			Name:       name,
			Source:     source,
			Middleware: entry.Middleware,
		})
	}
	return &Bundle{entries: snapshot}, nil
}

// CombineBundles creates one auditable Bundle by concatenating entries in
// argument order. Nil bundles are ignored and duplicate names are rejected.
func CombineBundles(bundles ...*Bundle) (*Bundle, error) {
	entries := make([]Entry, 0)

	for _, bundle := range bundles {
		if bundle == nil {
			continue
		}
		entries = append(entries, bundle.entries...)
	}
	return NewBundle(entries...)
}

// Chain returns this Bundle as a composable Middleware.
func (b *Bundle) Chain() Middleware {
	if b == nil {
		return Chain()
	}
	middlewares := make([]Middleware, len(b.entries))

	for index, entry := range b.entries {
		middlewares[index] = entry.Middleware
	}
	return Chain(middlewares...)
}

// Describe returns entries in final execution order.
func (b *Bundle) Describe() []Description {
	if b == nil {
		return nil
	}
	description := make([]Description, len(b.entries))
	for index, entry := range b.entries {
		description[index] = Description{
			Position: index,
			Name:     entry.Name,
			Source:   entry.Source,
		}
	}
	return description
}
