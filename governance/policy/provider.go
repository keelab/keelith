package policy

import (
	"context"

	"github.com/keelab/keelith/operation"
)

// Resolver returns the current fully resolved policy for an Operation.
type Resolver interface {
	Resolve(operation.Operation) Policy
}

// Provider loads and watches complete policy definitions.
type Provider interface {
	Load(context.Context) (Definition, error)
	Watch(context.Context) (Watcher, error)
}

// Watcher yields complete policy definitions.
type Watcher interface {
	Next(context.Context) (Definition, error)
	Close() error
}
