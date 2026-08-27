// Package consul adapts consul Agent and Health http APIs to Keelith registry
// contracts.
package consul

import (
	"context"
	"time"
)

// Registration is one consul service registration.
type Registration struct {
	ID      string
	Service string
	Address string
	Port    int
	Meta    map[string]string
	TTL     time.Duration
}

// BackendInstance is the bounded consul health representation used by Client.
type BackendInstance struct {
	ID      string
	Service string
	Address string
	Port    int
	Meta    map[string]string
}

// Backend isolates registry semantics from consul's http representation.
type Backend interface {
	Register(context.Context, Registration) error
	Deregister(context.Context, string) error
	Pass(context.Context, string) error
	List(
		context.Context,
		string,
		string,
		string,
		time.Duration,
	) ([]BackendInstance, string, error)
	Close() error
}
