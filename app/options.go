package app

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/server"
	"github.com/keelab/keelith/service"
)

const defaultStopTimeout = 30 * time.Second

// Lifecycle is an instance-scoped resource initialized before Servers and
// shut down after Servers. Telemetry providers and connection pools use this
// contract without making app depend on their concrete packages.
type Lifecycle interface {
	Start(context.Context) error
	Shutdown(context.Context) error
}

// Hook participates in an App lifecycle.
//
// A Hook should use the received context for cancellation and must not install
// process-wide signal handlers.
type Hook struct {
	BeforeStart func(context.Context) error
	AfterStart  func(context.Context) error
	BeforeStop  func(context.Context) error
	AfterStop   func(context.Context) error
}

// Option configures an App.
type Option interface {
	apply(*settings) error
}

type optionFunc func(*settings) error

func (f optionFunc) apply(s *settings) error {
	return f(s)
}

type settings struct {
	servers     []server.Server
	hooks       []Hook
	components  []Component
	health      *health.Registry
	identity    *service.Identity
	stopTimeout time.Duration
}

// WithServers registers servers in startup order.
func WithServers(servers ...server.Server) Option {
	snapshot := append([]server.Server(nil), servers...)
	return optionFunc(func(s *settings) error {
		for index, srv := range snapshot {
			if isNilInterface(srv) {
				return fmt.Errorf("server %d is nil", index)
			}
		}
		s.servers = append(s.servers, snapshot...)
		return nil
	})
}

// WithHooks registers lifecycle hooks in startup order.
func WithHooks(hooks ...Hook) Option {
	snapshot := append([]Hook(nil), hooks...)
	return optionFunc(func(s *settings) error {
		s.hooks = append(s.hooks, snapshot...)
		return nil
	})
}

// WithLifecycles registers instance resources in startup order.
func WithLifecycles(lifecycles ...Lifecycle) Option {
	snapshot := append([]Lifecycle(nil), lifecycles...)
	return optionFunc(func(s *settings) error {
		for index, lifecycle := range snapshot {
			if isNilInterface(lifecycle) {
				return fmt.Errorf("lifecycle %d is nil", index)
			}
			lc := lifecycle
			s.hooks = append(s.hooks, Hook{
				BeforeStart: lc.Start,
				AfterStop:   lc.Shutdown,
			})
		}
		return nil
	})
}

// WithComponents registers an instance-scoped dependency graph.
//
// Components are topologically sorted, start before servers, and stop in
// reverse order after servers.
func WithComponents(components ...Component) Option {
	snapshot := append([]Component(nil), components...)
	return optionFunc(func(s *settings) error {
		s.components = append(s.components, snapshot...)
		return nil
	})
}

// WithIdentity assigns the immutable identity shared by this App's runtime
// components.
func WithIdentity(identity service.Identity) Option {
	return optionFunc(func(s *settings) error {
		if err := identity.Validate(); err != nil {
			return err
		}
		snapshot := identity
		s.identity = &snapshot
		return nil
	})
}

// WithHealth connects an instance-scoped health Registry to App lifecycle
// transitions.
func WithHealth(registry *health.Registry) Option {
	return optionFunc(func(s *settings) error {
		if registry == nil {
			return fmt.Errorf("health registry is nil")
		}
		s.health = registry
		return nil
	})
}

// WithStopTimeout sets the maximum time Run gives the internal graceful
// shutdown sequence.
func WithStopTimeout(timeout time.Duration) Option {
	return optionFunc(func(s *settings) error {
		if timeout <= 0 {
			return fmt.Errorf("stop timeout must be positive")
		}
		s.stopTimeout = timeout
		return nil
	})
}

func defaultSettings() settings {
	return settings{
		stopTimeout: defaultStopTimeout,
	}
}

func isNilInterface(v any) bool {
	if v == nil {
		return true
	}
	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
