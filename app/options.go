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
	// Start initializes the lifecycle.
	Start(context.Context) error
	// Shutdown shuts down the lifecycle.
	Shutdown(context.Context) error
}

// Hook participates in an App lifecycle.
//
// A Hook should use the received context for cancellation and must not install
// process-wide signal handlers.
type Hook struct {
	// BeforeStart is called before the lifecycle is started.
	BeforeStart func(context.Context) error
	// AfterStart is called after the lifecycle is started.
	AfterStart func(context.Context) error
	// BeforeStop is called before the lifecycle is stopped.
	BeforeStop func(context.Context) error
	// AfterStop is called after the lifecycle is stopped.
	AfterStop func(context.Context) error
}

// Option configures an App.
type Option interface {
	// apply applies the option to the given options.
	apply(*options) error
}

type optionFunc func(*options) error

func (f optionFunc) apply(options *options) error {
	return f(options)
}

type options struct {
	servers     []server.Server   // servers to start in startup order
	hooks       []Hook            // lifecycle hooks to execute in startup order
	components  []Component       // components to start in startup order
	health      *health.Registry  // health registry
	identity    *service.Identity // service identity
	stopTimeout time.Duration     // stop timeout for the lifecycle
}

// WithServers registers servers in startup order.
func WithServers(servers ...server.Server) Option {
	snapshot := append([]server.Server(nil), servers...)
	return optionFunc(func(options *options) error {
		for index, component := range snapshot {
			if isNilServer(component) {
				return fmt.Errorf("server %d is nil", index)
			}
		}
		options.servers = append(options.servers, snapshot...)
		return nil
	})
}

// WithHooks registers lifecycle hooks in startup order.
func WithHooks(hooks ...Hook) Option {
	snapshot := append([]Hook(nil), hooks...)
	return optionFunc(func(options *options) error {
		options.hooks = append(options.hooks, snapshot...)
		return nil
	})
}

// WithLifecycles registers instance resources in startup order.
func WithLifecycles(lifecycles ...Lifecycle) Option {
	snapshot := append([]Lifecycle(nil), lifecycles...)
	return optionFunc(func(options *options) error {
		for index, lifecycle := range snapshot {
			if isNilLifecycle(lifecycle) {
				return fmt.Errorf("lifecycle %d is nil", index)
			}
			resource := lifecycle
			options.hooks = append(options.hooks, Hook{
				BeforeStart: resource.Start,
				AfterStop:   resource.Shutdown,
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
	return optionFunc(func(options *options) error {
		options.components = append(options.components, snapshot...)
		return nil
	})
}

// WithIdentity assigns the immutable identity shared by this App's runtime
// components.
func WithIdentity(identity service.Identity) Option {
	return optionFunc(func(options *options) error {
		if err := identity.Validate(); err != nil {
			return err
		}
		snapshot := identity
		options.identity = &snapshot
		return nil
	})
}

// WithHealth connects an instance-scoped health Registry to App lifecycle
// transitions.
func WithHealth(registry *health.Registry) Option {
	return optionFunc(func(options *options) error {
		if registry == nil {
			return fmt.Errorf("health registry is nil")
		}
		options.health = registry
		return nil
	})
}

// WithStopTimeout sets the maximum time Run gives the internal graceful
// shutdown sequence.
func WithStopTimeout(timeout time.Duration) Option {
	return optionFunc(func(options *options) error {
		if timeout <= 0 {
			return fmt.Errorf("stop timeout must be positive")
		}
		options.stopTimeout = timeout
		return nil
	})
}

func defaultOptions() options {
	return options{
		stopTimeout: defaultStopTimeout,
	}
}
func isNilServer(component server.Server) bool {
	if component == nil {
		return true
	}

	value := reflect.ValueOf(component)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilLifecycle(lifecycle Lifecycle) bool {
	if lifecycle == nil {
		return true
	}
	value := reflect.ValueOf(lifecycle)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
