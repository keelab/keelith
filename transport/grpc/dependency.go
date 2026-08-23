package grpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	kclient "github.com/keelab/keelith/client"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/registry"
	"github.com/keelab/keelith/selector"
	"go.opentelemetry.io/otel/propagation"
	ggrpc "google.golang.org/grpc"
)

const defaultDependencyRollbackTimeout = 5 * time.Second

// SelectorFactory creates one service-scoped Selector while preserving
// Outbound instance-health options.
type SelectorFactory func(
	scheme string,
	options ...selector.Option,
) (selector.Selector, error)

// ManagedDependencyConfig defines one lifecycle-owned gRPC dependency.
//
// Discovery, Dial, and Outbound are application-instance dependencies. The
// managed dependency owns only its Router, dynamic connection pool, and
// transport wrapper.
type ManagedDependencyConfig struct {
	Name                  string
	Service               string
	Discovery             registry.Discovery
	Outbound              *kclient.Outbound
	Dial                  DialFunc
	SelectorFactory       SelectorFactory
	SelectorOptions       []selector.Option
	ComponentDependencies []string
	ReconnectMin          time.Duration
	ReconnectMax          time.Duration
	MaxStale              time.Duration
	MaxConnections        int
	IdleTimeout           time.Duration
	RollbackTimeout       time.Duration
	MetadataPolicy        metadata.Policy
	Propagator            propagation.TextMapPropagator
	ErrorCodec            *ErrorCodec
}

// ManagedDependencyFactoryConfig defines application-instance defaults shared
// by generated gRPC dependency bindings.
type ManagedDependencyFactoryConfig struct {
	Discovery             registry.Discovery
	Outbound              *kclient.Outbound
	Dial                  DialFunc
	SelectorFactory       SelectorFactory
	SelectorOptions       []selector.Option
	ComponentDependencies []string
	ReconnectMin          time.Duration
	ReconnectMax          time.Duration
	MaxStale              time.Duration
	MaxConnections        int
	IdleTimeout           time.Duration
	RollbackTimeout       time.Duration
	MetadataPolicy        metadata.Policy
	Propagator            propagation.TextMapPropagator
	ErrorCodec            *ErrorCodec
}

// ManagedDependencyDescription is an immutable diagnostic snapshot.
//
// This detailed description may contain service identity and provider error
// summaries. Use the Ops adapter for an HTTP-safe, value-free projection.
type ManagedDependencyDescription struct {
	Name            string
	Service         string
	PreferenceTiers int
	Router          kclient.Description
	Connection      DiscoveryDescription
}

// ManagedDependency owns the complete dynamic gRPC client path for one
// logical service.
//
// It implements app.Component structurally. Start initializes discovery
// before the connection pool; Stop drains connections before closing the
// discovery watcher.
type ManagedDependency struct {
	name         string
	service      string
	dependencies []string
	router       *kclient.Router
	connection   *DiscoveryConnection
	client       *Client
	rollback     time.Duration
	preference   int
}

// ManagedDependencyFactory creates independent service-scoped components that
// share application-owned discovery, dial, policy, and telemetry defaults.
//
// The factory owns no goroutines or connections.
type ManagedDependencyFactory struct {
	config ManagedDependencyFactoryConfig
}

// NewManagedDependencyFactory validates shared generated-binding inputs.
func NewManagedDependencyFactory(
	config ManagedDependencyFactoryConfig,
) (*ManagedDependencyFactory, error) {
	if isNilDependencyValue(config.Discovery) {
		return nil, fmt.Errorf("%w: discovery is nil", ErrInvalidOption)
	}
	if config.Outbound == nil {
		return nil, fmt.Errorf("%w: outbound is nil", ErrInvalidOption)
	}
	if config.Dial == nil {
		return nil, fmt.Errorf("%w: dial function is nil", ErrInvalidOption)
	}
	dependencies, err := validateFactoryDependencies(
		config.ComponentDependencies,
	)
	if err != nil {
		return nil, err
	}
	if config.RollbackTimeout < 0 {
		return nil, fmt.Errorf(
			"%w: rollback timeout must not be negative",
			ErrInvalidOption,
		)
	}
	selectorOptions, err := copySelectorOptions(config.SelectorOptions)
	if err != nil {
		return nil, err
	}
	config.ComponentDependencies = dependencies
	config.SelectorOptions = selectorOptions
	return &ManagedDependencyFactory{config: config}, nil
}

// New creates one managed dependency with a distinct Router, Selector,
// connection pool, and lifecycle.
func (f *ManagedDependencyFactory) New(
	name string,
	service string,
) (*ManagedDependency, error) {
	if f == nil {
		return nil, fmt.Errorf(
			"%w: managed dependency factory is nil",
			ErrInvalidOption,
		)
	}
	config := f.config
	return NewManagedDependency(ManagedDependencyConfig{
		Name:                  name,
		Service:               service,
		Discovery:             config.Discovery,
		Outbound:              config.Outbound,
		Dial:                  config.Dial,
		SelectorFactory:       config.SelectorFactory,
		SelectorOptions:       config.SelectorOptions,
		ComponentDependencies: config.ComponentDependencies,
		ReconnectMin:          config.ReconnectMin,
		ReconnectMax:          config.ReconnectMax,
		MaxStale:              config.MaxStale,
		MaxConnections:        config.MaxConnections,
		IdleTimeout:           config.IdleTimeout,
		RollbackTimeout:       config.RollbackTimeout,
		MetadataPolicy:        config.MetadataPolicy,
		Propagator:            config.Propagator,
		ErrorCodec:            config.ErrorCodec,
	})
}

// NewManagedDependency assembles a service-scoped dynamic gRPC client.
func NewManagedDependency(
	config ManagedDependencyConfig,
) (*ManagedDependency, error) {
	name := strings.TrimSpace(config.Name)
	service := strings.TrimSpace(config.Service)
	if !validDiscoveryName(name) || !validDiscoveryName(service) {
		return nil, fmt.Errorf(
			"%w: dependency name or service is malformed",
			ErrInvalidOption,
		)
	}
	if isNilDependencyValue(config.Discovery) {
		return nil, fmt.Errorf("%w: discovery is nil", ErrInvalidOption)
	}
	if config.Outbound == nil {
		return nil, fmt.Errorf("%w: outbound is nil", ErrInvalidOption)
	}
	if config.Dial == nil {
		return nil, fmt.Errorf("%w: dial function is nil", ErrInvalidOption)
	}
	dependencies, err := validateComponentDependencies(
		name,
		config.ComponentDependencies,
	)
	if err != nil {
		return nil, err
	}
	f := config.SelectorFactory
	if f == nil {
		f = defaultSelectorFactory
	}
	selectorOptions, err := copySelectorOptions(config.SelectorOptions)
	if err != nil {
		return nil, err
	}
	selectorOptions = append(
		config.Outbound.SelectorOptions(),
		selectorOptions...,
	)
	selected, err := f("grpc", selectorOptions...)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: create selector: %w",
			ErrInvalidOption,
			err,
		)
	}
	if isNilDependencyValue(selected) {
		return nil, fmt.Errorf(
			"%w: selector factory returned nil",
			ErrInvalidOption,
		)
	}
	preferenceTiers := 0
	if describer, ok := selected.(selector.PreferenceDescriber); ok {
		preferenceTiers = describer.PreferenceTierCount()
	}
	router, err := kclient.NewRouter(kclient.RouterConfig{
		Name:         name + ".router",
		Service:      service,
		Discovery:    config.Discovery,
		Selector:     selected,
		ReconnectMin: config.ReconnectMin,
		ReconnectMax: config.ReconnectMax,
		MaxStale:     config.MaxStale,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"%w: create router: %w",
			ErrInvalidOption,
			err,
		)
	}
	connection, err := NewDiscoveryConnection(DiscoveryConnectionConfig{
		Name:           name + ".connection",
		Picker:         router,
		NodeChanges:    router,
		Dial:           config.Dial,
		MaxConnections: config.MaxConnections,
		IdleTimeout:    config.IdleTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"%w: create discovery connection: %w",
			ErrInvalidOption,
			err,
		)
	}
	options := []ClientOption{
		WithClientMiddleware(config.Outbound.Middleware()),
		WithClientStreamMiddleware(config.Outbound.StreamMiddleware()),
		WithClientMetadataPolicy(config.MetadataPolicy),
	}
	if config.Propagator != nil {
		options = append(options, WithClientPropagator(config.Propagator))
	}
	if config.ErrorCodec != nil {
		options = append(options, WithClientErrorCodec(config.ErrorCodec))
	}
	transport, err := NewClient(connection, options...)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: create client transport: %w",
			ErrInvalidOption,
			err,
		)
	}
	rollback := config.RollbackTimeout
	if rollback == 0 {
		rollback = defaultDependencyRollbackTimeout
	}
	if rollback <= 0 {
		return nil, fmt.Errorf(
			"%w: rollback timeout must be positive",
			ErrInvalidOption,
		)
	}
	return &ManagedDependency{
		name:         name,
		service:      service,
		dependencies: dependencies,
		router:       router,
		connection:   connection,
		client:       transport,
		rollback:     rollback,
		preference:   preferenceTiers,
	}, nil
}

// Name returns the stable App component name.
func (d *ManagedDependency) Name() string {
	if d == nil {
		return ""
	}
	return d.name
}

// Service returns the exact Protobuf service identity.
func (d *ManagedDependency) Service() string {
	if d == nil {
		return ""
	}
	return d.service
}

// Dependencies returns explicit App component prerequisites such as a
// provider-owned registry connection.
func (d *ManagedDependency) Dependencies() []string {
	if d == nil {
		return nil
	}
	return append([]string(nil), d.dependencies...)
}

// ClientConn returns the generated-client-compatible governed transport.
//
// It is safe to construct typed clients before Start. Invocations are rejected
// until the App starts this component.
func (d *ManagedDependency) ClientConn() ggrpc.ClientConnInterface {
	if d == nil {
		return nil
	}
	return d.client
}

// Start loads the first discovery snapshot and then enables pooled calls.
func (d *ManagedDependency) Start(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("%w: managed dependency is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := d.router.Start(ctx); err != nil {
		return fmt.Errorf("grpc transport: start dependency router: %w", err)
	}
	if err := d.connection.Start(ctx); err != nil {
		rollbackCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			d.rollback,
		)
		defer cancel()
		rollbackErr := errors.Join(
			d.connection.Stop(rollbackCtx),
			d.router.Stop(rollbackCtx),
		)
		return errors.Join(
			fmt.Errorf(
				"grpc transport: start dependency connection: %w",
				err,
			),
			rollbackErr,
		)
	}
	return nil
}

// Stop drains calls and streams before closing the discovery watcher.
func (d *ManagedDependency) Stop(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if ctx == nil {
		return ErrNilContext
	}
	connectionErr := d.connection.Stop(ctx)
	routerErr := d.router.Stop(ctx)
	return errors.Join(connectionErr, routerErr)
}

// Describe returns detailed local diagnostics for the managed path.
func (d *ManagedDependency) Describe() ManagedDependencyDescription {
	if d == nil {
		return ManagedDependencyDescription{}
	}
	return ManagedDependencyDescription{
		Name:            d.name,
		Service:         d.service,
		PreferenceTiers: d.preference,
		Router:          d.router.Describe(),
		Connection:      d.connection.Describe(),
	}
}

func copySelectorOptions(options []selector.Option) ([]selector.Option, error) {
	result := append([]selector.Option(nil), options...)
	for index, option := range result {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: selector option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
	}
	return result, nil
}

func defaultSelectorFactory(
	scheme string,
	options ...selector.Option,
) (selector.Selector, error) {
	return selector.NewP2C(scheme, options...)
}

func validateComponentDependencies(
	name string,
	dependencies []string,
) ([]string, error) {
	result := make([]string, len(dependencies))
	seen := make(map[string]struct{}, len(dependencies))
	for index, d := range dependencies {
		if strings.TrimSpace(d) != d ||
			!validDiscoveryName(d) ||
			d == name {
			return nil, fmt.Errorf(
				"%w: component dependency %d is malformed",
				ErrInvalidOption,
				index,
			)
		}
		if _, duplicate := seen[d]; duplicate {
			return nil, fmt.Errorf(
				"%w: component dependency %q is duplicated",
				ErrInvalidOption,
				d,
			)
		}
		seen[d] = struct{}{}
		result[index] = d
	}
	return result, nil
}

func validateFactoryDependencies(
	dependencies []string,
) ([]string, error) {
	result := make([]string, len(dependencies))
	seen := make(map[string]struct{}, len(dependencies))
	for index, d := range dependencies {
		if strings.TrimSpace(d) != d ||
			!validDiscoveryName(d) {
			return nil, fmt.Errorf(
				"%w: component dependency %d is malformed",
				ErrInvalidOption,
				index,
			)
		}
		if _, duplicate := seen[d]; duplicate {
			return nil, fmt.Errorf(
				"%w: component dependency %q is duplicated",
				ErrInvalidOption,
				d,
			)
		}
		seen[d] = struct{}{}
		result[index] = d
	}
	return result, nil
}

func isNilDependencyValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ ggrpc.ClientConnInterface = (*Client)(nil)
