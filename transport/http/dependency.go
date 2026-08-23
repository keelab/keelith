package http

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	kclient "github.com/keelab/keelith/client"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/registry"
	"github.com/keelab/keelith/selector"
	"go.opentelemetry.io/otel/propagation"
)

const defaultHTTPDependencyRollbackTimeout = 5 * time.Second

var (
	// ErrDependencyNotRunning reports an invocation outside the component's
	// active lifecycle window.
	ErrDependencyNotRunning = errors.New("http transport: dependency not running")
)

// ManagedDependencyState is the bounded HTTP dependency lifecycle state.
type ManagedDependencyState string

const (
	// ManagedDependencyNew is the state before the dependency starts.
	ManagedDependencyNew ManagedDependencyState = "new"
	// ManagedDependencyRunning is the active request-serving state.
	ManagedDependencyRunning ManagedDependencyState = "running"
	// ManagedDependencyDraining rejects new calls while existing calls finish.
	ManagedDependencyDraining ManagedDependencyState = "draining"
	// ManagedDependencyStopped is the terminal lifecycle state.
	ManagedDependencyStopped ManagedDependencyState = "stopped"
)

// DependencySelectorFactory creates one service-scoped selector over the
// exact endpoint schemes allowed by a managed HTTP dependency.
type DependencySelectorFactory func(
	schemes []string,
	options ...selector.Option,
) (selector.Selector, error)

// TransportFactory returns a distinct RoundTripper for one managed
// dependency. Implementations that return shared custom transports retain
// ownership of those transports; standard *http.Transport values should be
// cloned by the factory.
type TransportFactory func() (nethttp.RoundTripper, error)

// ManagedDependencyConfig defines one lifecycle-owned typed HTTP dependency.
type ManagedDependencyConfig struct {
	Name                  string
	Service               string
	Schemes               []string
	Discovery             registry.Discovery
	Outbound              *kclient.Outbound
	SelectorFactory       DependencySelectorFactory
	SelectorOptions       []selector.Option
	TransportFactory      TransportFactory
	ComponentDependencies []string
	ReconnectMin          time.Duration
	ReconnectMax          time.Duration
	MaxStale              time.Duration
	RollbackTimeout       time.Duration
	MetadataPolicy        metadata.Policy
	Propagator            propagation.TextMapPropagator
	MaxResponseBytes      int64
	MaxHeaderBytes        int
}

// ManagedDependencyFactoryConfig defines application-instance defaults shared
// by generated HTTP dependency bindings.
type ManagedDependencyFactoryConfig struct {
	Schemes               []string
	Discovery             registry.Discovery
	Outbound              *kclient.Outbound
	SelectorFactory       DependencySelectorFactory
	SelectorOptions       []selector.Option
	TransportFactory      TransportFactory
	ComponentDependencies []string
	ReconnectMin          time.Duration
	ReconnectMax          time.Duration
	MaxStale              time.Duration
	RollbackTimeout       time.Duration
	MetadataPolicy        metadata.Policy
	Propagator            propagation.TextMapPropagator
	MaxResponseBytes      int64
	MaxHeaderBytes        int
}

// ManagedDependencyDescription is a bounded immutable diagnostic snapshot.
type ManagedDependencyDescription struct {
	Name            string
	Service         string
	State           ManagedDependencyState
	Schemes         int
	ActiveRequests  int
	PreferenceTiers int
	Router          kclient.Description
}

// ManagedDependency owns one complete discovery-routed HTTP client path.
//
// The generated base URL is a non-routable placeholder. Client.Invoke replaces
// scheme and authority from the selected Node on every attempt.
type ManagedDependency struct {
	name         string
	service      string
	schemes      []string
	dependencies []string
	router       *kclient.Router
	gate         *managedDependencyTransport
	client       *Client
	baseURL      string
	rollback     time.Duration
	preference   int
}

// ManagedDependencyFactory creates independent service-scoped HTTP clients.
// The factory itself owns no watcher, transport, or goroutine.
type ManagedDependencyFactory struct {
	config ManagedDependencyFactoryConfig
}

// NewStandardTransportFactory builds independent standard transports with an
// optional cloned TLS 1.2+ configuration. A nil TLS config uses system roots
// for HTTPS and also permits explicitly selected HTTP endpoints.
func NewStandardTransportFactory(
	config *tls.Config,
) (TransportFactory, error) {
	if config != nil && config.MinVersion < tls.VersionTLS12 {
		return nil, fmt.Errorf(
			"%w: TLS minimum version must be 1.2 or newer",
			ErrInvalidOption,
		)
	}
	var frozen *tls.Config
	if config != nil {
		frozen = config.Clone()
	}
	return func() (nethttp.RoundTripper, error) {
		standard, ok := nethttp.DefaultTransport.(*nethttp.Transport)
		if !ok || standard == nil {
			return nil, fmt.Errorf(
				"%w: default HTTP transport is not standard",
				ErrInvalidOption,
			)
		}
		transport := standard.Clone()
		if frozen != nil {
			transport.TLSClientConfig = frozen.Clone()
		}
		return transport, nil
	}, nil
}

// NewManagedDependencyFactory validates shared generated-binding inputs.
func NewManagedDependencyFactory(
	config ManagedDependencyFactoryConfig,
) (*ManagedDependencyFactory, error) {
	if isNilManagedDependencyValue(config.Discovery) {
		return nil, fmt.Errorf("%w: discovery is nil", ErrInvalidOption)
	}
	if config.Outbound == nil {
		return nil, fmt.Errorf("%w: outbound is nil", ErrInvalidOption)
	}
	schemes, err := normalizeManagedHTTPSchemes(config.Schemes)
	if err != nil {
		return nil, err
	}
	dependencies, err := validateManagedHTTPDependencies(
		"",
		config.ComponentDependencies,
	)
	if err != nil {
		return nil, err
	}
	selectorOptions, err := copyManagedHTTPSelectorOptions(config.SelectorOptions)
	if err != nil {
		return nil, err
	}
	if config.RollbackTimeout < 0 ||
		config.MaxResponseBytes < 0 ||
		config.MaxHeaderBytes < 0 {
		return nil, fmt.Errorf(
			"%w: timeout and byte budgets must not be negative",
			ErrInvalidOption,
		)
	}
	config.Schemes = schemes
	config.ComponentDependencies = dependencies
	config.SelectorOptions = selectorOptions
	return &ManagedDependencyFactory{config: config}, nil
}

// New creates one managed dependency with a distinct Router, selector,
// standard client, and transport lifecycle.
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
		Schemes:               config.Schemes,
		Discovery:             config.Discovery,
		Outbound:              config.Outbound,
		SelectorFactory:       config.SelectorFactory,
		SelectorOptions:       config.SelectorOptions,
		TransportFactory:      config.TransportFactory,
		ComponentDependencies: config.ComponentDependencies,
		ReconnectMin:          config.ReconnectMin,
		ReconnectMax:          config.ReconnectMax,
		MaxStale:              config.MaxStale,
		RollbackTimeout:       config.RollbackTimeout,
		MetadataPolicy:        config.MetadataPolicy,
		Propagator:            config.Propagator,
		MaxResponseBytes:      config.MaxResponseBytes,
		MaxHeaderBytes:        config.MaxHeaderBytes,
	})
}

// NewManagedDependency assembles a service-scoped dynamic HTTP client.
func NewManagedDependency(
	config ManagedDependencyConfig,
) (*ManagedDependency, error) {
	name := strings.TrimSpace(config.Name)
	service := strings.TrimSpace(config.Service)
	if !validManagedHTTPName(name) || !validManagedHTTPName(service) {
		return nil, fmt.Errorf(
			"%w: dependency name or service is malformed",
			ErrInvalidOption,
		)
	}
	if isNilManagedDependencyValue(config.Discovery) {
		return nil, fmt.Errorf("%w: discovery is nil", ErrInvalidOption)
	}
	if config.Outbound == nil {
		return nil, fmt.Errorf("%w: outbound is nil", ErrInvalidOption)
	}
	schemes, err := normalizeManagedHTTPSchemes(config.Schemes)
	if err != nil {
		return nil, err
	}
	dependencies, err := validateManagedHTTPDependencies(
		name,
		config.ComponentDependencies,
	)
	if err != nil {
		return nil, err
	}
	selectorOptions, err := copyManagedHTTPSelectorOptions(config.SelectorOptions)
	if err != nil {
		return nil, err
	}
	selectorOptions = append(
		config.Outbound.SelectorOptions(),
		selectorOptions...,
	)
	selectorFactory := config.SelectorFactory
	if selectorFactory == nil {
		selectorFactory = func(
			schemes []string,
			options ...selector.Option,
		) (selector.Selector, error) {
			return selector.NewP2CForSchemes(schemes, options...)
		}
	}
	selected, err := selectorFactory(schemes, selectorOptions...)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: create selector: %w",
			ErrInvalidOption,
			err,
		)
	}
	if isNilManagedDependencyValue(selected) {
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
	transportFactory := config.TransportFactory
	if transportFactory == nil {
		transportFactory, err = NewStandardTransportFactory(nil)
		if err != nil {
			return nil, err
		}
	}
	baseTransport, err := transportFactory()
	if err != nil {
		return nil, fmt.Errorf(
			"%w: create HTTP transport: %w",
			ErrInvalidOption,
			err,
		)
	}
	if isNilManagedDependencyValue(baseTransport) {
		return nil, fmt.Errorf(
			"%w: transport factory returned nil",
			ErrInvalidOption,
		)
	}
	gate := newManagedDependencyTransport(baseTransport)
	standardClient := &nethttp.Client{
		Transport: gate,
		CheckRedirect: func(
			_ *nethttp.Request,
			_ []*nethttp.Request,
		) error {
			return nethttp.ErrUseLastResponse
		},
	}
	clientOptions := []ClientOption{
		WithClientMiddleware(config.Outbound.Middleware()),
		WithClientMetadataPolicy(config.MetadataPolicy),
		WithClientPicker(router),
	}
	if config.Propagator != nil {
		clientOptions = append(
			clientOptions,
			WithClientPropagator(config.Propagator),
		)
	}
	if config.MaxResponseBytes < 0 || config.MaxHeaderBytes < 0 {
		return nil, fmt.Errorf(
			"%w: response budgets must not be negative",
			ErrInvalidOption,
		)
	}
	if config.MaxResponseBytes > 0 {
		clientOptions = append(
			clientOptions,
			WithClientMaxResponseBytes(config.MaxResponseBytes),
		)
	}
	if config.MaxHeaderBytes > 0 {
		clientOptions = append(
			clientOptions,
			WithClientMaxHeaderBytes(config.MaxHeaderBytes),
		)
	}
	transport, err := NewClient(standardClient, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: create client transport: %w",
			ErrInvalidOption,
			err,
		)
	}
	rollback := config.RollbackTimeout
	if rollback == 0 {
		rollback = defaultHTTPDependencyRollbackTimeout
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
		schemes:      schemes,
		dependencies: dependencies,
		router:       router,
		gate:         gate,
		client:       transport,
		baseURL:      schemes[0] + "://keelith.invalid",
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

// Dependencies returns explicit App component prerequisites.
func (d *ManagedDependency) Dependencies() []string {
	if d == nil {
		return nil
	}
	return append([]string(nil), d.dependencies...)
}

// Client returns the typed generated-client-compatible HTTP transport.
func (d *ManagedDependency) Client() *Client {
	if d == nil {
		return nil
	}
	return d.client
}

// BaseURL returns a non-routable URL used only to construct typed requests.
// Every invocation replaces its scheme and authority with the selected Node.
func (d *ManagedDependency) BaseURL() string {
	if d == nil {
		return ""
	}
	return d.baseURL
}

// Start loads the first discovery snapshot before accepting requests.
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
		return fmt.Errorf("http transport: start dependency router: %w", err)
	}
	if err := d.gate.start(); err != nil {
		rollbackCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			d.rollback,
		)
		defer cancel()
		return errors.Join(err, d.router.Stop(rollbackCtx))
	}
	return nil
}

// Stop rejects new requests, drains active RoundTrips, closes idle
// connections, and finally closes the discovery watcher.
func (d *ManagedDependency) Stop(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if ctx == nil {
		return ErrNilContext
	}
	transportErr := d.gate.stop(ctx)
	routerErr := d.router.Stop(ctx)
	return errors.Join(transportErr, routerErr)
}

// Describe returns detailed local diagnostics without endpoint or metadata
// values.
func (d *ManagedDependency) Describe() ManagedDependencyDescription {
	if d == nil {
		return ManagedDependencyDescription{}
	}
	state, active := d.gate.describe()
	return ManagedDependencyDescription{
		Name:            d.name,
		Service:         d.service,
		State:           state,
		Schemes:         len(d.schemes),
		ActiveRequests:  active,
		PreferenceTiers: d.preference,
		Router:          d.router.Describe(),
	}
}

type managedDependencyTransport struct {
	base nethttp.RoundTripper

	mu      sync.Mutex
	state   ManagedDependencyState
	active  int
	drained chan struct{}
}

func newManagedDependencyTransport(
	base nethttp.RoundTripper,
) *managedDependencyTransport {
	return &managedDependencyTransport{
		base:    base,
		state:   ManagedDependencyNew,
		drained: make(chan struct{}),
	}
}

func (transport *managedDependencyTransport) start() error {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.state != ManagedDependencyNew {
		return fmt.Errorf(
			"%w: dependency state is %s",
			ErrDependencyNotRunning,
			transport.state,
		)
	}
	transport.state = ManagedDependencyRunning
	return nil
}

func (transport *managedDependencyTransport) RoundTrip(
	request *nethttp.Request,
) (*nethttp.Response, error) {
	transport.mu.Lock()
	if transport.state != ManagedDependencyRunning {
		state := transport.state
		transport.mu.Unlock()
		return nil, fmt.Errorf(
			"%w: dependency state is %s",
			ErrDependencyNotRunning,
			state,
		)
	}
	transport.active++
	transport.mu.Unlock()
	response, err := transport.base.RoundTrip(request)
	if err != nil || response == nil || response.Body == nil {
		transport.finish()
		return response, err
	}
	response.Body = &managedDependencyBody{
		body:   response.Body,
		finish: transport.finish,
	}
	return response, nil
}

type managedDependencyBody struct {
	body   io.ReadCloser
	finish func()
	once   sync.Once
}

func (body *managedDependencyBody) Read(destination []byte) (int, error) {
	count, err := body.body.Read(destination)
	if err != nil {
		body.complete()
	}
	return count, err
}

func (body *managedDependencyBody) Close() error {
	err := body.body.Close()
	body.complete()
	return err
}

func (body *managedDependencyBody) complete() {
	body.once.Do(body.finish)
}

func (transport *managedDependencyTransport) finish() {
	closeConnections := false
	transport.mu.Lock()
	if transport.active > 0 {
		transport.active--
	}
	if transport.active == 0 &&
		transport.state == ManagedDependencyDraining {
		transport.state = ManagedDependencyStopped
		close(transport.drained)
		closeConnections = true
	}
	transport.mu.Unlock()
	if closeConnections {
		transport.closeIdleConnections()
	}
}

func (transport *managedDependencyTransport) stop(ctx context.Context) error {
	transport.mu.Lock()
	switch transport.state {
	case ManagedDependencyNew:
		transport.state = ManagedDependencyStopped
		close(transport.drained)
		transport.mu.Unlock()
		transport.closeIdleConnections()
		return nil
	case ManagedDependencyStopped:
		transport.mu.Unlock()
		return nil
	case ManagedDependencyRunning:
		transport.state = ManagedDependencyDraining
		if transport.active == 0 {
			transport.state = ManagedDependencyStopped
			close(transport.drained)
		}
	}
	drained := transport.drained
	stopped := transport.state == ManagedDependencyStopped
	transport.mu.Unlock()
	if stopped {
		transport.closeIdleConnections()
		return nil
	}
	select {
	case <-drained:
		transport.closeIdleConnections()
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (transport *managedDependencyTransport) describe() (
	ManagedDependencyState,
	int,
) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.state, transport.active
}

func (transport *managedDependencyTransport) closeIdleConnections() {
	if closer, ok := transport.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func normalizeManagedHTTPSchemes(schemes []string) ([]string, error) {
	if len(schemes) == 0 || len(schemes) > 2 {
		return nil, fmt.Errorf(
			"%w: HTTP endpoint scheme count must be within 1..2",
			ErrInvalidOption,
		)
	}
	result := make([]string, 0, len(schemes))
	seen := make(map[string]struct{}, len(schemes))
	for _, scheme := range schemes {
		normalized := strings.ToLower(strings.TrimSpace(scheme))
		if normalized != "http" && normalized != "https" {
			return nil, fmt.Errorf(
				"%w: unsupported HTTP endpoint scheme %q",
				ErrInvalidOption,
				scheme,
			)
		}
		if _, duplicate := seen[normalized]; duplicate {
			return nil, fmt.Errorf(
				"%w: HTTP endpoint scheme %q is duplicated",
				ErrInvalidOption,
				normalized,
			)
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result, nil
}

func copyManagedHTTPSelectorOptions(
	options []selector.Option,
) ([]selector.Option, error) {
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

func validateManagedHTTPDependencies(
	name string,
	dependencies []string,
) ([]string, error) {
	result := make([]string, len(dependencies))
	seen := make(map[string]struct{}, len(dependencies))
	for index, d := range dependencies {
		if strings.TrimSpace(d) != d ||
			!validManagedHTTPName(d) ||
			name != "" && d == name {
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

func validManagedHTTPName(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func isNilManagedDependencyValue(value any) bool {
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

var _ nethttp.RoundTripper = (*managedDependencyTransport)(nil)
