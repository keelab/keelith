// Package xds adapts xDS v3 Endpoint Discovery Service resources to Keelith
// full-snapshot service discovery.
package xds

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/keelab/keelith/governance/admission"
	"github.com/keelab/keelith/registry"
	"google.golang.org/grpc"
)

const (
	// EndpointTypeurl is the canonical xDS v3 eds resource type.
	EndpointTypeurl = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"

	defaultMaxEndpoints      = 16_384
	maxAllowedEndpoints      = 65_536
	defaultMaxResponseBytes  = 4 * 1024 * 1024
	maxAllowedResponseBytes  = 32 * 1024 * 1024
	maxResources             = 4_096
	maxIdentityLength        = 1_024
	defaultEndpointurlScheme = "grpc"
)

var (
	// ErrInvalidOption reports an invalid connection, node identity, resource
	// mapping, or resource budget.
	ErrInvalidOption = errors.New("xds registry: invalid option")
	// ErrInvalidResponse reports an eds response outside Keelith's supported
	// deterministic projection subset.
	ErrInvalidResponse = errors.New("xds registry: invalid eds response")
	// ErrWatchClosed reports an unexpected ads stream termination.
	ErrWatchClosed = errors.New("xds registry: ads stream closed")
	// ErrClosed reports an operation after Client shutdown.
	ErrClosed = errors.New("xds registry: client closed")
)

// Resource maps one Keelith logical service to one eds cluster resource.
type Resource struct {
	Service string `config:"service"`
	Cluster string `config:"cluster"`
	Scheme  string `config:"scheme"`
}

// Options configure the xDS node identity, explicit eds subscriptions, and
// bounded resource projection.
type Options struct {
	Nodeid           string     `config:"node_id"`
	NodeCluster      string     `config:"node_cluster"`
	Resources        []Resource `config:"resources"`
	MaxEndpoints     int        `config:"max_endpoints"`
	MaxResponseBytes int        `config:"max_response_bytes"`
	// Admission receives validated eds drop_overloads policies. When it is nil,
	// responses containing drop policies remain unsupported and are NACKed.
	Admission admission.Sink `config:"-"`
}

// Description is a value-free runtime snapshot. It intentionally excludes
// node, resource, endpoint, version, nonce, and rejection values.
type Description struct {
	Closed    bool
	Watchers  int
	Resources int
	Responses uint64
	Accepted  uint64
	Rejected  uint64
	Expired   uint64
}

// Client implements registry.Discovery using explicit SotW ads v3 eds
// subscriptions. The caller retains ownership of the supplied gRPC connection.
type Client struct {
	ads              discoveryv3.AggregatedDiscoveryServiceClient
	node             *corev3.Node
	resources        map[string]Resource
	maxEndpoints     int
	maxResponseBytes int
	admission        admission.Sink

	mu        sync.Mutex
	closed    bool
	watchers  map[*watcher]struct{}
	responses uint64
	accepted  uint64
	rejected  uint64
	expired   uint64
	lastNACK  bool
	lastStale bool

	closeOnce sync.Once
	closeErr  error
}

// New constructs an eds discovery Client around a borrowed gRPC connection.
func New(
	connection grpc.ClientConnInterface,
	options Options,
) (*Client, error) {
	if isNilConnection(connection) {
		return nil, fmt.Errorf("%w: connection is nil", ErrInvalidOption)
	}
	return newClient(
		discoveryv3.NewAggregatedDiscoveryServiceClient(connection),
		options,
	)
}

func newClient(
	ads discoveryv3.AggregatedDiscoveryServiceClient,
	options Options,
) (*Client, error) {
	if ads == nil {
		return nil, fmt.Errorf("%w: ads client is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	resources := make(map[string]Resource, len(normalized.Resources))
	for _, resource := range normalized.Resources {
		resources[resource.Service] = resource
	}
	return &Client{
		ads: ads,
		node: &corev3.Node{
			Id:      normalized.Nodeid,
			Cluster: normalized.NodeCluster,
		},
		resources:        resources,
		maxEndpoints:     normalized.MaxEndpoints,
		maxResponseBytes: normalized.MaxResponseBytes,
		admission:        normalized.Admission,
		watchers:         make(map[*watcher]struct{}),
	}, nil
}

// NormalizeOptions applies stable defaults, defensively copies resource
// mappings, and validates the complete adapter contract.
func NormalizeOptions(input Options) (Options, error) {
	options := input
	options.Nodeid = strings.TrimSpace(options.Nodeid)
	options.NodeCluster = strings.TrimSpace(options.NodeCluster)
	options.Resources = append([]Resource(nil), options.Resources...)
	if options.MaxEndpoints == 0 {
		options.MaxEndpoints = defaultMaxEndpoints
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = defaultMaxResponseBytes
	}
	if options.Admission != nil && isNil(options.Admission) {
		return Options{}, fmt.Errorf(
			"%w: admission sink is nil",
			ErrInvalidOption,
		)
	}
	if !validIdentity(options.Nodeid, true) {
		return Options{}, fmt.Errorf("%w: node id is invalid", ErrInvalidOption)
	}
	if !validIdentity(options.NodeCluster, false) {
		return Options{}, fmt.Errorf(
			"%w: node cluster is invalid",
			ErrInvalidOption,
		)
	}
	if len(options.Resources) == 0 || len(options.Resources) > maxResources {
		return Options{}, fmt.Errorf(
			"%w: resources must contain 1..%d mappings",
			ErrInvalidOption,
			maxResources,
		)
	}
	if options.MaxEndpoints < 1 ||
		options.MaxEndpoints > maxAllowedEndpoints {
		return Options{}, fmt.Errorf(
			"%w: endpoint budget is outside 1..%d",
			ErrInvalidOption,
			maxAllowedEndpoints,
		)
	}
	if options.MaxResponseBytes < 1 ||
		options.MaxResponseBytes > maxAllowedResponseBytes {
		return Options{}, fmt.Errorf(
			"%w: response budget is outside 1..%d",
			ErrInvalidOption,
			maxAllowedResponseBytes,
		)
	}

	services := make(map[string]struct{}, len(options.Resources))
	for index, resource := range options.Resources {
		resource.Service = strings.TrimSpace(resource.Service)
		resource.Cluster = strings.TrimSpace(resource.Cluster)
		resource.Scheme = strings.ToLower(strings.TrimSpace(resource.Scheme))
		if resource.Scheme == "" {
			resource.Scheme = defaultEndpointurlScheme
		}
		if _, err := registry.NewSnapshot(
			resource.Service,
			"validation",
			nil,
		); err != nil {
			return Options{}, fmt.Errorf(
				"%w: resource service is invalid: %w",
				ErrInvalidOption,
				err,
			)
		}
		if !validIdentity(resource.Cluster, true) {
			return Options{}, fmt.Errorf(
				"%w: resource cluster is invalid",
				ErrInvalidOption,
			)
		}
		if _, err := registry.NewInstance(
			"validation",
			resource.Service,
			[]string{resource.Scheme + "://127.0.0.1:1"},
			nil,
		); err != nil {
			return Options{}, fmt.Errorf(
				"%w: endpoint scheme is invalid: %w",
				ErrInvalidOption,
				err,
			)
		}
		if _, duplicate := services[resource.Service]; duplicate {
			return Options{}, fmt.Errorf(
				"%w: duplicate logical service",
				ErrInvalidOption,
			)
		}
		services[resource.Service] = struct{}{}
		options.Resources[index] = resource
	}
	return options, nil
}

// ValidateOptions validates adapter settings without constructing a client.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

// Shutdown closes all active ads streams without closing the borrowed gRPC
// connection.
func (client *Client) Shutdown(ctx context.Context) error {
	if client == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	client.closeOnce.Do(func() {
		client.mu.Lock()
		client.closed = true
		watchers := make([]*watcher, 0, len(client.watchers))
		for current := range client.watchers {
			watchers = append(watchers, current)
		}
		client.mu.Unlock()
		for _, current := range watchers {
			client.closeErr = errors.Join(client.closeErr, current.Close())
		}
		if cause := context.Cause(ctx); cause != nil {
			client.closeErr = errors.Join(client.closeErr, cause)
		}
	})
	return client.closeErr
}

// WatcherCount reports currently open watchers for one logical service.
func (client *Client) WatcherCount(service string) int {
	if client == nil {
		return 0
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	count := 0
	for current := range client.watchers {
		if current.resource.Service == service {
			count++
		}
	}
	return count
}

// Describe returns bounded, value-free lifecycle and response counters.
func (client *Client) Describe() Description {
	if client == nil {
		return Description{Closed: true}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return Description{
		Closed:    client.closed,
		Watchers:  len(client.watchers),
		Resources: len(client.resources),
		Responses: client.responses,
		Accepted:  client.accepted,
		Rejected:  client.rejected,
		Expired:   client.expired,
	}
}

func (client *Client) removeWatcher(current *watcher) {
	client.mu.Lock()
	delete(client.watchers, current)
	client.mu.Unlock()
}

func (client *Client) recordResponse(accepted bool) {
	client.mu.Lock()
	client.responses++
	if accepted {
		client.accepted++
		client.lastNACK = false
		client.lastStale = false
	} else {
		client.rejected++
		client.lastNACK = true
	}
	client.mu.Unlock()
}

func (client *Client) recordExpiration() {
	client.mu.Lock()
	client.expired++
	client.lastStale = true
	client.mu.Unlock()
}

func (client *Client) degraded() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.lastNACK || client.lastStale
}

func validIdentity(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if strings.TrimSpace(value) != value ||
		len(value) > maxIdentityLength ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func isNilConnection(connection grpc.ClientConnInterface) bool {
	if connection == nil {
		return true
	}
	value := reflect.ValueOf(connection)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ registry.Discovery = (*Client)(nil)
