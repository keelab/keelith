// Package kubernetes adapts discovery.k8s.io/v1 EndpointSlices to Keelith
// full-snapshot service discovery.
package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"

	keelithconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/registry"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	discoveryclient "k8s.io/client-go/kubernetes/typed/discovery/v1"
	"k8s.io/client-go/rest"
)

const (
	defaultMaxEndpoints = 4096
	maxAllowedEndpoints = 65536
)

var (
	// ErrInvalidOption reports an invalid namespace, service mapping, port
	// mapping, client, or endpoint budget.
	ErrInvalidOption = errors.New("kubernetes registry: invalid option")
	// ErrInvalidSlice reports an EndpointSlice that cannot be represented as
	// a bounded Keelith snapshot.
	ErrInvalidSlice = errors.New("kubernetes registry: invalid endpoint slice")
	// ErrWatchClosed reports an unexpected Kubernetes watch termination.
	ErrWatchClosed = errors.New("kubernetes registry: watch closed")
	// ErrClosed reports an operation after Client shutdown.
	ErrClosed = errors.New("kubernetes registry: client closed")
)

// EndpointSliceClient is the read-only portion of the official generated
// EndpointSlice client used by Keelith.
type EndpointSliceClient interface {
	List(
		context.Context,
		metav1.ListOptions,
	) (*discoveryv1.EndpointSliceList, error)
	Watch(
		context.Context,
		metav1.ListOptions,
	) (watch.Interface, error)
}

// Options configure logical-service mapping and endpoint projection.
type Options struct {
	Namespace          string            `config:"namespace"`
	Services           map[string]string `config:"services"`
	PortSchemes        map[string]string `config:"port_schemes"`
	DefaultScheme      string            `config:"default_scheme"`
	IncludeNotReady    bool              `config:"include_not_ready"`
	IncludeTerminating bool              `config:"include_terminating"`
	MaxEndpoints       int               `config:"max_endpoints"`
}

// Description is a value-free runtime snapshot.
type Description struct {
	Closed   bool
	Watchers int
	Services int
}

// Client implements registry.Discovery. Kubernetes owns registration by
// reconciling Services, Pods, and EndpointSlices; this adapter intentionally
// does not implement registry.Registrar.
type Client struct {
	endpoints EndpointSliceClient
	options   Options

	mu       sync.Mutex
	closed   bool
	watchers map[*watcher]struct{}

	closeOnce sync.Once
}

// NewConfigBinding creates a strict construction-time Kubernetes discovery
// configuration binding.
func NewConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[Options],
) (*keelithconfig.Component[Options], error) {
	all := make(
		[]keelithconfig.ComponentOption[Options],
		0,
		len(options)+1,
	)
	all = append(
		all,
		keelithconfig.WithComponentValidator(ValidateOptions),
	)
	all = append(all, options...)
	return keelithconfig.NewComponent[Options](name, path, all...)
}

// New constructs a Discovery around one namespace-scoped official client.
func New(
	endpoints EndpointSliceClient,
	options Options,
) (*Client, error) {
	if endpoints == nil {
		return nil, fmt.Errorf("%w: endpoint client is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return &Client{
		endpoints: endpoints,
		options:   normalized,
		watchers:  make(map[*watcher]struct{}),
	}, nil
}

// Open constructs a Discovery from an explicit Kubernetes rest config.
func Open(restConfig *rest.Config, options Options) (*Client, error) {
	if restConfig == nil {
		return nil, fmt.Errorf("%w: rest config is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	config := rest.CopyConfig(restConfig)
	rest.AddUserAgent(config, "keelith-EndpointSlice-discovery")
	discovery, err := discoveryclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf(
			"kubernetes registry: create discovery client: %w",
			err,
		)
	}
	return New(discovery.EndpointSlices(normalized.Namespace), normalized)
}

// OpenInCluster constructs a Discovery from the pod service account.
func OpenInCluster(options Options) (*Client, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf(
			"kubernetes registry: in-cluster config: %w",
			err,
		)
	}
	return Open(config, options)
}

// NormalizeOptions copies maps, applies stable defaults, and validates the
// complete discovery contract.
func NormalizeOptions(input Options) (Options, error) {
	options := input
	options.Namespace = strings.TrimSpace(options.Namespace)
	options.DefaultScheme = strings.ToLower(
		strings.TrimSpace(options.DefaultScheme),
	)
	options.Services = cloneMap(options.Services)
	options.PortSchemes = cloneMap(options.PortSchemes)
	if options.MaxEndpoints == 0 {
		options.MaxEndpoints = defaultMaxEndpoints
	}
	if len(validation.IsDNS1123Label(options.Namespace)) != 0 {
		return Options{}, fmt.Errorf(
			"%w: namespace %q is invalid",
			ErrInvalidOption,
			options.Namespace,
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
	for logical, service := range options.Services {
		if _, err := registry.NewSnapshot(logical, "validation", nil); err != nil {
			return Options{}, fmt.Errorf(
				"%w: logical service %q: %w",
				ErrInvalidOption,
				logical,
				err,
			)
		}
		service = strings.TrimSpace(service)
		if len(validation.IsDNS1035Label(service)) != 0 {
			return Options{}, fmt.Errorf(
				"%w: Kubernetes service %q is invalid",
				ErrInvalidOption,
				service,
			)
		}
		options.Services[logical] = service
	}
	if options.DefaultScheme != "" &&
		!validScheme(options.DefaultScheme) {
		return Options{}, fmt.Errorf(
			"%w: default scheme %q is invalid",
			ErrInvalidOption,
			options.DefaultScheme,
		)
	}
	for port, scheme := range options.PortSchemes {
		if port == "" || len(validation.IsDNS1123Label(port)) != 0 {
			return Options{}, fmt.Errorf(
				"%w: port name %q is invalid",
				ErrInvalidOption,
				port,
			)
		}
		scheme = strings.ToLower(strings.TrimSpace(scheme))
		if !validScheme(scheme) {
			return Options{}, fmt.Errorf(
				"%w: scheme %q is invalid",
				ErrInvalidOption,
				scheme,
			)
		}
		options.PortSchemes[port] = scheme
	}
	return options, nil
}

// ValidateOptions validates discovery settings without constructing a client.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

// Watch establishes a revision-safe List/Watch for one logical service.
func (client *Client) Watch(
	ctx context.Context,
	service string,
) (registry.Watcher, error) {
	if client == nil || ctx == nil {
		return nil, fmt.Errorf(
			"%w: client or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	kubernetesService, err := client.serviceName(service)
	if err != nil {
		return nil, err
	}
	selector := labels.Set{
		discoveryv1.LabelServiceName: kubernetesService,
	}.AsSelector().String()
	list, err := client.endpoints.List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"kubernetes registry: list EndpointSlices: %w",
			err,
		)
	}
	state := make(map[string]*discoveryv1.EndpointSlice, len(list.Items))
	for index := range list.Items {
		slice := list.Items[index].DeepCopy()
		state[slice.Name] = slice
	}
	initial, err := client.snapshot(
		service,
		list.ResourceVersion,
		state,
	)
	if err != nil {
		return nil, err
	}
	watchContext, cancel := context.WithCancel(ctx)
	stream, err := client.endpoints.Watch(
		watchContext,
		metav1.ListOptions{
			LabelSelector:       selector,
			ResourceVersion:     list.ResourceVersion,
			AllowWatchBookmarks: true,
		},
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf(
			"kubernetes registry: watch EndpointSlices: %w",
			err,
		)
	}
	if stream == nil {
		cancel()
		return nil, fmt.Errorf(
			"%w: endpoint watch is nil",
			ErrInvalidOption,
		)
	}
	current := &watcher{
		client:   client,
		service:  service,
		context:  watchContext,
		cancel:   cancel,
		stream:   stream,
		state:    state,
		revision: list.ResourceVersion,
		updates:  make(chan registry.Snapshot, 1),
		terminal: make(chan error, 1),
		done:     make(chan struct{}),
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		cancel()
		stream.Stop()
		return nil, ErrClosed
	}
	client.watchers[current] = struct{}{}
	client.mu.Unlock()
	current.publish(initial)
	go current.run()
	return current, nil
}

// Shutdown closes all active watches. The official generated client has no
// closeable transport ownership.
func (client *Client) Shutdown(context.Context) error {
	if client == nil {
		return nil
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
			_ = current.Close()
		}
	})
	return nil
}

// Describe returns value-free lifecycle state.
func (client *Client) Describe() Description {
	if client == nil {
		return Description{Closed: true}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return Description{
		Closed:   client.closed,
		Watchers: len(client.watchers),
		Services: len(client.options.Services),
	}
}

// WatcherCount returns active logical watchers for one service.
func (client *Client) WatcherCount(service string) int {
	if client == nil {
		return 0
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	count := 0
	for current := range client.watchers {
		if current.service == service {
			count++
		}
	}
	return count
}

func (client *Client) serviceName(logical string) (string, error) {
	if _, err := registry.NewSnapshot(logical, "validation", nil); err != nil {
		return "", err
	}
	if service := client.options.Services[logical]; service != "" {
		return service, nil
	}
	if len(validation.IsDNS1035Label(logical)) == 0 {
		return logical, nil
	}
	return "", fmt.Errorf(
		"%w: logical service %q requires an explicit Kubernetes service mapping",
		ErrInvalidOption,
		logical,
	)
}

func (client *Client) snapshot(
	service string,
	revision string,
	state map[string]*discoveryv1.EndpointSlice,
) (registry.Snapshot, error) {
	if strings.TrimSpace(revision) == "" {
		revision = "0"
	}
	names := make([]string, 0, len(state))
	for name := range state {
		names = append(names, name)
	}
	sort.Strings(names)
	type projected struct {
		endpoints map[string]struct{}
		metadata  map[string]string
	}
	instances := make(map[string]*projected)
	for _, name := range names {
		slice := state[name]
		if slice == nil {
			continue
		}
		ports, err := client.projectPorts(slice.Ports)
		if err != nil {
			return registry.Snapshot{}, fmt.Errorf(
				"%w: EndpointSlice %q: %w",
				ErrInvalidSlice,
				name,
				err,
			)
		}
		if len(ports) == 0 {
			continue
		}
		for _, endpoint := range slice.Endpoints {
			if !client.acceptEndpoint(endpoint.Conditions) {
				continue
			}
			for _, address := range endpoint.Addresses {
				address = strings.TrimSpace(address)
				if address == "" {
					return registry.Snapshot{}, fmt.Errorf(
						"%w: EndpointSlice %q has an empty address",
						ErrInvalidSlice,
						name,
					)
				}
				id := endpointid(
					client.options.Namespace,
					service,
					address,
				)
				current := instances[id]
				if current == nil {
					if len(instances) >= client.options.MaxEndpoints {
						return registry.Snapshot{}, fmt.Errorf(
							"%w: endpoint budget %d exceeded",
							ErrInvalidSlice,
							client.options.MaxEndpoints,
						)
					}
					current = &projected{
						endpoints: make(map[string]struct{}),
						metadata:  endpointMetadata(endpoint),
					}
					instances[id] = current
				}
				for _, port := range ports {
					current.endpoints[port.scheme+"://"+
						net.JoinHostPort(
							address,
							fmt.Sprintf("%d", port.number),
						)] = struct{}{}
				}
			}
		}
	}
	result := make([]registry.Instance, 0, len(instances))
	ids := make([]string, 0, len(instances))
	for id := range instances {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		projected := instances[id]
		endpoints := make([]string, 0, len(projected.endpoints))
		for endpoint := range projected.endpoints {
			endpoints = append(endpoints, endpoint)
		}
		sort.Strings(endpoints)
		instance, err := registry.NewInstance(
			id,
			service,
			endpoints,
			projected.metadata,
		)
		if err != nil {
			return registry.Snapshot{}, err
		}
		result = append(result, instance)
	}
	return registry.NewSnapshot(
		service,
		"kubernetes:"+revision,
		result,
	)
}

type projectedPort struct {
	scheme string
	number int32
}

func (client *Client) projectPorts(
	ports []discoveryv1.EndpointPort,
) ([]projectedPort, error) {
	result := make([]projectedPort, 0, len(ports))
	for _, port := range ports {
		if port.Port == nil || *port.Port < 1 || *port.Port > 65535 {
			return nil, errors.New("port is missing or outside 1..65535")
		}
		if port.Protocol != nil && *port.Protocol != corev1.ProtocolTCP {
			continue
		}
		name := ""
		if port.Name != nil {
			name = strings.TrimSpace(*port.Name)
		}
		scheme := client.options.PortSchemes[name]
		if scheme == "" && inferredScheme(name) {
			scheme = name
		}
		if scheme == "" {
			scheme = client.options.DefaultScheme
		}
		if scheme == "" {
			continue
		}
		result = append(result, projectedPort{
			scheme: scheme,
			number: *port.Port,
		})
	}
	return result, nil
}

func (client *Client) acceptEndpoint(
	conditions discoveryv1.EndpointConditions,
) bool {
	if !client.options.IncludeTerminating &&
		conditions.Terminating != nil &&
		*conditions.Terminating {
		return false
	}
	if !client.options.IncludeNotReady &&
		conditions.Ready != nil &&
		!*conditions.Ready {
		return false
	}
	return true
}

func (client *Client) removeWatcher(current *watcher) {
	client.mu.Lock()
	delete(client.watchers, current)
	client.mu.Unlock()
}

type watcher struct {
	client   *Client
	service  string
	context  context.Context
	cancel   context.CancelFunc
	stream   watch.Interface
	state    map[string]*discoveryv1.EndpointSlice
	revision string

	updates  chan registry.Snapshot
	terminal chan error
	done     chan struct{}
	once     sync.Once
}

func (watcher *watcher) Next(
	ctx context.Context,
) (registry.Snapshot, error) {
	select {
	case snapshot := <-watcher.updates:
		return snapshot.Clone(), nil
	case err := <-watcher.terminal:
		return registry.Snapshot{}, err
	case <-watcher.done:
		return registry.Snapshot{}, registry.ErrWatcherClosed
	case <-ctx.Done():
		return registry.Snapshot{}, context.Cause(ctx)
	}
}

func (watcher *watcher) Close() error {
	watcher.once.Do(func() {
		watcher.client.removeWatcher(watcher)
		watcher.cancel()
		watcher.stream.Stop()
		close(watcher.done)
	})
	return nil
}

func (watcher *watcher) run() {
	for {
		select {
		case <-watcher.context.Done():
			return
		case event, open := <-watcher.stream.ResultChan():
			if !open {
				watcher.fail(ErrWatchClosed)
				return
			}
			if err := watcher.apply(event); err != nil {
				watcher.fail(err)
				return
			}
		}
	}
}

func (watcher *watcher) apply(event watch.Event) error {
	if event.Type == watch.Error {
		err := apierrors.FromObject(event.Object)
		return fmt.Errorf("kubernetes registry: watch event: %w", err)
	}
	slice, ok := event.Object.(*discoveryv1.EndpointSlice)
	if !ok || slice == nil {
		if event.Type == watch.Bookmark {
			if object, metadataOK := event.Object.(metav1.Object); metadataOK {
				watcher.revision = object.GetResourceVersion()
			}
			return nil
		}
		return fmt.Errorf(
			"%w: watch object is %T",
			ErrInvalidSlice,
			event.Object,
		)
	}
	if slice.ResourceVersion != "" {
		watcher.revision = slice.ResourceVersion
	}
	switch event.Type {
	case watch.Added, watch.Modified:
		watcher.state[slice.Name] = slice.DeepCopy()
	case watch.Deleted:
		delete(watcher.state, slice.Name)
	case watch.Bookmark:
		return nil
	default:
		return fmt.Errorf(
			"%w: unsupported watch event %q",
			ErrInvalidSlice,
			event.Type,
		)
	}
	snapshot, err := watcher.client.snapshot(
		watcher.service,
		watcher.revision,
		watcher.state,
	)
	if err != nil {
		return err
	}
	watcher.publish(snapshot)
	return nil
}

func (watcher *watcher) publish(snapshot registry.Snapshot) {
	select {
	case <-watcher.done:
		return
	default:
	}
	select {
	case <-watcher.updates:
	default:
	}
	select {
	case watcher.updates <- snapshot.Clone():
	default:
	}
}

func (watcher *watcher) fail(err error) {
	select {
	case <-watcher.done:
		return
	default:
	}
	select {
	case watcher.terminal <- err:
	default:
	}
}

func endpointid(namespace, service, address string) string {
	hash := sha256.Sum256(
		[]byte(namespace + "\x00" + service + "\x00" + address),
	)
	return "k8s-" + hex.EncodeToString(hash[:12])
}

func endpointMetadata(endpoint discoveryv1.Endpoint) map[string]string {
	metadata := make(map[string]string, 4)
	if endpoint.Zone != nil && strings.TrimSpace(*endpoint.Zone) != "" {
		metadata["topology.kubernetes.io/zone"] = *endpoint.Zone
		metadata["cloud.availability_zone"] = *endpoint.Zone
	}
	if endpoint.NodeName != nil && strings.TrimSpace(*endpoint.NodeName) != "" {
		metadata["kubernetes.io/node"] = *endpoint.NodeName
	}
	if endpoint.Hostname != nil && strings.TrimSpace(*endpoint.Hostname) != "" {
		metadata["kubernetes.io/hostname"] = *endpoint.Hostname
	}
	return metadata
}

func inferredScheme(name string) bool {
	switch name {
	case "grpc", "grpcs", "http", "https":
		return true
	default:
		return false
	}
}

func validScheme(scheme string) bool {
	if strings.ContainsAny(scheme, "/:") {
		return false
	}
	parsed, err := url.Parse(scheme + "://127.0.0.1:1")
	return err == nil && parsed.Scheme == scheme
}

func cloneMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

var _ registry.Discovery = (*Client)(nil)
