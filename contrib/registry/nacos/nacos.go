// Package nacos adapts nacos naming to Keelith registry contracts.
package nacos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	nacosruntime "github.com/keelab/contrib/nacos"
	"github.com/keelab/keelith/registry"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

const (
	instanceidKey = "keelith.instance.ID"
	endpointsKey  = "keelith.endpoints"

	maxBootstrapLists = 8
)

var (
	// ErrInvalidOption reports an invalid client, scheme, group, or instance.
	ErrInvalidOption = errors.New("nacos registry: invalid option")
	// ErrRejected reports a nacos register/deregister false response.
	ErrRejected = errors.New("nacos registry: operation rejected")
)

// BackendInstance is the minimal naming representation used by Client.
type BackendInstance struct {
	ID       string
	IP       string
	Port     uint64
	Metadata map[string]string
}

// Backend isolates registry semantics from the concrete nacos SDK.
type Backend interface {
	Register(
		context.Context,
		string,
		string,
		string,
		bool,
		BackendInstance,
	) error
	Deregister(
		context.Context,
		string,
		string,
		string,
		bool,
		BackendInstance,
	) error
	List(
		context.Context,
		string,
		string,
		string,
	) ([]BackendInstance, error)
	Subscribe(
		context.Context,
		string,
		string,
		string,
		func([]BackendInstance, error),
	) (func() error, error)
	Close()
}

// Options define how Keelith endpoints map to nacos instances.
type Options struct {
	Scheme    string `config:"scheme"`
	Group     string `config:"group"`
	Cluster   string `config:"cluster"`
	Ephemeral bool   `config:"ephemeral"`
	Owns      bool   `config:"owns"`
}

// Client implements registry.Registrar and registry.Discovery.
type Client struct {
	backend   Backend
	scheme    string
	group     string
	cluster   string
	ephemeral bool
	owns      bool

	mu       sync.Mutex
	watchers map[*watcher]struct{}
	closed   bool

	closeOnce sync.Once
	closeErr  error
}

// Description is a value-free runtime snapshot.
type Description struct {
	Closed   bool
	Watchers int
}

// New constructs a Client around an official nacos naming client.
func New(
	client naming_client.INamingClient,
	options Options,
) (*Client, error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	return Wrap(&sdkBackend{client: client}, options)
}

// Open constructs and owns an official nacos SDK client through the shared,
// secret-safe runtime configuration.
func Open(
	ctx context.Context,
	clientConfig nacosruntime.Config,
	resolver nacosruntime.SecretResolver,
	options Options,
) (*Client, error) {
	client, err := nacosruntime.OpenNamingClient(
		ctx,
		clientConfig,
		resolver,
	)
	if err != nil {
		return nil, err
	}
	options.Owns = true
	registryClient, err := New(client, options)
	if err != nil {
		client.CloseClient()
		return nil, err
	}
	return registryClient, nil
}

// Wrap constructs a Client around a custom Backend.
func Wrap(backend Backend, options Options) (*Client, error) {
	if isNil(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	if err := ValidateOptions(options); err != nil {
		return nil, err
	}
	scheme := strings.ToLower(strings.TrimSpace(options.Scheme))
	if scheme == "" {
		scheme = "grpc"
	}
	if _, err := url.Parse(scheme + "://127.0.0.1:1"); err != nil ||
		strings.ContainsAny(scheme, "/:") {
		return nil, fmt.Errorf("%w: scheme %q", ErrInvalidOption, scheme)
	}
	group := strings.TrimSpace(options.Group)
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	cluster := strings.TrimSpace(options.Cluster)
	if cluster == "" {
		cluster = "DEFAULT"
	}
	return &Client{
		backend:   backend,
		scheme:    scheme,
		group:     group,
		cluster:   cluster,
		ephemeral: options.Ephemeral,
		owns:      options.Owns,
		watchers:  make(map[*watcher]struct{}),
	}, nil
}

// ValidateOptions validates the stable nacos namespace and endpoint scheme.
func ValidateOptions(options Options) error {
	scheme := strings.ToLower(strings.TrimSpace(options.Scheme))
	if scheme == "" {
		scheme = "grpc"
	}
	if _, err := url.Parse(scheme + "://127.0.0.1:1"); err != nil ||
		strings.ContainsAny(scheme, "/:") {
		return fmt.Errorf("%w: scheme %q", ErrInvalidOption, scheme)
	}
	for name, value := range map[string]string{
		"group":   options.Group,
		"cluster": options.Cluster,
	} {
		if strings.ContainsAny(value, "\r\n\x00") {
			return fmt.Errorf("%w: %s contains control bytes", ErrInvalidOption, name)
		}
	}
	return nil
}

// Register publishes one instance using the configured endpoint scheme.
func (client *Client) Register(
	ctx context.Context,
	instance registry.Instance,
) error {
	if client == nil || isNil(client.backend) || ctx == nil {
		return fmt.Errorf("%w: client or context is nil", ErrInvalidOption)
	}
	backendInstance, err := client.toBackend(instance)
	if err != nil {
		return err
	}
	if err := client.backend.Register(
		ctx,
		instance.Service(),
		client.group,
		client.cluster,
		client.ephemeral,
		backendInstance,
	); err != nil {
		return fmt.Errorf("nacos registry: register: %w", err)
	}
	return nil
}

// Deregister removes one instance.
func (client *Client) Deregister(
	ctx context.Context,
	instance registry.Instance,
) error {
	if client == nil || isNil(client.backend) || ctx == nil {
		return fmt.Errorf("%w: client or context is nil", ErrInvalidOption)
	}
	backendInstance, err := client.toBackend(instance)
	if err != nil {
		return err
	}
	if err := client.backend.Deregister(
		ctx,
		instance.Service(),
		client.group,
		client.cluster,
		client.ephemeral,
		backendInstance,
	); err != nil {
		return fmt.Errorf("nacos registry: deregister: %w", err)
	}
	return nil
}

// Watch returns full, revisioned snapshots for one service.
func (client *Client) Watch(
	ctx context.Context,
	service string,
) (registry.Watcher, error) {
	if client == nil || isNil(client.backend) {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	if ctx == nil || strings.TrimSpace(service) == "" {
		return nil, fmt.Errorf("%w: context or service is invalid", ErrInvalidOption)
	}
	current := &watcher{
		client:        client,
		service:       service,
		updates:       make(chan registry.Snapshot, 1),
		errs:          make(chan error, 1),
		done:          make(chan struct{}),
		bootstrapping: true,
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil, registry.ErrWatcherClosed
	}
	client.watchers[current] = struct{}{}
	client.mu.Unlock()

	cancel, err := client.backend.Subscribe(
		ctx,
		service,
		client.group,
		client.cluster,
		current.onUpdate,
	)
	if err != nil {
		current.closeWithoutCancel()
		return nil, fmt.Errorf("nacos registry: subscribe: %w", err)
	}
	current.cancel = cancel
	if err := current.bootstrap(ctx); err != nil {
		_ = current.Close()
		return nil, err
	}
	return current, nil
}

// Shutdown closes all watchers and an owned nacos client.
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
			client.closeErr = errors.Join(client.closeErr, current.Close())
		}
		if client.owns {
			client.backend.Close()
		}
	})
	return client.closeErr
}

// WatcherCount returns open watchers for conformance and diagnostics.
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

// Describe returns a low-sensitive runtime snapshot.
func (client *Client) Describe() Description {
	if client == nil {
		return Description{Closed: true}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return Description{
		Closed:   client.closed,
		Watchers: len(client.watchers),
	}
}

func (client *Client) toBackend(
	instance registry.Instance,
) (BackendInstance, error) {
	if err := instance.Validate(); err != nil {
		return BackendInstance{}, err
	}
	endpoint, ok := instance.Endpoint(client.scheme)
	if !ok {
		return BackendInstance{}, fmt.Errorf(
			"%w: instance %q has no %s endpoint",
			ErrInvalidOption,
			instance.ID(),
			client.scheme,
		)
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return BackendInstance{}, fmt.Errorf("%w: endpoint: %w", ErrInvalidOption, err)
	}
	port, err := strconv.ParseUint(parsed.Port(), 10, 64)
	if err != nil || port == 0 {
		return BackendInstance{}, fmt.Errorf(
			"%w: endpoint port %q",
			ErrInvalidOption,
			parsed.Port(),
		)
	}
	metadata := cloneMetadata(instance.Metadata())
	if _, exists := metadata[instanceidKey]; exists {
		return BackendInstance{}, fmt.Errorf(
			"%w: metadata uses reserved key %q",
			ErrInvalidOption,
			instanceidKey,
		)
	}
	if _, exists := metadata[endpointsKey]; exists {
		return BackendInstance{}, fmt.Errorf(
			"%w: metadata uses reserved key %q",
			ErrInvalidOption,
			endpointsKey,
		)
	}
	encodedEndpoints, err := json.Marshal(instance.Endpoints())
	if err != nil {
		return BackendInstance{}, err
	}
	metadata[instanceidKey] = instance.ID()
	metadata[endpointsKey] = string(encodedEndpoints)
	return BackendInstance{
		ID:       instance.ID(),
		IP:       parsed.Hostname(),
		Port:     port,
		Metadata: metadata,
	}, nil
}

func (client *Client) snapshot(
	service string,
	backendInstances []BackendInstance,
) (registry.Snapshot, error) {
	instances := make([]registry.Instance, 0, len(backendInstances))
	for _, backend := range backendInstances {
		metadata := cloneMetadata(backend.Metadata)
		id := strings.TrimSpace(metadata[instanceidKey])
		if id == "" {
			id = strings.TrimSpace(backend.ID)
		}
		if id == "" {
			id = net.JoinHostPort(backend.IP, strconv.FormatUint(backend.Port, 10))
		}
		var endpoints []string
		if encoded := metadata[endpointsKey]; encoded != "" {
			if err := json.Unmarshal([]byte(encoded), &endpoints); err != nil {
				return registry.Snapshot{}, fmt.Errorf(
					"nacos registry: decode endpoints: %w",
					err,
				)
			}
		}
		if len(endpoints) == 0 {
			endpoints = []string{
				client.scheme + "://" +
					net.JoinHostPort(backend.IP, strconv.FormatUint(backend.Port, 10)),
			}
		}
		delete(metadata, instanceidKey)
		delete(metadata, endpointsKey)
		instance, err := registry.NewInstance(id, service, endpoints, metadata)
		if err != nil {
			return registry.Snapshot{}, err
		}
		instances = append(instances, instance)
	}
	revision := snapshotRevision(instances)
	return registry.NewSnapshot(service, revision, instances)
}

func (client *Client) removeWatcher(current *watcher) {
	client.mu.Lock()
	defer client.mu.Unlock()
	delete(client.watchers, current)
}

type watcher struct {
	client   *Client
	service  string
	cancel   func() error
	updates  chan registry.Snapshot
	errs     chan error
	done     chan struct{}
	once     sync.Once
	closeErr error

	stateMu       sync.Mutex
	bootstrapping bool
	generation    uint64
	pending       []BackendInstance
	pendingErr    error
}

func (watcher *watcher) Next(ctx context.Context) (registry.Snapshot, error) {
	select {
	case snapshot := <-watcher.updates:
		return snapshot.Clone(), nil
	case err := <-watcher.errs:
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
		if watcher.cancel != nil {
			watcher.closeErr = watcher.cancel()
		}
		close(watcher.done)
	})
	return watcher.closeErr
}

func (watcher *watcher) closeWithoutCancel() {
	watcher.once.Do(func() {
		watcher.client.removeWatcher(watcher)
		close(watcher.done)
	})
}

func (watcher *watcher) onUpdate(
	instances []BackendInstance,
	err error,
) {
	watcher.stateMu.Lock()
	if watcher.bootstrapping {
		watcher.generation++
		watcher.pending = cloneBackendInstances(instances)
		watcher.pendingErr = err
		watcher.stateMu.Unlock()
		return
	}
	watcher.stateMu.Unlock()
	if err != nil {
		watcher.publishError(err)
		return
	}
	snapshot, err := watcher.client.snapshot(watcher.service, instances)
	if err != nil {
		watcher.publishError(err)
		return
	}
	watcher.publish(snapshot)
}

func (watcher *watcher) bootstrap(ctx context.Context) error {
	for attempt := 0; attempt < maxBootstrapLists; attempt++ {
		watcher.stateMu.Lock()
		generation := watcher.generation
		watcher.stateMu.Unlock()

		instances, listErr := watcher.client.backend.List(
			ctx,
			watcher.service,
			watcher.client.group,
			watcher.client.cluster,
		)

		watcher.stateMu.Lock()
		changed := watcher.generation != generation
		if listErr != nil {
			if watcher.generation == 0 || watcher.pendingErr != nil {
				watcher.stateMu.Unlock()
				return fmt.Errorf(
					"nacos registry: initial list: %w",
					listErr,
				)
			}
			instances = cloneBackendInstances(watcher.pending)
			changed = false
		}
		if changed && attempt+1 < maxBootstrapLists {
			watcher.stateMu.Unlock()
			continue
		}
		if changed {
			if watcher.pendingErr != nil {
				err := watcher.pendingErr
				watcher.stateMu.Unlock()
				return fmt.Errorf(
					"nacos registry: bootstrap subscription: %w",
					err,
				)
			}
			instances = cloneBackendInstances(watcher.pending)
		}
		snapshot, err := watcher.client.snapshot(
			watcher.service,
			instances,
		)
		if err != nil {
			watcher.stateMu.Unlock()
			return err
		}
		watcher.bootstrapping = false
		watcher.pending = nil
		watcher.pendingErr = nil
		// Publish while holding stateMu. A concurrent callback cannot publish
		// ahead of the initial snapshot and then be overwritten by it.
		watcher.publish(snapshot)
		watcher.stateMu.Unlock()
		return nil
	}
	return fmt.Errorf(
		"nacos registry: bootstrap did not converge",
	)
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

func (watcher *watcher) publishError(err error) {
	select {
	case <-watcher.done:
		return
	default:
	}
	select {
	case <-watcher.errs:
	default:
	}
	select {
	case watcher.errs <- err:
	default:
	}
}

func snapshotRevision(instances []registry.Instance) string {
	sorted := append([]registry.Instance(nil), instances...)
	sort.Slice(sorted, func(first, second int) bool {
		return sorted[first].ID() < sorted[second].ID()
	})
	hash := sha256.New()
	for _, instance := range sorted {
		_, _ = hash.Write([]byte(instance.ID()))
		for _, endpoint := range instance.Endpoints() {
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write([]byte(endpoint))
		}
		metadata := instance.Metadata()
		keys := make([]string, 0, len(metadata))
		for key := range metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write([]byte(key))
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write([]byte(metadata[key]))
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

type sdkBackend struct {
	client naming_client.INamingClient
}

func (backend *sdkBackend) Register(
	ctx context.Context,
	service string,
	group string,
	cluster string,
	ephemeral bool,
	instance BackendInstance,
) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	ok, err := backend.client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          instance.IP,
		Port:        instance.Port,
		Weight:      1,
		Enable:      true,
		Healthy:     true,
		Metadata:    cloneMetadata(instance.Metadata),
		ClusterName: cluster,
		ServiceName: service,
		GroupName:   group,
		Ephemeral:   ephemeral,
	})
	if err != nil {
		return err
	}
	if !ok {
		return ErrRejected
	}
	return context.Cause(ctx)
}

func (backend *sdkBackend) Deregister(
	ctx context.Context,
	service string,
	group string,
	cluster string,
	ephemeral bool,
	instance BackendInstance,
) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	ok, err := backend.client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          instance.IP,
		Port:        instance.Port,
		Cluster:     cluster,
		ServiceName: service,
		GroupName:   group,
		Ephemeral:   ephemeral,
	})
	if err != nil {
		return err
	}
	if !ok {
		return ErrRejected
	}
	return context.Cause(ctx)
}

func (backend *sdkBackend) List(
	ctx context.Context,
	service string,
	group string,
	cluster string,
) ([]BackendInstance, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	instances, err := backend.client.SelectAllInstances(vo.SelectAllInstancesParam{
		ServiceName: service,
		GroupName:   group,
		Clusters:    []string{cluster},
	})
	if err != nil {
		return nil, err
	}
	healthy := make([]model.Instance, 0, len(instances))
	for _, instance := range instances {
		if instance.Healthy && instance.Enable && instance.Weight > 0 {
			healthy = append(healthy, instance)
		}
	}
	return fromSDKInstances(healthy), context.Cause(ctx)
}

func (backend *sdkBackend) Subscribe(
	ctx context.Context,
	service string,
	group string,
	cluster string,
	callback func([]BackendInstance, error),
) (func() error, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	param := &vo.SubscribeParam{
		ServiceName: service,
		GroupName:   group,
		Clusters:    []string{cluster},
		SubscribeCallback: func(instances []model.Instance, err error) {
			callback(fromSDKInstances(instances), err)
		},
	}
	if err := backend.client.Subscribe(param); err != nil {
		return nil, err
	}
	return func() error {
		return backend.client.Unsubscribe(param)
	}, nil
}

func (backend *sdkBackend) Close() {
	backend.client.CloseClient()
}

func fromSDKInstances(instances []model.Instance) []BackendInstance {
	result := make([]BackendInstance, 0, len(instances))
	for _, instance := range instances {
		if !instance.Enable || !instance.Healthy || instance.Weight <= 0 {
			continue
		}
		result = append(result, BackendInstance{
			ID:       instance.InstanceId,
			IP:       instance.Ip,
			Port:     instance.Port,
			Metadata: cloneMetadata(instance.Metadata),
		})
	}
	return result
}

func cloneMetadata(source map[string]string) map[string]string {
	if len(source) == 0 {
		return make(map[string]string)
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func cloneBackendInstances(
	source []BackendInstance,
) []BackendInstance {
	if len(source) == 0 {
		return nil
	}
	result := make([]BackendInstance, len(source))
	for index, instance := range source {
		result[index] = BackendInstance{
			ID:       instance.ID,
			IP:       instance.IP,
			Port:     instance.Port,
			Metadata: cloneMetadata(instance.Metadata),
		}
	}
	return result
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
