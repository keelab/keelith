package consul

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keelab/keelith/registry"
)

const (
	defaultScheme           = "grpc"
	defaultttl              = 15 * time.Second
	defaultBlockingWait     = 5 * time.Minute
	defaultMaxResponseBytes = 4 * 1024 * 1024
	maxResponseBytes        = 32 * 1024 * 1024
	endpointsMetaKey        = "keelith.endpoints"
	maxMetaEntries          = 64
	maxMetaKeyBytes         = 128
	maxMetaValueBytes       = 512
)

var (
	// ErrInvalidOption reports malformed endpoint, namespace, ttl, or budget.
	ErrInvalidOption = errors.New("consul registry: invalid option")
	// ErrClosed reports an operation after Client shutdown.
	ErrClosed = errors.New("consul registry: client closed")
	// ErrInvalidRecord reports malformed consul service data.
	ErrInvalidRecord = errors.New("consul registry: invalid record")
	// ErrWatchClosed reports an unexpected blocking-query termination.
	ErrWatchClosed = errors.New("consul registry: watch closed")
)

// Options configure consul http access and endpoint representation.
type Options struct {
	Address          string        `config:"address"`
	Datacenter       string        `config:"datacenter"`
	Scheme           string        `config:"scheme"`
	TTL              time.Duration `config:"ttl"`
	BlockingWait     time.Duration `config:"blockingWait"`
	MaxResponseBytes int64         `config:"maxResponseBytes"`
	OwnsClient       bool          `config:"ownsClient"`
}

// Description is a bounded operational snapshot.
type Description struct {
	Address       string
	Datacenter    string
	Scheme        string
	Closed        bool
	Registrations int
	Watchers      int
	HeartbeatFail uint64
	LastError     string
}

type registration struct {
	instance registry.Instance
	cancel   context.CancelFunc
	done     chan struct{}
}

// Client implements registry.Registrar and registry.Discovery.
type Client struct {
	backend      Backend
	address      string
	datacenter   string
	scheme       string
	TTL          time.Duration
	blockingWait time.Duration
	ownsClient   bool

	runtimeCtx context.Context
	cancel     context.CancelFunc
	opMu       sync.Mutex
	mu         sync.Mutex
	closed     bool
	registered map[string]*registration
	watchers   map[*watcher]struct{}
	lastError  string
	heartbeats atomic.Uint64

	closeOnce sync.Once
	closeErr  error
}

// Wrap constructs a Client around a custom Backend.
func Wrap(backend Backend, options Options) (*Client, error) {
	if isNilBackend(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	return &Client{
		backend:      backend,
		address:      normalized.Address,
		datacenter:   normalized.Datacenter,
		scheme:       normalized.Scheme,
		TTL:          normalized.TTL,
		blockingWait: normalized.BlockingWait,
		ownsClient:   normalized.OwnsClient,
		runtimeCtx:   runtimeCtx,
		cancel:       cancel,
		registered:   make(map[string]*registration),
		watchers:     make(map[*watcher]struct{}),
	}, nil
}

// ValidateOptions validates consul addressing, namespace, ttl, and budgets.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

// NormalizeOptions validates options and returns all effective defaults.
//
// Callers that derive an http timeout from BlockingWait should use this
// function before constructing the http client.
func NormalizeOptions(options Options) (Options, error) {
	address := strings.TrimSpace(options.Address)
	if address == "" {
		address = "http://127.0.0.1:8500"
	}
	parsed, err := url.Parse(address)
	if err != nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" ||
		parsed.User != nil ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" ||
		parsed.Path != "" && parsed.Path != "/" {
		return Options{}, fmt.Errorf(
			"%w: unsafe consul address %q",
			ErrInvalidOption,
			address,
		)
	}
	parsed.Path = ""
	address = strings.TrimSuffix(parsed.String(), "/")
	datacenter := strings.TrimSpace(options.Datacenter)
	if strings.ContainsAny(datacenter, "\r\n\x00") {
		return Options{}, fmt.Errorf(
			"%w: datacenter contains control bytes",
			ErrInvalidOption,
		)
	}
	scheme := strings.ToLower(strings.TrimSpace(options.Scheme))
	if scheme == "" {
		scheme = defaultScheme
	}
	if _, err := url.Parse(scheme + "://127.0.0.1:1"); err != nil ||
		strings.ContainsAny(scheme, "/:") {
		return Options{}, fmt.Errorf(
			"%w: endpoint scheme %q",
			ErrInvalidOption,
			scheme,
		)
	}
	ttl := options.TTL
	if ttl == 0 {
		ttl = defaultttl
	}
	wait := options.BlockingWait
	if wait == 0 {
		wait = defaultBlockingWait
	}
	maxBytes := options.MaxResponseBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxResponseBytes
	}
	if ttl < 2*time.Second ||
		ttl > 24*time.Hour ||
		wait < time.Second ||
		wait > 10*time.Minute ||
		maxBytes < 1024 ||
		maxBytes > maxResponseBytes {
		return Options{}, fmt.Errorf(
			"%w: ttl, blocking wait, or response budget is invalid",
			ErrInvalidOption,
		)
	}
	options.Address = address
	options.Datacenter = datacenter
	options.Scheme = scheme
	options.TTL = ttl
	options.BlockingWait = wait
	options.MaxResponseBytes = maxBytes
	return options, nil
}

func normalizeOptions(options Options) (Options, error) {
	return NormalizeOptions(options)
}

// Register publishes one ttl-backed instance and starts its heartbeat.
func (client *Client) Register(
	ctx context.Context,
	instance registry.Instance,
) error {
	if client == nil || ctx == nil {
		return fmt.Errorf("%w: client or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := instance.Validate(); err != nil {
		return err
	}
	backendRecord, err := client.toRegistration(instance)
	if err != nil {
		return err
	}
	client.opMu.Lock()
	defer client.opMu.Unlock()
	if err := client.requireOpen(); err != nil {
		return err
	}
	client.mu.Lock()
	previous := client.registered[instance.ID()]
	client.mu.Unlock()
	if err := client.backend.Register(ctx, backendRecord); err != nil {
		return fmt.Errorf("consul registry: register: %w", err)
	}
	if err := client.backend.Pass(ctx, instance.ID()); err != nil {
		_ = client.backend.Deregister(ctx, instance.ID())
		if previous != nil {
			previous.cancel()
			_ = waitDone(ctx, previous.done)
			client.mu.Lock()
			if client.registered[instance.ID()] == previous {
				delete(client.registered, instance.ID())
			}
			client.mu.Unlock()
		}
		return fmt.Errorf("consul registry: initial ttl pass: %w", err)
	}
	heartbeatCtx, cancel := context.WithCancel(client.runtimeCtx)
	current := &registration{
		instance: instance.Clone(),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		cancel()
		_ = client.backend.Deregister(ctx, instance.ID())
		return ErrClosed
	}
	client.registered[instance.ID()] = current
	client.mu.Unlock()
	go client.heartbeat(heartbeatCtx, current)
	if previous != nil {
		previous.cancel()
		if err := waitDone(ctx, previous.done); err != nil {
			return err
		}
	}
	return nil
}

// Deregister removes one instance and stops its heartbeat.
func (client *Client) Deregister(
	ctx context.Context,
	instance registry.Instance,
) error {
	if client == nil || ctx == nil {
		return fmt.Errorf("%w: client or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := instance.Validate(); err != nil {
		return err
	}
	client.opMu.Lock()
	defer client.opMu.Unlock()
	if err := client.requireOpen(); err != nil {
		return err
	}
	if err := client.backend.Deregister(ctx, instance.ID()); err != nil {
		return fmt.Errorf("consul registry: deregister: %w", err)
	}
	client.mu.Lock()
	current := client.registered[instance.ID()]
	delete(client.registered, instance.ID())
	client.mu.Unlock()
	if current != nil {
		current.cancel()
		if err := waitDone(ctx, current.done); err != nil {
			return err
		}
	}
	return nil
}

// Watch returns a full-snapshot consul blocking-query watcher.
func (client *Client) Watch(
	ctx context.Context,
	service string,
) (registry.Watcher, error) {
	if client == nil || ctx == nil {
		return nil, fmt.Errorf("%w: client or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if _, err := registry.NewSnapshot(service, "consul:0", nil); err != nil {
		return nil, err
	}
	client.opMu.Lock()
	defer client.opMu.Unlock()
	if err := client.requireOpen(); err != nil {
		return nil, err
	}
	records, revision, err := client.backend.List(
		ctx,
		service,
		client.datacenter,
		"",
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("consul registry: initial health query: %w", err)
	}
	snapshot, err := client.snapshot(service, revision, records)
	if err != nil {
		return nil, err
	}
	watchCtx, cancel := context.WithCancel(ctx)
	current := &watcher{
		client:   client,
		service:  service,
		context:  watchCtx,
		cancel:   cancel,
		revision: revision,
		updates:  make(chan registry.Snapshot, 1),
		done:     make(chan struct{}),
		terminal: registry.ErrWatcherClosed,
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	client.watchers[current] = struct{}{}
	client.mu.Unlock()
	current.publish(snapshot)
	go current.run()
	return current, nil
}

// Shutdown stops blocking queries and heartbeats, deregisters owned records,
// and optionally closes the backend.
func (client *Client) Shutdown(ctx context.Context) error {
	if client == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	client.closeOnce.Do(func() {
		client.opMu.Lock()
		defer client.opMu.Unlock()
		client.mu.Lock()
		client.closed = true
		watchers := make([]*watcher, 0, len(client.watchers))
		for current := range client.watchers {
			watchers = append(watchers, current)
		}
		registrations := make([]*registration, 0, len(client.registered))
		for _, current := range client.registered {
			registrations = append(registrations, current)
		}
		client.registered = make(map[string]*registration)
		client.mu.Unlock()
		client.cancel()
		for _, current := range watchers {
			client.closeErr = errors.Join(client.closeErr, current.Close())
		}
		for _, current := range registrations {
			current.cancel()
			if err := waitDone(ctx, current.done); err != nil {
				client.closeErr = errors.Join(client.closeErr, err)
				continue
			}
			client.closeErr = errors.Join(
				client.closeErr,
				client.backend.Deregister(ctx, current.instance.ID()),
			)
		}
		if cause := context.Cause(ctx); cause != nil {
			client.closeErr = errors.Join(client.closeErr, cause)
		}
		if client.ownsClient {
			client.closeErr = errors.Join(
				client.closeErr,
				client.backend.Close(),
			)
		}
	})
	return client.closeErr
}

// Describe returns lifecycle and bounded failure diagnostics.
func (client *Client) Describe() Description {
	if client == nil {
		return Description{Closed: true, LastError: "client is nil"}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return Description{
		Address:       client.address,
		Datacenter:    client.datacenter,
		Scheme:        client.scheme,
		Closed:        client.closed,
		Registrations: len(client.registered),
		Watchers:      len(client.watchers),
		HeartbeatFail: client.heartbeats.Load(),
		LastError:     client.lastError,
	}
}

// WatcherCount reports currently open watchers for one service.
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

func (client *Client) toRegistration(
	instance registry.Instance,
) (Registration, error) {
	endpoint, ok := instance.Endpoint(client.scheme)
	if !ok {
		return Registration{}, fmt.Errorf(
			"%w: instance %q has no %s endpoint",
			ErrInvalidOption,
			instance.ID(),
			client.scheme,
		)
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" || parsed.Port() == "" {
		return Registration{}, fmt.Errorf(
			"%w: endpoint %q has no host or port",
			ErrInvalidOption,
			endpoint,
		)
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || port < 1 || port > 65535 {
		return Registration{}, fmt.Errorf(
			"%w: endpoint port %q",
			ErrInvalidOption,
			parsed.Port(),
		)
	}
	meta := cloneMetadata(instance.Metadata())
	if len(meta) >= maxMetaEntries {
		return Registration{}, fmt.Errorf(
			"%w: metadata exceeds consul entry budget",
			ErrInvalidOption,
		)
	}
	if _, exists := meta[endpointsMetaKey]; exists {
		return Registration{}, fmt.Errorf(
			"%w: metadata uses reserved key %q",
			ErrInvalidOption,
			endpointsMetaKey,
		)
	}
	payload, err := json.Marshal(instance.Endpoints())
	if err != nil {
		return Registration{}, err
	}
	meta[endpointsMetaKey] = base64.RawURLEncoding.EncodeToString(payload)
	for key, value := range meta {
		if len(key) == 0 ||
			len(key) > maxMetaKeyBytes ||
			len(value) > maxMetaValueBytes ||
			strings.ContainsAny(key, "\r\n\x00") ||
			strings.ContainsAny(value, "\r\n\x00") {
			return Registration{}, fmt.Errorf(
				"%w: metadata exceeds consul key/value budget",
				ErrInvalidOption,
			)
		}
	}
	return Registration{
		ID:      instance.ID(),
		Service: instance.Service(),
		Address: parsed.Hostname(),
		Port:    port,
		Meta:    meta,
		TTL:     client.TTL,
	}, nil
}

func (client *Client) snapshot(
	service string,
	revision string,
	records []BackendInstance,
) (registry.Snapshot, error) {
	if strings.TrimSpace(revision) == "" {
		return registry.Snapshot{}, fmt.Errorf(
			"%w: empty consul index",
			ErrInvalidRecord,
		)
	}
	instances := make([]registry.Instance, 0, len(records))
	for _, record := range records {
		if record.Service != service ||
			record.ID == "" ||
			record.Address == "" ||
			record.Port < 1 ||
			record.Port > 65535 {
			return registry.Snapshot{}, fmt.Errorf(
				"%w: invalid service identity",
				ErrInvalidRecord,
			)
		}
		meta := cloneMetadata(record.Meta)
		encoded, ok := meta[endpointsMetaKey]
		if !ok {
			return registry.Snapshot{}, fmt.Errorf(
				"%w: instance %q lacks endpoint metadata",
				ErrInvalidRecord,
				record.ID,
			)
		}
		delete(meta, endpointsMetaKey)
		payload, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			return registry.Snapshot{}, fmt.Errorf(
				"%w: decode endpoints",
				ErrInvalidRecord,
			)
		}
		var endpoints []string
		if err := json.Unmarshal(payload, &endpoints); err != nil {
			return registry.Snapshot{}, fmt.Errorf(
				"%w: decode endpoints json",
				ErrInvalidRecord,
			)
		}
		instance, err := registry.NewInstance(
			record.ID,
			service,
			endpoints,
			meta,
		)
		if err != nil {
			return registry.Snapshot{}, fmt.Errorf(
				"%w: %w",
				ErrInvalidRecord,
				err,
			)
		}
		instances = append(instances, instance)
	}
	return registry.NewSnapshot(
		service,
		"consul:"+revision,
		instances,
	)
}

func (client *Client) heartbeat(
	ctx context.Context,
	current *registration,
) {
	defer close(current.done)
	ticker := time.NewTicker(client.TTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			passCtx, cancel := context.WithTimeout(
				ctx,
				min(client.TTL/3, 5*time.Second),
			)
			err := client.backend.Pass(passCtx, current.instance.ID())
			cancel()
			if err != nil {
				client.heartbeats.Add(1)
				client.recordError(fmt.Errorf(
					"ttl heartbeat %q: %w",
					current.instance.ID(),
					err,
				))
			}
		}
	}
}

func (client *Client) requireOpen() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return ErrClosed
	}
	return nil
}

func (client *Client) recordError(err error) {
	if err == nil {
		return
	}
	client.mu.Lock()
	client.lastError = err.Error()
	client.mu.Unlock()
}

func (client *Client) removeWatcher(current *watcher) {
	client.mu.Lock()
	delete(client.watchers, current)
	client.mu.Unlock()
}

func cloneMetadata(source map[string]string) map[string]string {
	if len(source) == 0 {
		return make(map[string]string)
	}
	result := make(map[string]string, len(source)+1)
	for key, value := range source {
		result[key] = value
	}
	return result
}

func isNilBackend(backend Backend) bool {
	if backend == nil {
		return true
	}
	value := reflect.ValueOf(backend)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func waitDone(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
