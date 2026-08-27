package etcd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultPrefix         = "/keelith/registry"
	defaultLeaseTTL       = 15 * time.Second
	defaultMaxRecordBytes = 64 * 1024
	maxAllowedRecordBytes = 1024 * 1024
	recordVersion         = 1
)

var (
	// ErrInvalidOption reports an invalid backend, prefix, ttl, or record budget.
	ErrInvalidOption = errors.New("etcd registry: invalid option")
	// ErrClosed reports an operation after Client shutdown.
	ErrClosed = errors.New("etcd registry: client closed")
	// ErrWatchClosed reports an unexpected backend watch channel closure.
	ErrWatchClosed = errors.New("etcd registry: backend watch closed")
	// ErrLeaseLost reports an unexpected keepalive termination.
	ErrLeaseLost = errors.New("etcd registry: lease keepalive lost")
	// ErrInvalidRecord reports a corrupt, mismatched, or oversized stored value.
	ErrInvalidRecord = errors.New("etcd registry: invalid record")
)

// Options configure key ownership, lease lifetime, and record budgets.
type Options struct {
	Prefix         string        `config:"prefix"`
	LeaseTTL       time.Duration `config:"leasettl"`
	MaxRecordBytes int           `config:"maxRecordBytes"`
	OwnsClient     bool          `config:"ownsClient"`
}

// Description is a bounded operational snapshot.
type Description struct {
	Prefix         string
	LeaseTTL       time.Duration
	Closed         bool
	Registrations  int
	Watchers       int
	LeaseLosses    uint64
	LastError      string
	MaxRecordBytes int
}

type registration struct {
	key    string
	lease  LeaseID
	cancel context.CancelFunc
	done   chan struct{}
}

type record struct {
	Version   int               `json:"version"`
	ID        string            `json:"id"`
	Service   string            `json:"service"`
	Endpoints []string          `json:"endpoints"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Client implements registry.Registrar and registry.Discovery.
type Client struct {
	backend        Backend
	prefix         string
	leasettl       time.Duration
	maxRecordBytes int
	ownsClient     bool

	runtimeCtx    context.Context
	cancel        context.CancelFunc
	opMu          sync.Mutex
	mu            sync.Mutex
	closed        bool
	registrations map[string]*registration
	watchers      map[*watcher]struct{}
	lastError     string
	leaseLosses   atomic.Uint64

	closeOnce sync.Once
	closeErr  error
}

// New constructs a Client around an official etcd v3 client.
func New(client *clientv3.Client, options Options) (*Client, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	return Wrap(&sdkBackend{client: client}, options)
}

// Wrap constructs a Client around a custom Backend.
func Wrap(backend Backend, options Options) (*Client, error) {
	if isNilBackend(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	if err := ValidateOptions(options); err != nil {
		return nil, err
	}
	prefix := strings.TrimSpace(options.Prefix)
	if prefix == "" {
		prefix = defaultPrefix
	}
	prefix = strings.TrimSuffix(prefix, "/")
	if !validPrefix(prefix) {
		return nil, fmt.Errorf("%w: prefix %q", ErrInvalidOption, prefix)
	}
	leasettl := options.LeaseTTL
	if leasettl == 0 {
		leasettl = defaultLeaseTTL
	}
	maxRecordBytes := options.MaxRecordBytes
	if maxRecordBytes == 0 {
		maxRecordBytes = defaultMaxRecordBytes
	}
	if leasettl < time.Second ||
		maxRecordBytes <= 0 ||
		maxRecordBytes > maxAllowedRecordBytes {
		return nil, fmt.Errorf(
			"%w: lease ttl or record budget is invalid",
			ErrInvalidOption,
		)
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	return &Client{
		backend:        backend,
		prefix:         prefix,
		leasettl:       leasettl,
		maxRecordBytes: maxRecordBytes,
		ownsClient:     options.OwnsClient,
		runtimeCtx:     runtimeCtx,
		cancel:         cancel,
		registrations:  make(map[string]*registration),
		watchers:       make(map[*watcher]struct{}),
	}, nil
}

// ValidateOptions validates registry namespace, lease, and record budgets.
func ValidateOptions(options Options) error {
	prefix := strings.TrimSpace(options.Prefix)
	if prefix == "" {
		prefix = defaultPrefix
	}
	prefix = strings.TrimSuffix(prefix, "/")
	if !validPrefix(prefix) {
		return fmt.Errorf("%w: prefix %q", ErrInvalidOption, prefix)
	}
	leasettl := options.LeaseTTL
	if leasettl == 0 {
		leasettl = defaultLeaseTTL
	}
	maxRecordBytes := options.MaxRecordBytes
	if maxRecordBytes == 0 {
		maxRecordBytes = defaultMaxRecordBytes
	}
	if leasettl < time.Second ||
		maxRecordBytes <= 0 ||
		maxRecordBytes > maxAllowedRecordBytes {
		return fmt.Errorf(
			"%w: lease ttl or record budget is invalid",
			ErrInvalidOption,
		)
	}
	return nil
}

// Register publishes or atomically replaces one lease-backed instance.
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
	value, err := client.encode(instance)
	if err != nil {
		return err
	}
	key := client.instanceKey(instance.Service(), instance.ID())

	client.opMu.Lock()
	defer client.opMu.Unlock()
	if err := client.requireOpen(); err != nil {
		return err
	}
	lease, err := client.backend.Grant(ctx, client.leasettl)
	if err != nil {
		return fmt.Errorf("etcd registry: grant lease: %w", err)
	}
	keepaliveCtx, cancelKeepalive := context.WithCancel(client.runtimeCtx)
	keepalive, err := client.backend.KeepAlive(keepaliveCtx, lease)
	if err != nil || keepalive == nil {
		cancelKeepalive()
		_ = client.backend.Revoke(ctx, lease)
		if err == nil {
			err = ErrLeaseLost
		}
		return fmt.Errorf("etcd registry: start keepalive: %w", err)
	}
	if _, err := client.backend.Put(ctx, key, value, lease); err != nil {
		cancelKeepalive()
		_ = client.backend.Revoke(ctx, lease)
		return fmt.Errorf("etcd registry: put instance: %w", err)
	}

	current := &registration{
		key:    key,
		lease:  lease,
		cancel: cancelKeepalive,
		done:   make(chan struct{}),
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		cancelKeepalive()
		_ = client.backend.Revoke(ctx, lease)
		return ErrClosed
	}
	previous := client.registrations[key]
	client.registrations[key] = current
	client.mu.Unlock()
	go client.monitorLease(keepaliveCtx, current, keepalive)

	if previous != nil {
		if stopErr := client.stopRegistration(ctx, previous, true); stopErr != nil {
			client.recordError(stopErr)
		}
	}
	return nil
}

// Deregister removes one key before revoking its local lease.
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
	key := client.instanceKey(instance.Service(), instance.ID())

	client.opMu.Lock()
	defer client.opMu.Unlock()
	if err := client.requireOpen(); err != nil {
		return err
	}
	if _, err := client.backend.Delete(ctx, key); err != nil {
		return fmt.Errorf("etcd registry: delete instance: %w", err)
	}
	client.mu.Lock()
	current := client.registrations[key]
	if current != nil {
		delete(client.registrations, key)
	}
	client.mu.Unlock()
	if current == nil {
		return nil
	}
	if err := client.stopRegistration(ctx, current, true); err != nil {
		client.recordError(err)
		return fmt.Errorf("etcd registry: revoke instance lease: %w", err)
	}
	return nil
}

// Watch returns a full-snapshot watcher with no List/Watch revision gap.
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
	if _, err := registry.NewSnapshot(service, "etcd:0", nil); err != nil {
		return nil, err
	}

	client.opMu.Lock()
	defer client.opMu.Unlock()
	if err := client.requireOpen(); err != nil {
		return nil, err
	}
	prefix := client.servicePrefix(service)
	entries, revision, err := client.backend.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("etcd registry: initial list: %w", err)
	}
	state, snapshot, err := client.snapshotFromEntries(
		service,
		revision,
		entries,
	)
	if err != nil {
		return nil, err
	}
	watchCtx, cancel := context.WithCancel(ctx)
	batches := client.backend.Watch(watchCtx, prefix, revision+1)
	if batches == nil {
		cancel()
		return nil, fmt.Errorf("%w: backend returned nil watch", ErrInvalidOption)
	}
	current := &watcher{
		client:   client,
		service:  service,
		prefix:   prefix,
		context:  watchCtx,
		cancel:   cancel,
		batches:  batches,
		state:    state,
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

// Shutdown closes watchers, keepalives, leases, and optionally the SDK client.
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
		registrations := make([]*registration, 0, len(client.registrations))
		for _, current := range client.registrations {
			registrations = append(registrations, current)
		}
		client.registrations = make(map[string]*registration)
		client.mu.Unlock()
		client.cancel()
		for _, current := range watchers {
			client.closeErr = errors.Join(client.closeErr, current.Close())
		}
		for _, current := range registrations {
			client.closeErr = errors.Join(
				client.closeErr,
				client.stopRegistration(ctx, current, true),
			)
		}
		if cause := context.Cause(ctx); cause != nil {
			client.closeErr = errors.Join(client.closeErr, cause)
		}
		if client.ownsClient {
			client.closeErr = errors.Join(client.closeErr, client.backend.Close())
		}
	})
	return client.closeErr
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

// Describe returns bounded lease, watcher, and lifecycle diagnostics.
func (client *Client) Describe() Description {
	if client == nil {
		return Description{Closed: true, LastError: "client is nil"}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return Description{
		Prefix:         client.prefix,
		LeaseTTL:       client.leasettl,
		Closed:         client.closed,
		Registrations:  len(client.registrations),
		Watchers:       len(client.watchers),
		LeaseLosses:    client.leaseLosses.Load(),
		LastError:      client.lastError,
		MaxRecordBytes: client.maxRecordBytes,
	}
}

func (client *Client) monitorLease(
	ctx context.Context,
	current *registration,
	keepalive <-chan error,
) {
	defer close(current.done)
	defer current.cancel()
	var terminal error
	select {
	case err, ok := <-keepalive:
		if context.Cause(ctx) != nil {
			return
		}
		if !ok || err == nil {
			terminal = ErrLeaseLost
		} else {
			terminal = errors.Join(ErrLeaseLost, err)
		}
	case <-ctx.Done():
		return
	}
	client.leaseLosses.Add(1)
	client.recordError(terminal)
	client.mu.Lock()
	if client.registrations[current.key] == current {
		delete(client.registrations, current.key)
	}
	client.mu.Unlock()
}

func (client *Client) stopRegistration(
	ctx context.Context,
	current *registration,
	revoke bool,
) error {
	current.cancel()
	var result error
	select {
	case <-current.done:
	case <-ctx.Done():
		result = context.Cause(ctx)
	}
	if revoke {
		result = errors.Join(result, client.backend.Revoke(ctx, current.lease))
	}
	return result
}

func (client *Client) requireOpen() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return ErrClosed
	}
	return nil
}

func (client *Client) encode(instance registry.Instance) ([]byte, error) {
	value, err := json.Marshal(record{
		Version:   recordVersion,
		ID:        instance.ID(),
		Service:   instance.Service(),
		Endpoints: instance.Endpoints(),
		Metadata:  instance.Metadata(),
	})
	if err != nil {
		return nil, fmt.Errorf("%w: encode: %w", ErrInvalidRecord, err)
	}
	if len(value) > client.maxRecordBytes {
		return nil, fmt.Errorf(
			"%w: %d bytes exceeds %d",
			ErrInvalidRecord,
			len(value),
			client.maxRecordBytes,
		)
	}
	return value, nil
}

func (client *Client) decode(
	key string,
	value []byte,
	service string,
) (registry.Instance, error) {
	if len(value) == 0 || len(value) > client.maxRecordBytes {
		return registry.Instance{}, fmt.Errorf(
			"%w: record size %d",
			ErrInvalidRecord,
			len(value),
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded record
	if err := decoder.Decode(&decoded); err != nil {
		return registry.Instance{}, fmt.Errorf(
			"%w: decode %q: %w",
			ErrInvalidRecord,
			key,
			err,
		)
	}
	if decoded.Version != recordVersion || decoded.Service != service {
		return registry.Instance{}, fmt.Errorf(
			"%w: record %q version or service mismatch",
			ErrInvalidRecord,
			key,
		)
	}
	instance, err := registry.NewInstance(
		decoded.ID,
		decoded.Service,
		decoded.Endpoints,
		decoded.Metadata,
	)
	if err != nil {
		return registry.Instance{}, fmt.Errorf(
			"%w: record %q: %w",
			ErrInvalidRecord,
			key,
			err,
		)
	}
	if key != client.instanceKey(service, instance.ID()) {
		return registry.Instance{}, fmt.Errorf(
			"%w: record %q key mismatch",
			ErrInvalidRecord,
			key,
		)
	}
	return instance, nil
}

func (client *Client) snapshotFromEntries(
	service string,
	revision int64,
	entries []Entry,
) (map[string]registry.Instance, registry.Snapshot, error) {
	state := make(map[string]registry.Instance, len(entries))
	for _, entry := range entries {
		if _, duplicate := state[entry.Key]; duplicate {
			return nil, registry.Snapshot{}, fmt.Errorf(
				"%w: duplicate key %q",
				ErrInvalidRecord,
				entry.Key,
			)
		}
		instance, err := client.decode(entry.Key, entry.Value, service)
		if err != nil {
			return nil, registry.Snapshot{}, err
		}
		state[entry.Key] = instance
	}
	snapshot, err := snapshotFromState(service, revision, state)
	if err != nil {
		return nil, registry.Snapshot{}, err
	}
	return state, snapshot, nil
}

func (client *Client) instanceKey(service, id string) string {
	return client.servicePrefix(service) + encodeSegment(id)
}

func (client *Client) servicePrefix(service string) string {
	return client.prefix + "/" + encodeSegment(service) + "/"
}

func (client *Client) removeWatcher(current *watcher) {
	client.mu.Lock()
	delete(client.watchers, current)
	client.mu.Unlock()
}

func (client *Client) recordError(err error) {
	if err == nil {
		return
	}
	message := sanitizeError(err.Error(), 512)
	client.mu.Lock()
	client.lastError = message
	client.mu.Unlock()
}

func encodeSegment(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func validPrefix(prefix string) bool {
	if prefix == "" ||
		prefix == "/" ||
		!strings.HasPrefix(prefix, "/") ||
		len(prefix) > 256 ||
		!utf8.ValidString(prefix) {
		return false
	}
	for _, character := range prefix {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func sanitizeError(value string, limit int) string {
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, value)
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func isNilBackend(backend Backend) bool {
	if backend == nil {
		return true
	}
	// Backend implementations are expected to be pointers. A small type switch
	// is not sufficient because Wrap is also a test seam for custom providers.
	value := reflectValue(backend)
	return value
}
