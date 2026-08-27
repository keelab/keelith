package etcd

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/registry"
	"github.com/keelab/keelith/secret"
	"github.com/keelab/keelith/transport/tlsconfig"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultManagedDialTimeout   = 5 * time.Second
	defaultManagedReadyTimeout  = 10 * time.Second
	minimumManagedReadyTimeout  = 100 * time.Millisecond
	maximumManagedReadyTimeout  = 5 * time.Minute
	maximumManagedEndpoints     = 16
	maximumManagedPasswordBytes = 64 * 1024
	managedRotationRetryInitial = 250 * time.Millisecond
	managedRotationRetryMaximum = 5 * time.Second
)

var (
	// ErrRuntimeNotStarted reports discovery use before App Component start.
	ErrRuntimeNotStarted = errors.New("etcd registry: managed runtime not started")
	// ErrConnectionUnavailable reports a candidate that failed its Status gate.
	ErrConnectionUnavailable = errors.New("etcd registry: managed connection unavailable")
)

// ManagedTLSConfig controls private trust and optional client identity.
type ManagedTLSConfig struct {
	BundleReference string `config:"bundle_reference"`
	ServerName      string `config:"server_name"`
	MutualTLS       bool   `config:"mutual_tls"`
}

// ManagedConfig defines a read-only, owned etcd registry connection.
// Registration lease settings are deliberately absent.
type ManagedConfig struct {
	Endpoints            []string         `config:"endpoints"`
	Prefix               string           `config:"prefix"`
	DialTimeout          time.Duration    `config:"dial_timeout"`
	Username             string           `config:"username"`
	PasswordReference    string           `config:"password_reference"`
	AllowInsecure        bool             `config:"allow_insecure"`
	MaxRecordBytes       int              `config:"max_record_bytes"`
	RotationReadyTimeout time.Duration    `config:"rotation_ready_timeout"`
	TLS                  ManagedTLSConfig `config:"tls"`
}

// ValidateManagedConfig validates transport, credentials, namespace, and
// candidate readiness budgets without resolving secret material.
func ValidateManagedConfig(input ManagedConfig) error {
	config := normalizeManagedConfig(input)
	if len(config.Endpoints) < 1 || len(config.Endpoints) > maximumManagedEndpoints {
		return fmt.Errorf("%w: endpoint count is outside 1..%d", ErrInvalidOption, maximumManagedEndpoints)
	}
	scheme := ""
	seen := make(map[string]struct{}, len(config.Endpoints))
	for _, endpoint := range config.Endpoints {
		parsed, err := url.ParseRequestURI(endpoint)
		if err != nil || parsed.Host == "" ||
			(parsed.Scheme != "http" && parsed.Scheme != "https") ||
			(parsed.Path != "" && parsed.Path != "/") ||
			parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
			return fmt.Errorf("%w: managed endpoint is invalid", ErrInvalidOption)
		}
		if scheme == "" {
			scheme = parsed.Scheme
		} else if scheme != parsed.Scheme {
			return fmt.Errorf("%w: http and https endpoints cannot be mixed", ErrInvalidOption)
		}
		if _, duplicate := seen[endpoint]; duplicate {
			return fmt.Errorf("%w: duplicate managed endpoint", ErrInvalidOption)
		}
		seen[endpoint] = struct{}{}
	}
	if (scheme == "http") != config.AllowInsecure {
		return fmt.Errorf("%w: plaintext endpoints require explicit allow_insecure", ErrInvalidOption)
	}
	if config.Username == "" != (config.PasswordReference == "") {
		return fmt.Errorf("%w: username and password reference must be configured together", ErrInvalidOption)
	}
	if config.PasswordReference != "" {
		if config.AllowInsecure {
			return fmt.Errorf("%w: credentials require https", ErrInvalidOption)
		}
		if _, err := secret.Parse(config.PasswordReference); err != nil {
			return fmt.Errorf("%w: password reference: %w", ErrInvalidOption, err)
		}
	}
	if strings.TrimSpace(config.Username) != config.Username ||
		strings.IndexFunc(config.Username, unicode.IsControl) >= 0 ||
		strings.TrimSpace(config.TLS.ServerName) != config.TLS.ServerName ||
		strings.IndexFunc(config.TLS.ServerName, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: username or tls server name is invalid", ErrInvalidOption)
	}
	if config.TLS.BundleReference != "" {
		if scheme != "https" || config.TLS.ServerName == "" {
			return fmt.Errorf("%w: tls bundle requires https and server_name", ErrInvalidOption)
		}
		if _, err := secret.Parse(config.TLS.BundleReference); err != nil {
			return fmt.Errorf("%w: tls bundle reference: %w", ErrInvalidOption, err)
		}
	}
	if config.TLS.MutualTLS && config.TLS.BundleReference == "" {
		return fmt.Errorf("%w: mtls requires a tls bundle", ErrInvalidOption)
	}
	if scheme != "https" && (config.TLS.BundleReference != "" ||
		config.TLS.ServerName != "" || config.TLS.MutualTLS) {
		return fmt.Errorf("%w: plaintext runtime contains tls settings", ErrInvalidOption)
	}
	if err := ValidateOptions(Options{
		Prefix:         config.Prefix,
		MaxRecordBytes: config.MaxRecordBytes,
	}); err != nil {
		return err
	}
	return nil
}

// ManagedRuntime owns a replaceable etcd SDK client and exposes a stable,
// read-only registry.Discovery facade. TLS updates swap only after a real
// endpoint Status succeeds; old watchers then reconnect through the facade.
type ManagedRuntime struct {
	config     ManagedConfig
	manager    tlsconfig.SecretManager
	reloader   *tlsconfig.Reloader
	tlsWatcher *tlsconfig.SecretWatcher

	mu           sync.RWMutex
	client       *Client
	started      bool
	closed       bool
	rotating     bool
	degraded     bool
	subscription secret.UpdateSubscription
	cancel       context.CancelFunc
	done         chan struct{}
	loopStarted  bool
	rotations    uint64
	failures     uint64

	closeOnce sync.Once
	closeErr  error
}

// ManagedDescription is a bounded, material-free lifecycle snapshot.
type ManagedDescription struct {
	Started          bool
	Closed           bool
	Rotating         bool
	Degraded         bool
	RotationEnabled  bool
	Watchers         int
	Rotations        uint64
	RotationFailures uint64
}

// OpenManaged constructs an unstarted runtime without network traffic.
func OpenManaged(
	config ManagedConfig,
	manager tlsconfig.SecretManager,
) (*ManagedRuntime, error) {
	config = normalizeManagedConfig(config)
	if err := ValidateManagedConfig(config); err != nil {
		return nil, err
	}
	needsManager := config.PasswordReference != "" || config.TLS.BundleReference != ""
	if needsManager && isNilManagedSecretManager(manager) {
		return nil, fmt.Errorf("%w: secret-backed runtime requires a manager", ErrInvalidOption)
	}
	runtime := &ManagedRuntime{config: config, manager: manager, done: make(chan struct{})}
	if config.TLS.BundleReference != "" {
		reference, _ := secret.Parse(config.TLS.BundleReference)
		runtime.reloader = tlsconfig.NewEmpty()
		watcher, err := tlsconfig.NewSecretWatcher(manager, reference, runtime.reloader)
		if err != nil {
			return nil, err
		}
		runtime.tlsWatcher = watcher
	}
	return runtime, nil
}

// Start resolves initial credentials, proves one etcd endpoint, and begins
// observing valid tls replacements.
func (runtime *ManagedRuntime) Start(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return fmt.Errorf("%w: runtime or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	runtime.mu.Lock()
	if runtime.closed || runtime.started {
		runtime.mu.Unlock()
		return fmt.Errorf("%w: runtime is closed or already started", ErrInvalidOption)
	}
	runtime.mu.Unlock()
	if runtime.tlsWatcher != nil {
		if err := runtime.tlsWatcher.Start(ctx); err != nil {
			return err
		}
	}
	client, err := runtime.candidate(ctx)
	if err != nil {
		if runtime.tlsWatcher != nil {
			_ = runtime.tlsWatcher.Shutdown(context.WithoutCancel(ctx))
		}
		return err
	}
	var subscription secret.UpdateSubscription
	if runtime.reloader != nil {
		subscription, err = runtime.reloader.SubscribeUpdates()
		if err != nil {
			_ = client.Shutdown(context.WithoutCancel(ctx))
			_ = runtime.tlsWatcher.Shutdown(context.WithoutCancel(ctx))
			return err
		}
	}
	runContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	runtime.mu.Lock()
	runtime.client = client
	runtime.subscription = subscription
	runtime.cancel = cancel
	runtime.started = true
	runtime.loopStarted = subscription != nil
	runtime.mu.Unlock()
	if subscription != nil {
		go runtime.run(runContext, subscription)
	}
	return nil
}

// Watch delegates to the active generation after Component start.
func (runtime *ManagedRuntime) Watch(ctx context.Context, service string) (registry.Watcher, error) {
	if runtime == nil || ctx == nil {
		return nil, fmt.Errorf("%w: runtime or context is nil", ErrInvalidOption)
	}
	runtime.mu.RLock()
	if runtime.closed {
		runtime.mu.RUnlock()
		return nil, ErrClosed
	}
	if !runtime.started || runtime.client == nil {
		runtime.mu.RUnlock()
		return nil, ErrRuntimeNotStarted
	}
	client := runtime.client
	runtime.mu.RUnlock()
	return client.Watch(ctx, service)
}

// Shutdown stops rotation, closes watchers/client, then stops TLS watching.
func (runtime *ManagedRuntime) Shutdown(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	runtime.closeOnce.Do(func() { runtime.closeErr = runtime.shutdown(ctx) })
	return runtime.closeErr
}

// Describe omits endpoints, prefix, identities, references, revisions, and errors.
func (runtime *ManagedRuntime) Describe() ManagedDescription {
	if runtime == nil {
		return ManagedDescription{Closed: true}
	}
	runtime.mu.RLock()
	description := ManagedDescription{
		Started: runtime.started, Closed: runtime.closed,
		Rotating: runtime.rotating, Degraded: runtime.degraded,
		RotationEnabled: runtime.reloader != nil,
		Rotations:       runtime.rotations, RotationFailures: runtime.failures,
	}
	client := runtime.client
	runtime.mu.RUnlock()
	if client != nil {
		description.Watchers = client.Describe().Watchers
	}
	return description
}

// ManagedRuntimeStatus projects only bounded lifecycle and rotation counters.
func ManagedRuntimeStatus(runtime *ManagedRuntime) ops.RuntimeStatusProvider {
	return func(context.Context) (ops.RuntimeStatus, error) {
		description := runtime.Describe()
		state := "active"
		if description.Rotating {
			state = "rotating"
		}
		if description.Closed {
			state = "stopped"
		}
		return ops.RuntimeStatus{
			State: state, Ready: description.Started && !description.Closed,
			Degraded: description.Degraded, Active: description.Watchers,
			Counters: []ops.RuntimeCounter{
				{Name: "rotations", Value: description.Rotations},
				{Name: "rotation_failures", Value: description.RotationFailures},
			},
			Capabilities: []string{"etcd-v3", "full-snapshot-watch", "read-only-discovery", "active-tls-connection-rotation"},
		}, nil
	}
}

func (runtime *ManagedRuntime) run(ctx context.Context, subscription secret.UpdateSubscription) {
	defer close(runtime.done)
	for {
		select {
		case <-ctx.Done():
			return
		case _, open := <-subscription.Updates():
			if !open {
				return
			}
		}
		if runtime.rotateUntilReady(ctx, subscription.Updates()) != nil {
			return
		}
	}
}

func (runtime *ManagedRuntime) rotateUntilReady(ctx context.Context, updates <-chan uint64) error {
	delay := managedRotationRetryInitial
	for {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		runtime.setRotating(true)
		err := runtime.rotate(ctx)
		runtime.setRotating(false)
		if err == nil {
			runtime.mu.Lock()
			runtime.degraded = false
			runtime.rotations++
			runtime.mu.Unlock()
			return nil
		}
		if context.Cause(ctx) != nil {
			return context.Cause(ctx)
		}
		runtime.mu.Lock()
		runtime.degraded = true
		runtime.failures++
		runtime.mu.Unlock()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopManagedTimer(timer)
			return context.Cause(ctx)
		case _, open := <-updates:
			stopManagedTimer(timer)
			if !open {
				return ErrClosed
			}
			delay = managedRotationRetryInitial
		case <-timer.C:
			delay *= 2
			if delay > managedRotationRetryMaximum {
				delay = managedRotationRetryMaximum
			}
		}
	}
}

func (runtime *ManagedRuntime) rotate(ctx context.Context) error {
	client, err := runtime.candidate(ctx)
	if err != nil {
		return err
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		_ = client.Shutdown(context.Background())
		return ErrClosed
	}
	previous := runtime.client
	runtime.client = client
	runtime.mu.Unlock()
	if previous != nil {
		_ = previous.Shutdown(context.Background())
	}
	return nil
}

func (runtime *ManagedRuntime) candidate(ctx context.Context) (*Client, error) {
	clientConfig := clientv3.Config{
		Endpoints:   append([]string(nil), runtime.config.Endpoints...),
		DialTimeout: runtime.config.DialTimeout,
		Username:    runtime.config.Username,
	}
	if runtime.config.PasswordReference != "" {
		reference, _ := secret.Parse(runtime.config.PasswordReference)
		value, err := runtime.manager.Resolve(ctx, reference)
		if err != nil {
			return nil, fmt.Errorf("etcd registry: resolve password: %w", err)
		}
		password := value.Bytes()
		trimmed := secret.TrimLineBreaks(password)
		if len(trimmed) == 0 || len(trimmed) > maximumManagedPasswordBytes {
			clear(password)
			return nil, fmt.Errorf("%w: password size is invalid", ErrInvalidOption)
		}
		clientConfig.Password = string(trimmed)
		clear(password)
	}
	if strings.HasPrefix(runtime.config.Endpoints[0], "https://") {
		clientConfig.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: runtime.config.TLS.ServerName}
		if runtime.reloader != nil {
			var err error
			clientConfig.TLS, err = runtime.reloader.ClientConfig(tlsconfig.ClientOptions{
				ServerName: runtime.config.TLS.ServerName, MutualTLS: runtime.config.TLS.MutualTLS,
			})
			if err != nil {
				return nil, err
			}
		}
	}
	sdk, err := clientv3.New(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("etcd registry: create client: %w", err)
	}
	options := Options{Prefix: runtime.config.Prefix, MaxRecordBytes: runtime.config.MaxRecordBytes, OwnsClient: true}
	client, err := New(sdk, options)
	if err != nil {
		_ = sdk.Close()
		return nil, err
	}
	readyContext, cancel := context.WithTimeout(ctx, runtime.config.RotationReadyTimeout)
	_, err = sdk.Status(readyContext, runtime.config.Endpoints[0])
	cancel()
	if err != nil {
		_ = client.Shutdown(context.Background())
		return nil, fmt.Errorf("%w: %w", ErrConnectionUnavailable, err)
	}
	return client, nil
}

func (runtime *ManagedRuntime) shutdown(ctx context.Context) error {
	runtime.mu.Lock()
	runtime.closed = true
	runtime.started = false
	cancel, subscription, loopStarted, done, client := runtime.cancel, runtime.subscription, runtime.loopStarted, runtime.done, runtime.client
	runtime.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if subscription != nil {
		subscription.Close()
	}
	var result error
	if loopStarted {
		select {
		case <-done:
		case <-ctx.Done():
			result = errors.Join(result, context.Cause(ctx))
		}
	}
	if client != nil {
		result = errors.Join(result, client.Shutdown(context.Background()))
	}
	if runtime.tlsWatcher != nil {
		result = errors.Join(result, runtime.tlsWatcher.Shutdown(ctx))
	}
	return result
}

func (runtime *ManagedRuntime) setRotating(value bool) {
	runtime.mu.Lock()
	runtime.rotating = value
	runtime.mu.Unlock()
}

func normalizeManagedConfig(config ManagedConfig) ManagedConfig {
	config.Endpoints = append([]string(nil), config.Endpoints...)
	for index := range config.Endpoints {
		config.Endpoints[index] = strings.TrimSpace(config.Endpoints[index])
	}
	config.Prefix = strings.TrimSpace(config.Prefix)
	config.Username = strings.TrimSpace(config.Username)
	config.PasswordReference = strings.TrimSpace(config.PasswordReference)
	config.TLS.BundleReference = strings.TrimSpace(config.TLS.BundleReference)
	config.TLS.ServerName = strings.TrimSpace(config.TLS.ServerName)
	if config.DialTimeout == 0 {
		config.DialTimeout = defaultManagedDialTimeout
	}
	if config.RotationReadyTimeout == 0 {
		config.RotationReadyTimeout = defaultManagedReadyTimeout
	}
	return config
}

func isNilManagedSecretManager(manager tlsconfig.SecretManager) bool {
	if manager == nil {
		return true
	}
	value := reflect.ValueOf(manager)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func stopManagedTimer(timer *time.Timer) {
	if timer == nil || !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

var _ registry.Discovery = (*ManagedRuntime)(nil)
