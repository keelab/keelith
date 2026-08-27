package etcdversioned

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
	"unicode/utf8"

	"github.com/keelab/keelith/config"
	"github.com/keelab/keelith/secret"
	"github.com/keelab/keelith/transport/tlsconfig"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultDialTimeout      = 5 * time.Second
	maximumDialTimeout      = time.Minute
	maximumRuntimeEndpoints = 16
	maximumPasswordBytes    = 64 * 1024
)

// TLSRuntimeConfig controls server trust and optional client identity.
// BundleReference points to one atomic tlsconfig json secret.
type TLSRuntimeConfig struct {
	BundleReference string `config:"bundle_reference"`
	ServerName      string `config:"server_name"`
	MutualTLS       bool   `config:"mutual_tls"`
}

// RuntimeConfig is the construction-time, read-only application connection.
// Password material is always resolved from a Secret reference.
type RuntimeConfig struct {
	Endpoints         []string         `config:"endpoints"`
	Prefix            string           `config:"prefix"`
	DialTimeout       time.Duration    `config:"dial_timeout"`
	Username          string           `config:"username"`
	PasswordReference string           `config:"password_reference"`
	AllowInsecure     bool             `config:"allow_insecure"`
	TLS               TLSRuntimeConfig `config:"tls"`
}

// SecretManager resolves credentials and watches atomic tls replacements.
type SecretManager interface {
	tlsconfig.SecretManager
}

// RuntimeDescription is low-cardinality and omits endpoints, prefix,
// references, identities, revisions, and backend error text.
type RuntimeDescription struct {
	Closed        bool
	Active        bool
	Generation    uint64
	Watchers      int
	LastError     string
	TLS           bool
	MutualTLS     bool
	Authenticated bool
	AllowInsecure bool
	WatchesTLS    bool
	TLSReloads    uint64
	TLSReconnects uint64
	TLSFailures   uint64
}

// Runtime owns the read-only etcd client, immutable Store view, Source, and
// optional TLS Secret watcher used by a generated application.
type Runtime struct {
	source     *Source
	tlsWatcher *tlsconfig.SecretWatcher
	config     RuntimeConfig

	closeOnce sync.Once
	closeErr  error
}

// ValidateRuntimeConfig validates transport, namespace, auth, and TLS
// invariants without resolving any Secret material.
func ValidateRuntimeConfig(runtime RuntimeConfig) error {
	runtime = normalizeRuntimeConfig(runtime)
	if len(runtime.Endpoints) < 1 || len(runtime.Endpoints) > maximumRuntimeEndpoints {
		return fmt.Errorf("%w: endpoints count must be between 1 and %d", ErrInvalidOption, maximumRuntimeEndpoints)
	}
	seen := make(map[string]struct{}, len(runtime.Endpoints))
	scheme := ""
	for _, endpoint := range runtime.Endpoints {
		parsed, err := url.ParseRequestURI(endpoint)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
			return fmt.Errorf("%w: invalid endpoint", ErrInvalidOption)
		}
		if (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" ||
			parsed.Fragment != "" || parsed.User != nil {
			return fmt.Errorf("%w: endpoint contains unsupported url fields", ErrInvalidOption)
		}
		if scheme == "" {
			scheme = parsed.Scheme
		} else if scheme != parsed.Scheme {
			return fmt.Errorf("%w: http and https endpoints cannot be mixed", ErrInvalidOption)
		}
		if _, duplicate := seen[endpoint]; duplicate {
			return fmt.Errorf("%w: duplicate endpoint", ErrInvalidOption)
		}
		seen[endpoint] = struct{}{}
	}
	if (scheme == "http") != runtime.AllowInsecure {
		return fmt.Errorf("%w: plaintext endpoints require exactly one explicit allow-insecure setting", ErrInvalidOption)
	}
	if !validPrefix(runtime.Prefix) {
		return fmt.Errorf("%w: prefix is invalid", ErrInvalidOption)
	}
	if runtime.DialTimeout < time.Second || runtime.DialTimeout > maximumDialTimeout {
		return fmt.Errorf("%w: dial timeout must be between 1s and 1m", ErrInvalidOption)
	}
	if !validRuntimeText(runtime.Username, false, 256) ||
		!validRuntimeText(runtime.TLS.ServerName, false, 253) {
		return fmt.Errorf("%w: username or tls server name is invalid", ErrInvalidOption)
	}
	if (runtime.Username == "") != (runtime.PasswordReference == "") {
		return fmt.Errorf("%w: username and password reference must be configured together", ErrInvalidOption)
	}
	if runtime.PasswordReference != "" {
		if _, err := secret.Parse(runtime.PasswordReference); err != nil {
			return fmt.Errorf("%w: password reference: %w", ErrInvalidOption, err)
		}
	}
	if runtime.TLS.BundleReference != "" {
		if scheme != "https" {
			return fmt.Errorf("%w: tls bundle requires https", ErrInvalidOption)
		}
		if _, err := secret.Parse(runtime.TLS.BundleReference); err != nil {
			return fmt.Errorf("%w: tls bundle reference: %w", ErrInvalidOption, err)
		}
	}
	if runtime.TLS.MutualTLS && runtime.TLS.BundleReference == "" {
		return fmt.Errorf("%w: mtls requires a tls bundle", ErrInvalidOption)
	}
	if scheme != "https" && (runtime.TLS.BundleReference != "" ||
		runtime.TLS.ServerName != "" || runtime.TLS.MutualTLS) {
		return fmt.Errorf("%w: plaintext runtime contains tls settings", ErrInvalidOption)
	}
	return nil
}

// OpenRuntime resolves initial credentials, starts an optional tls bundle
// watcher, and constructs a read-only versioned Source. It does not perform a
// network read; Config Manager Load remains the startup gate.
func OpenRuntime(
	ctx context.Context,
	runtime RuntimeConfig,
	manager SecretManager,
) (*Runtime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	runtime = normalizeRuntimeConfig(runtime)
	if err := ValidateRuntimeConfig(runtime); err != nil {
		return nil, err
	}
	if (runtime.PasswordReference != "" || runtime.TLS.BundleReference != "") && isNilSecretManager(manager) {
		return nil, fmt.Errorf("%w: secret-backed runtime requires a manager", ErrInvalidOption)
	}

	clientConfig := clientv3.Config{
		Endpoints:   append([]string(nil), runtime.Endpoints...),
		DialTimeout: runtime.DialTimeout,
		Username:    runtime.Username,
	}
	if runtime.PasswordReference != "" {
		reference, _ := secret.Parse(runtime.PasswordReference)
		value, err := manager.Resolve(ctx, reference)
		if err != nil {
			return nil, fmt.Errorf("versioned etcd config: resolve password: %w", err)
		}
		password := value.Bytes()
		trimmed := secret.TrimLineBreaks(password)
		if len(trimmed) == 0 || len(trimmed) > maximumPasswordBytes {
			clear(password)
			return nil, fmt.Errorf("%w: password size is invalid", ErrInvalidOption)
		}
		clientConfig.Password = string(trimmed)
		clear(password)
	}

	var watcher *tlsconfig.SecretWatcher
	if strings.HasPrefix(runtime.Endpoints[0], "https://") {
		clientConfig.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: runtime.TLS.ServerName,
		}
		if runtime.TLS.BundleReference != "" {
			reference, _ := secret.Parse(runtime.TLS.BundleReference)
			reloader := tlsconfig.NewEmpty()
			var err error
			watcher, err = tlsconfig.NewSecretWatcher(manager, reference, reloader)
			if err != nil {
				return nil, fmt.Errorf("versioned etcd config: tls watcher: %w", err)
			}
			if err := watcher.Start(ctx); err != nil {
				return nil, fmt.Errorf("versioned etcd config: start tls watcher: %w", err)
			}
			clientConfig.TLS, err = reloader.ClientConfig(tlsconfig.ClientOptions{
				ServerName: runtime.TLS.ServerName,
				MutualTLS:  runtime.TLS.MutualTLS,
			})
			if err != nil {
				_ = watcher.Shutdown(context.WithoutCancel(ctx))
				return nil, fmt.Errorf("versioned etcd config: tls client: %w", err)
			}
		}
	}

	client, err := clientv3.New(clientConfig)
	if err != nil {
		if watcher != nil {
			_ = watcher.Shutdown(context.WithoutCancel(ctx))
		}
		return nil, fmt.Errorf("versioned etcd config: create client: %w", err)
	}
	store, err := New(client, Options{
		Prefix: runtime.Prefix, OwnsBackend: true,
	})
	if err != nil {
		_ = client.Close()
		if watcher != nil {
			_ = watcher.Shutdown(context.WithoutCancel(ctx))
		}
		return nil, err
	}
	source, err := NewSource(store, true)
	if err != nil {
		_ = store.Close()
		if watcher != nil {
			_ = watcher.Shutdown(context.WithoutCancel(ctx))
		}
		return nil, err
	}
	return &Runtime{source: source, tlsWatcher: watcher, config: runtime}, nil
}

// Load delegates to the read-only active-pointer Source.
func (runtime *Runtime) Load(ctx context.Context) (config.Snapshot, error) {
	if runtime == nil || runtime.source == nil {
		return config.Snapshot{}, ErrInvalidOption
	}
	return runtime.source.Load(ctx)
}

// Watch delegates to the read-only active-pointer Source.
func (runtime *Runtime) Watch(ctx context.Context) (config.Watcher, error) {
	if runtime == nil || runtime.source == nil {
		return nil, ErrInvalidOption
	}
	return runtime.source.Watch(ctx)
}

// Shutdown closes Source/client before stopping the credential watcher.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		return ErrInvalidOption
	}
	runtime.closeOnce.Do(func() {
		if runtime.source != nil {
			runtime.closeErr = errors.Join(runtime.closeErr, runtime.source.Shutdown(ctx))
		}
		if runtime.tlsWatcher != nil {
			runtime.closeErr = errors.Join(runtime.closeErr, runtime.tlsWatcher.Shutdown(ctx))
		}
	})
	return runtime.closeErr
}

// Describe returns bounded, secret-free runtime state.
func (runtime *Runtime) Describe() RuntimeDescription {
	if runtime == nil || runtime.source == nil {
		return RuntimeDescription{Closed: true}
	}
	source := runtime.source.Describe()
	description := RuntimeDescription{
		Closed: source.Closed, Active: source.Active,
		Generation: source.Generation, Watchers: source.Watchers,
		LastError:     source.LastError,
		TLS:           strings.HasPrefix(runtime.config.Endpoints[0], "https://"),
		MutualTLS:     runtime.config.TLS.MutualTLS,
		Authenticated: runtime.config.PasswordReference != "",
		AllowInsecure: runtime.config.AllowInsecure,
		WatchesTLS:    runtime.tlsWatcher != nil,
	}
	if runtime.tlsWatcher != nil {
		watcher := runtime.tlsWatcher.Description()
		description.TLSReloads = watcher.Reloads
		description.TLSReconnects = watcher.Reconnects
		description.TLSFailures = watcher.Failures
	}
	return description
}

func normalizeRuntimeConfig(runtime RuntimeConfig) RuntimeConfig {
	runtime.Endpoints = append([]string(nil), runtime.Endpoints...)
	for index := range runtime.Endpoints {
		runtime.Endpoints[index] = strings.TrimSpace(runtime.Endpoints[index])
	}
	runtime.Prefix = strings.TrimSuffix(strings.TrimSpace(runtime.Prefix), "/")
	runtime.Username = strings.TrimSpace(runtime.Username)
	runtime.PasswordReference = strings.TrimSpace(runtime.PasswordReference)
	runtime.TLS.BundleReference = strings.TrimSpace(runtime.TLS.BundleReference)
	runtime.TLS.ServerName = strings.TrimSpace(runtime.TLS.ServerName)
	if runtime.DialTimeout == 0 {
		runtime.DialTimeout = defaultDialTimeout
	}
	return runtime
}

func validRuntimeText(value string, required bool, limit int) bool {
	if !utf8.ValidString(value) || len(value) > limit ||
		(required && strings.TrimSpace(value) == "") {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func isNilSecretManager(manager SecretManager) bool {
	if manager == nil {
		return true
	}
	value := reflect.ValueOf(manager)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ config.Source = (*Runtime)(nil)
