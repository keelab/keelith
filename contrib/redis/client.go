// Package redis owns a shared go-redis runtime for cache, quota, coordination,
// and other Redis-backed adapters.
package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/secret"
	goredis "github.com/redis/go-redis/v9"
)

var (
	// ErrInvalidConfig reports malformed topology, pool, or secret settings.
	ErrInvalidConfig = errors.New("redis runtime: invalid config")
	// ErrClosed reports an operation after runtime shutdown.
	ErrClosed = errors.New("redis runtime: closed")
)

// Mode makes Redis topology an explicit construction-time decision.
type Mode string

const (
	// ModeStandalone connects to a standalone Redis server.
	ModeStandalone Mode = "standalone"
	// ModeCluster connects to a Redis Cluster.
	ModeCluster Mode = "cluster"
	// ModeSentinel connects through Redis Sentinel.
	ModeSentinel Mode = "sentinel"
)

const (
	maxAddresses = 32
	maxPoolSize  = 100_000
	maxTimeout   = 5 * time.Minute
)

// Config contains topology and connection-pool settings. Password fields are
// secret references, never secret values.
type Config struct {
	Mode                      Mode          `config:"mode"`
	Addresses                 []string      `config:"addresses"`
	Username                  string        `config:"username"`
	PasswordReference         string        `config:"passwordRef"`
	SentinelUsername          string        `config:"sentinelUsername"`
	SentinelPasswordReference string        `config:"sentinelPasswordRef"`
	MasterName                string        `config:"masterName"`
	ClientName                string        `config:"clientName"`
	Protocol                  int           `config:"protocol"`
	DB                        int           `config:"db"`
	MaxRetries                int           `config:"maxRetries"`
	DialTimeout               time.Duration `config:"dialTimeout"`
	ReadTimeout               time.Duration `config:"readTimeout"`
	WriteTimeout              time.Duration `config:"writeTimeout"`
	PoolTimeout               time.Duration `config:"poolTimeout"`
	PoolSize                  int           `config:"poolSize"`
	MinIdleConnections        int           `config:"minIdleConnections"`
	MaxIdleConnections        int           `config:"maxIdleConnections"`
}

// SecretResolver resolves a safe reference through secret.Manager or another
// compatible provider router.
type SecretResolver interface {
	Resolve(context.Context, secret.Reference) (secret.Value, error)
}

// Option configures construction-only runtime behavior.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (function optionFunc) apply(options *options) error {
	return function(options)
}

type options struct {
	tlsConfig *tls.Config
}

// WithTLSConfig snapshots tls settings for all Redis connections.
func WithTLSConfig(config *tls.Config) Option {
	return optionFunc(func(options *options) error {
		if config == nil {
			return errors.New("tls config is nil")
		}
		options.tlsConfig = config.Clone()
		return nil
	})
}

// Description is a value-free, low-cardinality runtime snapshot.
type Description struct {
	Mode           Mode
	AddressCount   int
	DB             int
	TLS            bool
	PoolSize       int
	Started        bool
	Closed         bool
	HealthChecks   uint64
	HealthFailures uint64
}

// Client owns one shared UniversalClient and exposes it to Redis adapters.
type Client struct {
	client goredis.UniversalClient
	owns   bool

	mu          sync.Mutex
	description Description
	closeOnce   sync.Once
	closeErr    error
}

// Open resolves referenced credentials, snapshots options, and creates an
// owned UniversalClient. Connectivity remains inside Start's App rollback
// boundary.
func Open(
	ctx context.Context,
	config Config,
	resolver SecretResolver,
	optionList ...Option,
) (*Client, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	settings := options{}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidConfig,
				index,
			)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf(
				"%w: option %d: %w",
				ErrInvalidConfig,
				index,
				err,
			)
		}
	}
	universal, err := buildOptions(ctx, config, resolver)
	if err != nil {
		return nil, err
	}
	if settings.tlsConfig != nil {
		universal.TLSConfig = settings.tlsConfig.Clone()
	}
	raw := goredis.NewUniversalClient(universal)
	return &Client{
		client: raw,
		owns:   true,
		description: Description{
			Mode:         config.Mode,
			AddressCount: len(config.Addresses),
			DB:           config.DB,
			TLS:          universal.TLSConfig != nil,
			PoolSize:     config.PoolSize,
		},
	}, nil
}

// Wrap adopts an existing UniversalClient. The supplied topology description
// is diagnostic only; construction of the existing client remains external.
func Wrap(
	client goredis.UniversalClient,
	mode Mode,
	owns bool,
) (*Client, error) {
	if isNil(client) || !validMode(mode) {
		return nil, fmt.Errorf(
			"%w: client or mode is invalid",
			ErrInvalidConfig,
		)
	}
	return &Client{
		client: client,
		owns:   owns,
		description: Description{
			Mode: mode,
		},
	}, nil
}

// buildOptions validates topology and resolves secrets without opening a
// connection. It stays internal because the result contains resolved secrets.
func buildOptions(
	ctx context.Context,
	config Config,
	resolver SecretResolver,
) (*goredis.UniversalOptions, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	password, err := resolvePassword(
		ctx,
		resolver,
		config.PasswordReference,
	)
	if err != nil {
		return nil, fmt.Errorf("redis runtime: password: %w", err)
	}
	sentinelPassword, err := resolvePassword(
		ctx,
		resolver,
		config.SentinelPasswordReference,
	)
	if err != nil {
		return nil, fmt.Errorf("redis runtime: sentinel password: %w", err)
	}
	return &goredis.UniversalOptions{
		Addrs:            append([]string(nil), config.Addresses...),
		ClientName:       config.ClientName,
		Protocol:         config.Protocol,
		DB:               config.DB,
		Username:         config.Username,
		Password:         password,
		SentinelUsername: config.SentinelUsername,
		SentinelPassword: sentinelPassword,
		MasterName:       config.MasterName,
		MaxRetries:       config.MaxRetries,
		DialTimeout:      config.DialTimeout,
		ReadTimeout:      config.ReadTimeout,
		WriteTimeout:     config.WriteTimeout,
		PoolTimeout:      config.PoolTimeout,
		PoolSize:         config.PoolSize,
		MinIdleConns:     config.MinIdleConnections,
		MaxIdleConns:     config.MaxIdleConnections,
		IsClusterMode:    config.Mode == ModeCluster,
	}, nil
}

// ValidateConfig rejects ambiguous topology and unsafe connection settings.
func ValidateConfig(config Config) error {
	if !validMode(config.Mode) {
		return invalidConfig("mode %q is unsupported", config.Mode)
	}
	if len(config.Addresses) == 0 || len(config.Addresses) > maxAddresses {
		return invalidConfig("address count is outside 1..%d", maxAddresses)
	}
	seen := make(map[string]struct{}, len(config.Addresses))
	for _, address := range config.Addresses {
		if strings.TrimSpace(address) != address ||
			!validText(address, 512) {
			return invalidConfig("address is malformed")
		}
		host, port, err := net.SplitHostPort(address)
		portNumber, portErr := strconv.Atoi(port)
		if err != nil ||
			strings.TrimSpace(host) == "" ||
			strings.ContainsFunc(host, unicode.IsSpace) ||
			portErr != nil ||
			portNumber < 1 ||
			portNumber > 65_535 {
			return invalidConfig("address %q must be host:port", address)
		}
		if _, duplicate := seen[address]; duplicate {
			return invalidConfig("address %q is duplicated", address)
		}
		seen[address] = struct{}{}
	}
	if !validOptionalText(config.Username, 512) ||
		!validOptionalText(config.SentinelUsername, 512) ||
		!validOptionalText(config.ClientName, 512) ||
		!validOptionalText(config.MasterName, 512) {
		return invalidConfig("identity contains invalid text")
	}
	for _, reference := range []string{
		config.PasswordReference,
		config.SentinelPasswordReference,
	} {
		if reference == "" {
			continue
		}
		if _, err := secret.Parse(reference); err != nil {
			return invalidConfig("secret reference is malformed")
		}
	}
	switch config.Mode {
	case ModeStandalone:
		if len(config.Addresses) != 1 || config.MasterName != "" ||
			config.SentinelUsername != "" ||
			config.SentinelPasswordReference != "" {
			return invalidConfig(
				"standalone requires one address and no sentinel settings",
			)
		}
	case ModeCluster:
		if config.DB != 0 || config.MasterName != "" ||
			config.SentinelUsername != "" ||
			config.SentinelPasswordReference != "" {
			return invalidConfig(
				"cluster requires DB 0 and no sentinel settings",
			)
		}
	case ModeSentinel:
		if strings.TrimSpace(config.MasterName) == "" {
			return invalidConfig("sentinel requires masterName")
		}
	}
	if config.Protocol != 0 &&
		config.Protocol != 2 &&
		config.Protocol != 3 ||
		config.DB < 0 ||
		config.MaxRetries < -1 ||
		config.MaxRetries > 100 ||
		config.PoolSize < 0 ||
		config.PoolSize > maxPoolSize ||
		config.MinIdleConnections < 0 ||
		config.MinIdleConnections > maxPoolSize ||
		config.MaxIdleConnections < 0 ||
		config.MaxIdleConnections > maxPoolSize ||
		config.PoolSize > 0 &&
			(config.MinIdleConnections > config.PoolSize ||
				config.MaxIdleConnections > config.PoolSize) {
		return invalidConfig("DB, retry, or pool settings are invalid")
	}
	for _, timeout := range []time.Duration{
		config.DialTimeout,
		config.ReadTimeout,
		config.WriteTimeout,
		config.PoolTimeout,
	} {
		if timeout < 0 || timeout > maxTimeout ||
			timeout > 0 && timeout < time.Millisecond {
			return invalidConfig("timeout is outside the supported range")
		}
	}
	return nil
}

// Start verifies connectivity inside the App startup rollback boundary.
func (client *Client) Start(ctx context.Context) error {
	if client == nil || isNil(client.client) || ctx == nil {
		return fmt.Errorf("%w: client or context is nil", ErrInvalidConfig)
	}
	client.mu.Lock()
	if client.description.Closed {
		client.mu.Unlock()
		return ErrClosed
	}
	client.description.HealthChecks++
	client.mu.Unlock()
	if err := client.client.Ping(ctx).Err(); err != nil {
		client.mu.Lock()
		client.description.HealthFailures++
		client.mu.Unlock()
		return fmt.Errorf("redis runtime: ping: %w", err)
	}
	client.mu.Lock()
	client.description.Started = true
	client.mu.Unlock()
	return nil
}

// Shutdown closes an owned pool exactly once.
func (client *Client) Shutdown(ctx context.Context) error {
	if client == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	}
	client.closeOnce.Do(func() {
		client.mu.Lock()
		client.description.Closed = true
		client.mu.Unlock()
		if client.owns && !isNil(client.client) {
			client.closeErr = client.client.Close()
		}
	})
	return client.closeErr
}

// Universal returns the shared SDK client for cache, quota, coordination, and
// other adapters. Callers must not close it unless they own this runtime.
func (client *Client) Universal() goredis.UniversalClient {
	if client == nil {
		return nil
	}
	return client.client
}

// Description returns a value-free snapshot.
func (client *Client) Description() Description {
	if client == nil {
		return Description{}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.description
}

func resolvePassword(
	ctx context.Context,
	resolver SecretResolver,
	rawReference string,
) (string, error) {
	if rawReference == "" {
		return "", nil
	}
	if isNil(resolver) {
		return "", errors.New("secret resolver is nil")
	}
	reference, err := secret.Parse(rawReference)
	if err != nil {
		return "", err
	}
	value, err := resolver.Resolve(ctx, reference)
	if err != nil {
		return "", err
	}
	if err := value.Validate(); err != nil {
		return "", err
	}
	if value.Expired(time.Now()) {
		return "", secret.ErrInvalidValue
	}
	content := value.Bytes()
	defer clear(content)
	return string(secret.TrimLineBreaks(content)), nil
}

func validMode(mode Mode) bool {
	switch mode {
	case ModeStandalone, ModeCluster, ModeSentinel:
		return true
	default:
		return false
	}
}

func validOptionalText(value string, limit int) bool {
	return value == "" || validText(value, limit)
}

func validText(value string, limit int) bool {
	if value == "" ||
		len(value) > limit ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func invalidConfig(format string, arguments ...any) error {
	return fmt.Errorf(
		"%w: %s",
		ErrInvalidConfig,
		fmt.Sprintf(format, arguments...),
	)
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
