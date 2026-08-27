// Package managed constructs provider-backed, read-only registry discovery
// runtimes without exposing service registration to consumers.
package managed

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	nacosruntime "github.com/keelab/contrib/nacos"
	consulregistry "github.com/keelab/contrib/registry/consul"
	etcdregistry "github.com/keelab/contrib/registry/etcd"
	nacosregistry "github.com/keelab/contrib/registry/nacos"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/registry"
	"github.com/keelab/keelith/secret"
)

var (
	// ErrInvalidConfig reports malformed or mixed provider configuration.
	ErrInvalidConfig = errors.New("managed registry discovery: invalid config")
	// ErrClosed reports use of a nil or closed runtime.
	ErrClosed = errors.New("managed registry discovery: closed")
)

// Provider identifies one registry discovery backend.
type Provider string

const (
	// ProviderConsul uses Consul health service discovery.
	ProviderConsul Provider = "consul"
	// ProviderEtcd uses etcd-backed registry discovery.
	ProviderEtcd Provider = "etcd"
	// ProviderNacos uses Nacos naming discovery.
	ProviderNacos Provider = "nacos"
)

// consulConfig contains discovery-only consul settings. Registration ttl and
// advertised endpoints are deliberately absent.
type consulConfig struct {
	Address          string        `config:"address"`
	Datacenter       string        `config:"datacenter"`
	Scheme           string        `config:"scheme"`
	BlockingWait     time.Duration `config:"blocking_wait"`
	MaxResponseBytes int64         `config:"max_response_bytes"`
	TokenReference   string        `config:"token_reference"`
}

func (config consulConfig) options() consulregistry.Options {
	return consulregistry.Options{
		Address:          config.Address,
		Datacenter:       config.Datacenter,
		Scheme:           config.Scheme,
		BlockingWait:     config.BlockingWait,
		MaxResponseBytes: config.MaxResponseBytes,
		OwnsClient:       true,
	}
}

// nacosConfig contains discovery-only nacos SDK and naming settings.
// Registration ephemerality is deliberately absent.
type nacosConfig struct {
	Client  nacosruntime.Config `config:"client"`
	Scheme  string              `config:"scheme"`
	Group   string              `config:"group"`
	Cluster string              `config:"cluster"`
}

func (config nacosConfig) options() nacosregistry.Options {
	return nacosregistry.Options{
		Scheme:  config.Scheme,
		Group:   config.Group,
		Cluster: config.Cluster,
	}
}

// Config selects exactly one read-only registry discovery provider.
type Config struct {
	Provider Provider                   `config:"provider"`
	consul   consulConfig               `config:"consul"`
	etcd     etcdregistry.ManagedConfig `config:"etcd"`
	nacos    nacosConfig                `config:"nacos"`
}

// ValidateConfig rejects ambiguous providers, registration settings, unsafe
// transports, malformed credentials, and inactive-provider configuration.
func ValidateConfig(config Config) error {
	provider := Provider(strings.ToLower(strings.TrimSpace(string(config.Provider))))
	if string(provider) != string(config.Provider) {
		return invalidConfig("provider must be normalized")
	}
	switch provider {
	case ProviderConsul:
		if !reflect.DeepEqual(config.etcd, etcdregistry.ManagedConfig{}) ||
			!reflect.DeepEqual(config.nacos, nacosConfig{}) {
			return invalidConfig("consul discovery cannot include other provider settings")
		}
		normalized, err := consulregistry.NormalizeOptions(config.consul.options())
		if err != nil {
			return fmt.Errorf("%w: consul: %w", ErrInvalidConfig, err)
		}
		if config.consul.TokenReference != "" {
			if _, err := secret.Parse(config.consul.TokenReference); err != nil {
				return fmt.Errorf("%w: consul token: %w", ErrInvalidConfig, err)
			}
			if !secureCredentialEndpoint(normalized.Address) {
				return invalidConfig(
					"consul token requires https or a loopback endpoint",
				)
			}
		}
	case ProviderEtcd:
		if !reflect.DeepEqual(config.consul, consulConfig{}) ||
			!reflect.DeepEqual(config.nacos, nacosConfig{}) {
			return invalidConfig("etcd discovery cannot include other provider settings")
		}
		if err := etcdregistry.ValidateManagedConfig(config.etcd); err != nil {
			return fmt.Errorf("%w: etcd: %w", ErrInvalidConfig, err)
		}
	case ProviderNacos:
		if !reflect.DeepEqual(config.consul, consulConfig{}) ||
			!reflect.DeepEqual(config.etcd, etcdregistry.ManagedConfig{}) {
			return invalidConfig("nacos discovery cannot include other provider settings")
		}
		if err := nacosruntime.ValidateConfig(config.nacos.Client); err != nil {
			return fmt.Errorf("%w: nacos client: %w", ErrInvalidConfig, err)
		}
		if config.nacos.Client.PasswordReference != "" &&
			!config.nacos.Client.TLS.Enabled {
			return invalidConfig("nacos credentials require TLS")
		}
		if err := nacosregistry.ValidateOptions(config.nacos.options()); err != nil {
			return fmt.Errorf("%w: nacos naming: %w", ErrInvalidConfig, err)
		}
	default:
		return invalidConfig("provider must be consul, etcd, or nacos")
	}
	return nil
}

// Runtime owns one provider client but deliberately implements only
// registry.Discovery, never registry.Registrar.
type Runtime struct {
	provider  Provider
	discovery registry.Discovery
	start     func(context.Context) error
	shutdown  func(context.Context) error
	describe  func() Description
}

// Description is a material-free operational snapshot.
type Description struct {
	Provider     Provider
	Started      bool
	Closed       bool
	Rotating     bool
	Degraded     bool
	Watchers     int
	Rotations    uint64
	Failures     uint64
	Capabilities []string
}

// Open validates, constructs, and owns one read-only discovery runtime.
func Open(
	ctx context.Context,
	manager *secret.Manager,
	config Config,
) (*Runtime, error) {
	if ctx == nil {
		return nil, invalidConfig("context is nil")
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	switch config.Provider {
	case ProviderConsul:
		options, err := consulregistry.NormalizeOptions(config.consul.options())
		if err != nil {
			return nil, err
		}
		clientOptions := make([]consulregistry.ClientOption, 0, 1)
		if config.consul.TokenReference != "" {
			if manager == nil {
				return nil, invalidConfig("consul token requires Secret Manager")
			}
			reference, err := secret.Parse(config.consul.TokenReference)
			if err != nil {
				return nil, err
			}
			clientOptions = append(
				clientOptions,
				consulregistry.WithSecretToken(manager, reference),
			)
		}
		client, err := consulregistry.New(
			&http.Client{Timeout: options.BlockingWait + 30*time.Second},
			options,
			clientOptions...,
		)
		if err != nil {
			return nil, err
		}
		return &Runtime{
			provider:  ProviderConsul,
			discovery: client,
			start:     func(context.Context) error { return nil },
			shutdown:  client.Shutdown,
			describe: func() Description {
				current := client.Describe()
				return Description{
					Provider: ProviderConsul,
					Started:  !current.Closed,
					Closed:   current.Closed,
					Degraded: current.LastError != "",
					Watchers: current.Watchers,
					Capabilities: []string{
						"blocking-query-discovery",
						"full-snapshot-watch",
						"read-only-discovery",
					},
				}
			},
		}, nil
	case ProviderEtcd:
		client, err := etcdregistry.OpenManaged(config.etcd, manager)
		if err != nil {
			return nil, err
		}
		return &Runtime{
			provider:  ProviderEtcd,
			discovery: client,
			start:     client.Start,
			shutdown:  client.Shutdown,
			describe: func() Description {
				current := client.Describe()
				return Description{
					Provider:  ProviderEtcd,
					Started:   current.Started,
					Closed:    current.Closed,
					Rotating:  current.Rotating,
					Degraded:  current.Degraded,
					Watchers:  current.Watchers,
					Rotations: current.Rotations,
					Failures:  current.RotationFailures,
					Capabilities: []string{
						"etcd-v3",
						"full-snapshot-watch",
						"read-only-discovery",
						"active-tls-connection-rotation",
					},
				}
			},
		}, nil
	case ProviderNacos:
		var resolver nacosruntime.SecretResolver
		if config.nacos.Client.PasswordReference != "" {
			if manager == nil {
				return nil, invalidConfig("nacos password requires Secret Manager")
			}
			resolver = manager
		}
		client, err := nacosregistry.Open(
			ctx,
			config.nacos.Client,
			resolver,
			config.nacos.options(),
		)
		if err != nil {
			return nil, err
		}
		return &Runtime{
			provider:  ProviderNacos,
			discovery: client,
			start:     func(context.Context) error { return nil },
			shutdown:  client.Shutdown,
			describe: func() Description {
				current := client.Describe()
				return Description{
					Provider: ProviderNacos,
					Started:  !current.Closed,
					Closed:   current.Closed,
					Watchers: current.Watchers,
					Capabilities: []string{
						"full-snapshot-watch",
						"nacos-subscription",
						"read-only-discovery",
					},
				}
			},
		}, nil
	default:
		return nil, invalidConfig("unsupported provider")
	}
}

// Start activates providers that defer network traffic until App startup.
// Providers constructed eagerly keep this method as an idempotent no-op.
func (runtime *Runtime) Start(ctx context.Context) error {
	if runtime == nil || runtime.start == nil {
		return ErrClosed
	}
	return runtime.start(ctx)
}

// Watch delegates to the owned provider without exposing Registrar methods.
func (runtime *Runtime) Watch(
	ctx context.Context,
	service string,
) (registry.Watcher, error) {
	if runtime == nil || runtime.discovery == nil {
		return nil, ErrClosed
	}
	return runtime.discovery.Watch(ctx, service)
}

// Shutdown closes provider watchers and the owned backend client.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil || runtime.shutdown == nil {
		return nil
	}
	return runtime.shutdown(ctx)
}

// Describe returns a bounded snapshot without addresses, credentials, service
// names, endpoint values, revisions, or provider error text.
func (runtime *Runtime) Describe() Description {
	if runtime == nil || runtime.describe == nil {
		return Description{Closed: true}
	}
	current := runtime.describe()
	current.Capabilities = append([]string(nil), current.Capabilities...)
	return current
}

// RuntimeStatus adapts a managed provider to the shared low-sensitive catalog.
func RuntimeStatus(runtime *Runtime) ops.RuntimeStatusProvider {
	return func(context.Context) (ops.RuntimeStatus, error) {
		description := runtime.Describe()
		state := "active"
		if !description.Started {
			state = "starting"
		}
		if description.Rotating {
			state = "rotating"
		}
		if description.Closed {
			state = "stopped"
		}
		return ops.RuntimeStatus{
			State:    state,
			Ready:    description.Started && !description.Closed,
			Degraded: description.Degraded,
			Active:   description.Watchers,
			Counters: []ops.RuntimeCounter{
				{Name: "rotations", Value: description.Rotations},
				{Name: "rotation_failures", Value: description.Failures},
			},
			Capabilities: append([]string(nil), description.Capabilities...),
		}, nil
	}
}

func invalidConfig(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, arguments...))
}

func secureCredentialEndpoint(address string) bool {
	parsed, err := url.Parse(address)
	if err != nil {
		return false
	}
	if parsed.Scheme == "https" {
		return true
	}
	host := parsed.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

var _ registry.Discovery = (*Runtime)(nil)
