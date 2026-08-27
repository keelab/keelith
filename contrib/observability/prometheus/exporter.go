// Package prometheus provides an isolated OpenTelemetry Prometheus reader and
// a bounded scrape handler for Keelith Ops servers.
package prometheus

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const (
	defaultMaxRequestsInFlight = 4
	maxMaxRequestsInFlight     = 32
	defaultScrapeTimeout       = 10 * time.Second
	maxScrapeTimeout           = time.Minute
	maxNamespaceBytes          = 64
)

var (
	// ErrInvalidConfig reports an unsafe or malformed exporter configuration.
	ErrInvalidConfig = errors.New("prometheus exporter: invalid config")
	// ErrRegistration reports a duplicate or invalid runtime collector.
	ErrRegistration = errors.New("prometheus exporter: collector registration")
)

// Config controls one isolated registry and its scrape resource limits.
type Config struct {
	Namespace               string
	MaxRequestsInFlight     int
	ScrapeTimeout           time.Duration
	IncludeGoCollector      bool
	IncludeProcessCollector bool
	DisableTargetInfo       bool
	DisableScopeInfo        bool
}

// Exporter owns a private Prometheus registry, an OTel pull Reader and one
// bounded http handler. It never registers with Prometheus globals.
type Exporter struct {
	reader  *otelprom.Exporter
	handler http.Handler
}

// New constructs an isolated exporter without starting listeners or
// background goroutines.
func New(config Config) (*Exporter, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	registry := promclient.NewRegistry()
	if normalized.IncludeGoCollector {
		if err := registry.Register(collectors.NewGoCollector()); err != nil {
			return nil, fmt.Errorf("%w: go: %w", ErrRegistration, err)
		}
	}
	if normalized.IncludeProcessCollector {
		if err := registry.Register(
			collectors.NewProcessCollector(
				collectors.ProcessCollectorOpts{},
			),
		); err != nil {
			return nil, fmt.Errorf("%w: process: %w", ErrRegistration, err)
		}
	}
	optionList := []otelprom.Option{
		otelprom.WithRegisterer(registry),
	}
	if normalized.Namespace != "" {
		optionList = append(
			optionList,
			otelprom.WithNamespace(normalized.Namespace),
		)
	}
	if normalized.DisableTargetInfo {
		optionList = append(optionList, otelprom.WithoutTargetInfo())
	}
	if normalized.DisableScopeInfo {
		optionList = append(optionList, otelprom.WithoutScopeInfo())
	}
	reader, err := otelprom.New(optionList...)
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: create reader: %w", err)
	}
	handler := promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			ErrorHandling:       promhttp.HTTPErrorOnError,
			MaxRequestsInFlight: normalized.MaxRequestsInFlight,
			Timeout:             normalized.ScrapeTimeout,
			EnableOpenMetrics:   true,
		},
	)
	return &Exporter{reader: reader, handler: handler}, nil
}

// Reader returns the pull reader to pass to observability.Config.
func (exporter *Exporter) Reader() sdkmetric.Reader {
	if exporter == nil {
		return nil
	}
	return exporter.reader
}

// Handler returns the bounded scrape handler to pass to ops.WithMetrics.
func (exporter *Exporter) Handler() http.Handler {
	if exporter == nil {
		return nil
	}
	return exporter.handler
}

func normalizeConfig(config Config) (Config, error) {
	if strings.TrimSpace(config.Namespace) != config.Namespace ||
		!validNamespace(config.Namespace) {
		return Config{}, fmt.Errorf(
			"%w: namespace %q is malformed",
			ErrInvalidConfig,
			config.Namespace,
		)
	}
	if config.MaxRequestsInFlight < 0 ||
		config.MaxRequestsInFlight > maxMaxRequestsInFlight {
		return Config{}, fmt.Errorf(
			"%w: max requests in flight must be within [0, %d]",
			ErrInvalidConfig,
			maxMaxRequestsInFlight,
		)
	}
	if config.MaxRequestsInFlight == 0 {
		config.MaxRequestsInFlight = defaultMaxRequestsInFlight
	}
	if config.ScrapeTimeout < 0 ||
		config.ScrapeTimeout > maxScrapeTimeout {
		return Config{}, fmt.Errorf(
			"%w: scrape timeout must be within [0, %s]",
			ErrInvalidConfig,
			maxScrapeTimeout,
		)
	}
	if config.ScrapeTimeout == 0 {
		config.ScrapeTimeout = defaultScrapeTimeout
	}
	return config, nil
}

func validNamespace(namespace string) bool {
	if namespace == "" {
		return true
	}
	if len(namespace) > maxNamespaceBytes {
		return false
	}
	for index := 0; index < len(namespace); index++ {
		character := namespace[index]
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character == '_' ||
			character == ':' ||
			index > 0 && character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return true
}
