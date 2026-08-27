// Package otlpgrpc provides an App-scoped OTLP/gRPC telemetry pipeline.
package otlpgrpc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/keelab/keelith/secret"
	"google.golang.org/grpc/credentials"
)

const (
	defaultTimeout              = 10 * time.Second
	defaultMetricInterval       = 30 * time.Second
	defaultMetricExportTimeout  = 10 * time.Second
	defaultTraceBatchTimeout    = 5 * time.Second
	defaultTraceQueueSize       = 2_048
	defaultTraceBatchSize       = 512
	defaultLogExportInterval    = time.Second
	defaultLogQueueSize         = 2_048
	defaultLogBatchSize         = 512
	defaultLogMaxRequestBytes   = 4 * 1_024 * 1_024
	defaultRotationReadyTimeout = 10 * time.Second
	maxEndpointBytes            = 1_024
	maxHeaders                  = 32
	maxHeaderKeyBytes           = 128
	maxHeaderValueBytes         = 4 * 1_024
	maxHeaderBytes              = 16 * 1_024
	maxUpdateSources            = 8
)

var (
	// ErrInvalidConfig reports unsafe or unsupported exporter configuration.
	ErrInvalidConfig = errors.New("otlpgrpc: invalid config")
	// ErrInvalidState reports a lifecycle operation in an invalid state.
	ErrInvalidState = errors.New("otlpgrpc: invalid lifecycle state")
)

// Config configures one isolated OTLP/gRPC pipeline.
type Config struct {
	// Endpoint is an OTLP collector host:port. Schemes and paths are rejected.
	Endpoint string

	// Headers are sent as gRPC metadata. They are cloned and never exposed by
	// Description. Resolve secret references before constructing the Pipeline.
	Headers map[string]string

	// Insecure disables transport security. It is intended for trusted local
	// development networks and cannot be combined with TLSConfig.
	Insecure bool

	// TLSConfig customizes secure transport. It is cloned and hardened to TLS
	// 1.2 or newer.
	TLSConfig *tls.Config

	// PerRPCCredentials supplies dynamic request metadata such as a
	// Secret-backed bearer token. It must require transport security and cannot
	// be combined with insecure transport or a static authorization header.
	PerRPCCredentials credentials.PerRPCCredentials

	// UpdateSources notify the pipeline after tls material has been atomically
	// replaced. Each successful notification creates and verifies a new
	// exporter generation before the active generation is swapped.
	UpdateSources []secret.UpdateSource

	// RotationReadyTimeout bounds construction and readiness verification for
	// one replacement exporter generation.
	RotationReadyTimeout time.Duration

	// Timeout bounds connection setup and individual OTLP operations.
	Timeout time.Duration

	// MetricInterval controls periodic metric collection and export.
	MetricInterval time.Duration

	// MetricExportTimeout bounds one periodic metric export.
	MetricExportTimeout time.Duration

	// TraceBatchTimeout bounds how long spans wait before a batch export.
	TraceBatchTimeout time.Duration

	// TraceQueueSize bounds spans buffered in process.
	TraceQueueSize int

	// TraceBatchSize bounds spans sent in one OTLP request.
	TraceBatchSize int

	// Compression enables gRPC gzip compression for traces, metrics, and logs.
	Compression bool

	// LogsEnabled attaches an isolated OpenTelemetry log provider to the
	// pipeline. The caller must install LogHandler before Start.
	LogsEnabled bool

	// LogExportInterval bounds how long log records wait before export.
	LogExportInterval time.Duration

	// LogQueueSize bounds log records buffered in process.
	LogQueueSize int

	// LogBatchSize bounds log records sent in one export batch.
	LogBatchSize int

	// LogMaxRequestBytes bounds an uncompressed OTLP log request.
	LogMaxRequestBytes int
}

type normalizedConfig struct {
	endpoint             string
	headers              map[string]string
	insecure             bool
	tlsConfig            *tls.Config
	perRPCCredentials    credentials.PerRPCCredentials
	updateSources        []secret.UpdateSource
	rotationReadyTimeout time.Duration
	timeout              time.Duration
	metricInterval       time.Duration
	metricExportTimeout  time.Duration
	traceBatchTimeout    time.Duration
	traceQueueSize       int
	traceBatchSize       int
	compression          bool
	logsEnabled          bool
	logExportInterval    time.Duration
	logQueueSize         int
	logBatchSize         int
	logMaxRequestBytes   int
}

func normalize(config Config) (normalizedConfig, error) {
	endpoint, err := normalizeEndpoint(config.Endpoint)
	if err != nil {
		return normalizedConfig{}, err
	}
	headers, err := normalizeHeaders(config.Headers)
	if err != nil {
		return normalizedConfig{}, err
	}
	if config.Insecure && config.TLSConfig != nil {
		return normalizedConfig{}, fmt.Errorf(
			"%w: insecure transport and tls config are mutually exclusive",
			ErrInvalidConfig,
		)
	}
	updateSources, rotationReadyTimeout, err := normalizeUpdateSources(config)
	if err != nil {
		return normalizedConfig{}, err
	}
	perRPCCredentials := config.PerRPCCredentials
	if perRPCCredentials != nil {
		value := reflect.ValueOf(perRPCCredentials)
		if value.Kind() == reflect.Pointer && value.IsNil() {
			return normalizedConfig{}, fmt.Errorf(
				"%w: per-RPC credentials are typed nil",
				ErrInvalidConfig,
			)
		}
		if config.Insecure {
			return normalizedConfig{}, fmt.Errorf(
				"%w: per-RPC credentials require secure transport",
				ErrInvalidConfig,
			)
		}
		if !perRPCCredentials.RequireTransportSecurity() {
			return normalizedConfig{}, fmt.Errorf(
				"%w: per-RPC credentials must require transport security",
				ErrInvalidConfig,
			)
		}
		if _, duplicate := headers["authorization"]; duplicate {
			return normalizedConfig{}, fmt.Errorf(
				"%w: dynamic credentials conflict with authorization header",
				ErrInvalidConfig,
			)
		}
	}
	timeout, err := normalizeDuration(
		"timeout",
		config.Timeout,
		defaultTimeout,
		time.Second,
		time.Minute,
	)
	if err != nil {
		return normalizedConfig{}, err
	}
	interval, err := normalizeDuration(
		"metric interval",
		config.MetricInterval,
		defaultMetricInterval,
		time.Second,
		10*time.Minute,
	)
	if err != nil {
		return normalizedConfig{}, err
	}
	exportTimeout, err := normalizeDuration(
		"metric export timeout",
		config.MetricExportTimeout,
		defaultMetricExportTimeout,
		time.Second,
		time.Minute,
	)
	if err != nil {
		return normalizedConfig{}, err
	}
	traceBatchTimeout, err := normalizeDuration(
		"trace batch timeout",
		config.TraceBatchTimeout,
		defaultTraceBatchTimeout,
		100*time.Millisecond,
		time.Minute,
	)
	if err != nil {
		return normalizedConfig{}, err
	}
	traceQueueSize := config.TraceQueueSize
	if traceQueueSize == 0 {
		traceQueueSize = defaultTraceQueueSize
	}
	if traceQueueSize < 64 || traceQueueSize > 65_536 {
		return normalizedConfig{}, fmt.Errorf(
			"%w: trace queue size must be between 64 and 65536",
			ErrInvalidConfig,
		)
	}
	traceBatchSize := config.TraceBatchSize
	if traceBatchSize == 0 {
		traceBatchSize = defaultTraceBatchSize
	}
	if traceBatchSize < 1 || traceBatchSize > traceQueueSize {
		return normalizedConfig{}, fmt.Errorf(
			"%w: trace batch size must be positive and no larger than the queue",
			ErrInvalidConfig,
		)
	}
	logExportInterval := config.LogExportInterval
	logQueueSize := config.LogQueueSize
	logBatchSize := config.LogBatchSize
	logMaxRequestBytes := config.LogMaxRequestBytes
	if !config.LogsEnabled {
		if logExportInterval != 0 || logQueueSize != 0 ||
			logBatchSize != 0 || logMaxRequestBytes != 0 {
			return normalizedConfig{}, fmt.Errorf(
				"%w: log tuning requires logs to be enabled",
				ErrInvalidConfig,
			)
		}
	} else {
		logExportInterval, err = normalizeDuration(
			"log export interval",
			logExportInterval,
			defaultLogExportInterval,
			100*time.Millisecond,
			time.Minute,
		)
		if err != nil {
			return normalizedConfig{}, err
		}
		if logQueueSize == 0 {
			logQueueSize = defaultLogQueueSize
		}
		if logQueueSize < 64 || logQueueSize > 65_536 {
			return normalizedConfig{}, fmt.Errorf(
				"%w: log queue size must be between 64 and 65536",
				ErrInvalidConfig,
			)
		}
		if logBatchSize == 0 {
			logBatchSize = defaultLogBatchSize
		}
		if logBatchSize < 1 || logBatchSize > logQueueSize {
			return normalizedConfig{}, fmt.Errorf(
				"%w: log batch size must be positive and no larger than the queue",
				ErrInvalidConfig,
			)
		}
		if logMaxRequestBytes == 0 {
			logMaxRequestBytes = defaultLogMaxRequestBytes
		}
		if logMaxRequestBytes < 64*1_024 || logMaxRequestBytes > 16*1_024*1_024 {
			return normalizedConfig{}, fmt.Errorf(
				"%w: log max request bytes must be between 65536 and 16777216",
				ErrInvalidConfig,
			)
		}
	}
	var tlsConfig *tls.Config
	if config.TLSConfig != nil {
		tlsConfig = config.TLSConfig.Clone()
		if tlsConfig.InsecureSkipVerify &&
			(tlsConfig.VerifyConnection == nil || len(updateSources) == 0) {
			return normalizedConfig{}, fmt.Errorf(
				"%w: skipped built-in TLS verification requires a dynamic verifier and update source",
				ErrInvalidConfig,
			)
		}
		if tlsConfig.MaxVersion != 0 &&
			tlsConfig.MaxVersion < tls.VersionTLS12 {
			return normalizedConfig{}, fmt.Errorf(
				"%w: TLS maximum version is older than TLS 1.2",
				ErrInvalidConfig,
			)
		}
		if tlsConfig.MinVersion == 0 ||
			tlsConfig.MinVersion < tls.VersionTLS12 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
	}
	return normalizedConfig{
		endpoint:             endpoint,
		headers:              headers,
		insecure:             config.Insecure,
		tlsConfig:            tlsConfig,
		perRPCCredentials:    perRPCCredentials,
		updateSources:        updateSources,
		rotationReadyTimeout: rotationReadyTimeout,
		timeout:              timeout,
		metricInterval:       interval,
		metricExportTimeout:  exportTimeout,
		traceBatchTimeout:    traceBatchTimeout,
		traceQueueSize:       traceQueueSize,
		traceBatchSize:       traceBatchSize,
		compression:          config.Compression,
		logsEnabled:          config.LogsEnabled,
		logExportInterval:    logExportInterval,
		logQueueSize:         logQueueSize,
		logBatchSize:         logBatchSize,
		logMaxRequestBytes:   logMaxRequestBytes,
	}, nil
}

func normalizeUpdateSources(
	config Config,
) ([]secret.UpdateSource, time.Duration, error) {
	if len(config.UpdateSources) == 0 {
		if config.RotationReadyTimeout != 0 {
			return nil, 0, fmt.Errorf(
				"%w: rotation timeout requires an update source",
				ErrInvalidConfig,
			)
		}
		return nil, 0, nil
	}
	if len(config.UpdateSources) > maxUpdateSources {
		return nil, 0, fmt.Errorf(
			"%w: too many update sources",
			ErrInvalidConfig,
		)
	}
	if config.Insecure || config.TLSConfig == nil {
		return nil, 0, fmt.Errorf(
			"%w: update sources require explicit secure tls config",
			ErrInvalidConfig,
		)
	}
	sources := make([]secret.UpdateSource, len(config.UpdateSources))
	for index, source := range config.UpdateSources {
		if source == nil || isTypedNil(source) {
			return nil, 0, fmt.Errorf(
				"%w: update source %d is nil",
				ErrInvalidConfig,
				index,
			)
		}
		sources[index] = source
	}
	timeout, err := normalizeDuration(
		"rotation ready timeout",
		config.RotationReadyTimeout,
		defaultRotationReadyTimeout,
		100*time.Millisecond,
		5*time.Minute,
	)
	if err != nil {
		return nil, 0, err
	}
	return sources, timeout, nil
}

func isTypedNil(value any) bool {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func normalizeEndpoint(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("%w: endpoint is empty", ErrInvalidConfig)
	}
	if value != strings.TrimSpace(value) {
		return "", fmt.Errorf(
			"%w: endpoint contains surrounding whitespace",
			ErrInvalidConfig,
		)
	}
	if len(value) > maxEndpointBytes {
		return "", fmt.Errorf("%w: endpoint is too long", ErrInvalidConfig)
	}
	if strings.Contains(value, "://") ||
		strings.ContainsAny(value, "/?#") ||
		containsControl(value) {
		return "", fmt.Errorf(
			"%w: endpoint must be host:port",
			ErrInvalidConfig,
		)
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf(
			"%w: endpoint must be a valid host:port",
			ErrInvalidConfig,
		)
	}
	for _, character := range host {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return "", fmt.Errorf(
				"%w: endpoint host is invalid",
				ErrInvalidConfig,
			)
		}
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return "", fmt.Errorf(
			"%w: endpoint port is invalid",
			ErrInvalidConfig,
		)
	}
	return value, nil
}

func normalizeHeaders(input map[string]string) (map[string]string, error) {
	if len(input) > maxHeaders {
		return nil, fmt.Errorf("%w: too many headers", ErrInvalidConfig)
	}
	if len(input) == 0 {
		return nil, nil
	}
	output := make(map[string]string, len(input))
	total := 0
	for rawKey, value := range input {
		key := strings.ToLower(rawKey)
		if key == "" ||
			len(key) > maxHeaderKeyBytes ||
			!validHeaderKey(key) {
			return nil, fmt.Errorf("%w: invalid header key", ErrInvalidConfig)
		}
		if strings.HasPrefix(key, "grpc-") ||
			strings.HasSuffix(key, "-bin") {
			return nil, fmt.Errorf(
				"%w: reserved or binary header key",
				ErrInvalidConfig,
			)
		}
		if len(value) > maxHeaderValueBytes || containsControl(value) {
			return nil, fmt.Errorf(
				"%w: invalid header value for %q",
				ErrInvalidConfig,
				key,
			)
		}
		if _, exists := output[key]; exists {
			return nil, fmt.Errorf(
				"%w: duplicate header %q after normalization",
				ErrInvalidConfig,
				key,
			)
		}
		total += len(key) + len(value)
		if total > maxHeaderBytes {
			return nil, fmt.Errorf("%w: headers are too large", ErrInvalidConfig)
		}
		output[key] = value
	}
	return output, nil
}

func validHeaderKey(value string) bool {
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') ||
			character == '-' ||
			character == '_' ||
			character == '.' {
			continue
		}
		return false
	}
	return true
}

func containsControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}

func normalizeDuration(
	name string,
	value time.Duration,
	defaultValue time.Duration,
	minimum time.Duration,
	maximum time.Duration,
) (time.Duration, error) {
	if value == 0 {
		return defaultValue, nil
	}
	if value < minimum || value > maximum {
		return 0, fmt.Errorf(
			"%w: %s must be between %s and %s",
			ErrInvalidConfig,
			name,
			minimum,
			maximum,
		)
	}
	return value, nil
}
