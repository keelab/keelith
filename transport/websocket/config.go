package websocket

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
	"unicode"

	coderws "github.com/coder/websocket"
	kerrors "github.com/keelab/keelith/errors"
)

const (
	defaultName           = "keelith.websocket"
	defaultMaxConnections = 1024
	defaultMaxMessage     = int64(1024 * 1024)
	maxConnections        = 1_000_000
	maxMessageBytes       = int64(16 * 1024 * 1024)
)

var (
	// ErrInvalidOption reports malformed configuration, requests, or messages.
	ErrInvalidOption = errors.New("websocket: invalid option")
	// ErrNotRunning reports a handshake outside the Hub lifecycle.
	ErrNotRunning = errors.New("websocket: hub is not running")
	// ErrClosed reports cancellation caused by Hub shutdown.
	ErrClosed = errors.New("websocket: hub is closed")
	// ErrHandshake is the stable pre-upgrade HTTP error.
	ErrHandshake = kerrors.New(
		400,
		"WEBSOCKET_HANDSHAKE_FAILED",
		"websocket handshake failed",
	)
	// ErrCapacity is the stable pre-upgrade connection-capacity error.
	ErrCapacity = kerrors.New(
		503,
		"WEBSOCKET_CAPACITY_EXCEEDED",
		"websocket connection capacity is exceeded",
	)
	// ErrMessageTooLarge reports a message outside the configured byte budget.
	ErrMessageTooLarge = errors.New("websocket: message is too large")
	// ErrConnectionClosed reports a non-normal peer or transport termination.
	ErrConnectionClosed = errors.New("websocket: connection closed")
	// ErrHeartbeat reports a failed opt-in ping/pong liveness check.
	ErrHeartbeat = errors.New("websocket: heartbeat failed")
)

// CompressionMode selects the RFC 7692 per-message deflate policy.
type CompressionMode string

const (
	// CompressionDisabled avoids compression CPU and per-connection state.
	CompressionDisabled CompressionMode = "disabled"
	// CompressionNoContextTakeover resets compression state per message.
	CompressionNoContextTakeover CompressionMode = "no-context-takeover"
	// CompressionContextTakeover reuses state and consumes more connection memory.
	CompressionContextTakeover CompressionMode = "context-takeover"
)

// Options configure one lifecycle-owned WebSocket Hub.
type Options struct {
	Name                 string          `config:"name"`
	OriginPatterns       []string        `config:"origin_patterns"`
	AllowAnyOrigin       bool            `config:"allow_any_origin"`
	Subprotocols         []string        `config:"subprotocols"`
	RequireSubprotocol   bool            `config:"require_subprotocol"`
	Compression          CompressionMode `config:"compression"`
	CompressionThreshold int             `config:"compression_threshold"`
	MaxConnections       int             `config:"max_connections"`
	MaxReadBytes         int64           `config:"max_read_bytes"`
	MaxWriteBytes        int64           `config:"max_write_bytes"`
	PingInterval         time.Duration   `config:"ping_interval"`
	PingTimeout          time.Duration   `config:"ping_timeout"`
}

// NormalizeOptions applies safe defaults and validates bounded resources.
func NormalizeOptions(input Options) (Options, error) {
	options := input
	options.Name = strings.TrimSpace(options.Name)
	if options.Name == "" {
		options.Name = defaultName
	}
	if options.Compression == "" {
		options.Compression = CompressionDisabled
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = defaultMaxConnections
	}
	if options.MaxReadBytes == 0 {
		options.MaxReadBytes = defaultMaxMessage
	}
	if options.MaxWriteBytes == 0 {
		options.MaxWriteBytes = defaultMaxMessage
	}
	if !validName(options.Name) ||
		options.MaxConnections < 1 ||
		options.MaxConnections > maxConnections ||
		options.MaxReadBytes < 1 ||
		options.MaxReadBytes > maxMessageBytes ||
		options.MaxWriteBytes < 1 ||
		options.MaxWriteBytes > maxMessageBytes {
		return Options{}, fmt.Errorf(
			"%w: name or resource budget",
			ErrInvalidOption,
		)
	}
	switch options.Compression {
	case CompressionDisabled:
		if options.CompressionThreshold != 0 {
			return Options{}, fmt.Errorf(
				"%w: compression threshold requires compression",
				ErrInvalidOption,
			)
		}
	case CompressionNoContextTakeover, CompressionContextTakeover:
		if options.CompressionThreshold < 0 ||
			options.CompressionThreshold > int(maxMessageBytes) {
			return Options{}, fmt.Errorf(
				"%w: compression threshold",
				ErrInvalidOption,
			)
		}
	default:
		return Options{}, fmt.Errorf(
			"%w: compression mode %q",
			ErrInvalidOption,
			options.Compression,
		)
	}
	if options.PingInterval == 0 {
		if options.PingTimeout != 0 {
			return Options{}, fmt.Errorf(
				"%w: ping timeout requires ping interval",
				ErrInvalidOption,
			)
		}
	} else if options.PingInterval < time.Second ||
		options.PingInterval > 10*time.Minute ||
		options.PingTimeout < 100*time.Millisecond ||
		options.PingTimeout >= options.PingInterval {
		return Options{}, fmt.Errorf(
			"%w: heartbeat budget",
			ErrInvalidOption,
		)
	}
	origins, err := normalizeOrigins(options.OriginPatterns)
	if err != nil {
		return Options{}, err
	}
	subprotocols, err := normalizeSubprotocols(options.Subprotocols)
	if err != nil {
		return Options{}, err
	}
	if options.AllowAnyOrigin && len(origins) != 0 {
		return Options{}, fmt.Errorf(
			"%w: allow-any-origin conflicts with origin patterns",
			ErrInvalidOption,
		)
	}
	if options.RequireSubprotocol && len(subprotocols) == 0 {
		return Options{}, fmt.Errorf(
			"%w: required subprotocol list is empty",
			ErrInvalidOption,
		)
	}
	options.OriginPatterns = origins
	options.Subprotocols = subprotocols
	return options, nil
}

// ValidateOptions validates Options without constructing a Hub.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

func (options Options) acceptOptions() *coderws.AcceptOptions {
	return &coderws.AcceptOptions{
		Subprotocols:         append([]string(nil), options.Subprotocols...),
		InsecureSkipVerify:   options.AllowAnyOrigin,
		OriginPatterns:       append([]string(nil), options.OriginPatterns...),
		CompressionMode:      options.coderCompression(),
		CompressionThreshold: options.CompressionThreshold,
	}
}

func (options Options) coderCompression() coderws.CompressionMode {
	switch options.Compression {
	case CompressionNoContextTakeover:
		return coderws.CompressionNoContextTakeover
	case CompressionContextTakeover:
		return coderws.CompressionContextTakeover
	default:
		return coderws.CompressionDisabled
	}
}

func normalizeOrigins(values []string) ([]string, error) {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	if len(values) > 32 {
		return nil, fmt.Errorf(
			"%w: too many origin patterns",
			ErrInvalidOption,
		)
	}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if !validPlainText(value, 256) || value == "*" {
			return nil, fmt.Errorf(
				"%w: origin pattern",
				ErrInvalidOption,
			)
		}
		if _, err := path.Match(value, value); err != nil {
			return nil, fmt.Errorf(
				"%w: origin pattern",
				ErrInvalidOption,
			)
		}
		normalized := strings.ToLower(value)
		if _, duplicate := seen[normalized]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate origin pattern",
				ErrInvalidOption,
			)
		}
		seen[normalized] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

func normalizeSubprotocols(values []string) ([]string, error) {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	if len(values) > 32 {
		return nil, fmt.Errorf(
			"%w: too many subprotocols",
			ErrInvalidOption,
		)
	}
	for _, value := range values {
		if !validSubprotocol(value) {
			return nil, fmt.Errorf(
				"%w: subprotocol",
				ErrInvalidOption,
			)
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate subprotocol",
				ErrInvalidOption,
			)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

func validSubprotocol(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' ||
			strings.ContainsRune("!#$%&'*+-.^_`|~", r) {
			continue
		}
		return false
	}
	return true
}

func validPlainText(value string, maximum int) bool {
	if value == "" || len(value) > maximum ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validName(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '.',
			r == '_',
			r == '-',
			r == '/',
			r == ':':
		default:
			return false
		}
	}
	return true
}
