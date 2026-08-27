package vault

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
	"unicode"
)

const (
	defaultPollInterval    = 5 * time.Second
	defaultRequestTimeout  = 5 * time.Second
	defaultMaxBytes        = 1024 * 1024
	defaultMaxResponseSize = 2 * 1024 * 1024
	defaultMaxTokenBytes   = 8 * 1024

	maxSecretBytes   = 16 * 1024 * 1024
	maxResponseBytes = 32 * 1024 * 1024
)

var (
	// ErrInvalidOption reports an unsafe endpoint, path, credential, or budget.
	ErrInvalidOption = errors.New("secret/vault: invalid option")
	// ErrInvalidResponse reports a malformed or unusable Vault response.
	ErrInvalidResponse = errors.New("secret/vault: invalid response")
	// ErrUnauthorized reports a rejected Vault token without exposing it.
	ErrUnauthorized = errors.New("secret/vault: unauthorized")
	// ErrUnavailable reports throttling or a temporary Vault server failure.
	ErrUnavailable = errors.New("secret/vault: unavailable")
	// ErrTooLarge reports a response or selected field beyond its budget.
	ErrTooLarge = errors.New("secret/vault: value is too large")
	// ErrClosed reports an operation after Provider shutdown.
	ErrClosed = errors.New("secret/vault: provider closed")
)

// Options configure one read-only Vault KV v2 Provider.
type Options struct {
	Endpoint              string        `config:"endpoint"`
	Mount                 string        `config:"mount"`
	Namespace             string        `config:"namespace"`
	PollInterval          time.Duration `config:"poll_interval"`
	RequestTimeout        time.Duration `config:"request_timeout"`
	MaxBytes              int           `config:"max_bytes"`
	MaxResponseBytes      int           `config:"max_response_bytes"`
	MaxTokenBytes         int           `config:"max_token_bytes"`
	AllowInsecureLoopback bool          `config:"allow_insecure_loopback"`
}

// NormalizeOptions applies defaults and validates transport and resource
// boundaries without making a network request.
func NormalizeOptions(input Options) (Options, error) {
	options := input
	options.Endpoint = strings.TrimSpace(options.Endpoint)
	options.Mount = strings.Trim(strings.TrimSpace(options.Mount), "/")
	options.Namespace = strings.Trim(strings.TrimSpace(options.Namespace), "/")
	if options.PollInterval == 0 {
		options.PollInterval = defaultPollInterval
	}
	if options.RequestTimeout == 0 {
		options.RequestTimeout = defaultRequestTimeout
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = defaultMaxBytes
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = defaultMaxResponseSize
		if options.MaxBytes+1024 > options.MaxResponseBytes {
			options.MaxResponseBytes = options.MaxBytes + 1024
		}
	}
	if options.MaxTokenBytes == 0 {
		options.MaxTokenBytes = defaultMaxTokenBytes
	}
	if options.PollInterval < 100*time.Millisecond ||
		options.PollInterval > 10*time.Minute ||
		options.RequestTimeout < 100*time.Millisecond ||
		options.RequestTimeout > time.Minute ||
		options.MaxBytes < 1 ||
		options.MaxBytes > maxSecretBytes ||
		options.MaxResponseBytes < options.MaxBytes+1024 ||
		options.MaxResponseBytes > maxResponseBytes ||
		options.MaxTokenBytes < 1 ||
		options.MaxTokenBytes > defaultMaxTokenBytes {
		return Options{}, fmt.Errorf("%w: resource budgets", ErrInvalidOption)
	}
	endpoint, err := normalizeEndpoint(
		options.Endpoint,
		options.AllowInsecureLoopback,
	)
	if err != nil {
		return Options{}, err
	}
	options.Endpoint = endpoint
	if !validPath(options.Mount, 256) {
		return Options{}, fmt.Errorf("%w: mount is malformed", ErrInvalidOption)
	}
	if options.Namespace != "" && !validPath(options.Namespace, 256) {
		return Options{}, fmt.Errorf(
			"%w: namespace is malformed",
			ErrInvalidOption,
		)
	}
	return options, nil
}

// ValidateOptions validates Options without constructing a Provider.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

func normalizeEndpoint(raw string, allowInsecure bool) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil ||
		parsed.Host == "" ||
		parsed.User != nil ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" ||
		parsed.Path != "" && parsed.Path != "/" {
		return "", fmt.Errorf("%w: endpoint is malformed", ErrInvalidOption)
	}
	switch parsed.Scheme {
	case "https":
	case "http":
		if !allowInsecure || !isLoopbackHost(parsed.Hostname()) {
			return "", fmt.Errorf(
				"%w: plaintext endpoint is not an allowed loopback",
				ErrInvalidOption,
			)
		}
	default:
		return "", fmt.Errorf(
			"%w: endpoint must use https",
			ErrInvalidOption,
		)
	}
	parsed.Path = ""
	return strings.TrimSuffix(parsed.String(), "/"), nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func validPath(value string, maxLength int) bool {
	if value == "" ||
		len(value) > maxLength ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
		for _, character := range segment {
			if unicode.IsControl(character) {
				return false
			}
		}
	}
	return true
}
