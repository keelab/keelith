package yapi

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

const (
	defaultRequestTimeout   = 10 * time.Second
	defaultMaxDocumentBytes = 8 * 1024 * 1024
	defaultMaxResponseBytes = 64 * 1024
	defaultMaxTokenBytes    = 8 * 1024

	maxDocumentBytes = 32 * 1024 * 1024
	maxResponseBytes = 1024 * 1024
)

var (
	// ErrInvalidOption reports an unsafe endpoint, credential, merge mode, or
	// resource budget.
	ErrInvalidOption = errors.New("docs/yapi: invalid option")
	// ErrInvalidDocument reports input that is not a supported OpenAPI or
	// Swagger json document.
	ErrInvalidDocument = errors.New("docs/yapi: invalid OpenAPI document")
	// ErrTooLarge reports a document or response beyond its configured budget.
	ErrTooLarge = errors.New("docs/yapi: payload is too large")
	// ErrUnauthorized reports a rejected YApi project token without exposing it.
	ErrUnauthorized = errors.New("docs/yapi: unauthorized")
	// ErrUnavailable reports throttling, timeout, or a temporary server failure.
	ErrUnavailable = errors.New("docs/yapi: unavailable")
	// ErrRejected reports a syntactically valid request rejected by YApi.
	ErrRejected = errors.New("docs/yapi: import rejected")
	// ErrInvalidResponse reports a malformed, redirected, or otherwise unusable
	// YApi response.
	ErrInvalidResponse = errors.New("docs/yapi: invalid response")
)

// MergeMode controls how YApi reconciles imported endpoints with existing
// project data.
type MergeMode string

const (
	// MergeNormal leaves existing endpoints unchanged.
	MergeNormal MergeMode = "normal"
	// MergeSmart preserves YApi-managed response details where supported.
	MergeSmart MergeMode = "good"
	// MergeOverwrite replaces existing endpoint data with the OpenAPI source.
	MergeOverwrite MergeMode = "merge"
)

// Valid reports whether the value is an official YApi import merge mode.
func (mode MergeMode) Valid() bool {
	return mode == MergeNormal || mode == MergeSmart || mode == MergeOverwrite
}

// Options configure one OpenAPI-to-YApi Reporter.
type Options struct {
	Endpoint              string        `config:"endpoint"`
	Merge                 MergeMode     `config:"merge"`
	RequestTimeout        time.Duration `config:"request_timeout"`
	MaxDocumentBytes      int           `config:"max_document_bytes"`
	MaxResponseBytes      int           `config:"max_response_bytes"`
	MaxTokenBytes         int           `config:"max_token_bytes"`
	AllowInsecureLoopback bool          `config:"allow_insecure_loopback"`
}

// NormalizeOptions applies bounded defaults and validates transport safety
// without resolving credentials or making a network request.
func NormalizeOptions(input Options) (Options, error) {
	options := input
	options.Endpoint = strings.TrimSpace(options.Endpoint)
	if options.Merge == "" {
		options.Merge = MergeNormal
	}
	if options.RequestTimeout == 0 {
		options.RequestTimeout = defaultRequestTimeout
	}
	if options.MaxDocumentBytes == 0 {
		options.MaxDocumentBytes = defaultMaxDocumentBytes
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = defaultMaxResponseBytes
	}
	if options.MaxTokenBytes == 0 {
		options.MaxTokenBytes = defaultMaxTokenBytes
	}
	if !options.Merge.Valid() ||
		options.RequestTimeout < 100*time.Millisecond ||
		options.RequestTimeout > time.Minute ||
		options.MaxDocumentBytes < 1 ||
		options.MaxDocumentBytes > maxDocumentBytes ||
		options.MaxResponseBytes < 1 ||
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
	return options, nil
}

// ValidateOptions validates Options without constructing a Reporter.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

func normalizeEndpoint(raw string, allowInsecure bool) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil ||
		parsed.Host == "" ||
		parsed.Opaque != "" ||
		parsed.User != nil ||
		parsed.ForceQuery ||
		parsed.RawQuery != "" ||
		parsed.RawPath != "" ||
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
