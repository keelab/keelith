// Package mcp exposes Keelith handlers as typed Model Context Protocol tools.
package mcp

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/middleware"
)

const (
	defaultMaxMessageBytes = int64(1 * 1024 * 1024)
	defaultMaxResultBytes  = int64(1 * 1024 * 1024)
	hardMaxPayloadBytes    = int64(16 * 1024 * 1024)
	maxImplementationBytes = 128
	maxTitleBytes          = 256
	maxDescriptionBytes    = 4 * 1024
	maxInstructionsBytes   = 16 * 1024
)

var (
	// ErrInvalidOption means server or transport options are unsafe or incomplete.
	ErrInvalidOption = errors.New("mcp transport: invalid option")
	// ErrInvalidTool means a tool identity, schema, or handler is invalid.
	ErrInvalidTool = errors.New("mcp transport: invalid tool")
	// ErrDuplicateTool means a tool name was registered more than once.
	ErrDuplicateTool = errors.New("mcp transport: duplicate tool")
	// ErrRegistryFrozen means registration was attempted after serving began.
	ErrRegistryFrozen = errors.New("mcp transport: registry is frozen")
	// ErrNilContext means a blocking transport received a nil context.
	ErrNilContext = errors.New("mcp transport: nil context")
	// ErrInvalidStream means a stdio-compatible reader or writer is nil.
	ErrInvalidStream = errors.New("mcp transport: invalid stream")
	// ErrMessageTooLarge means one MCP protocol message exceeded its budget.
	ErrMessageTooLarge = errors.New("mcp transport: message too large")
)

// Implementation is the stable server identity announced during MCP initialize.
type Implementation struct {
	Name    string
	Title   string
	Version string
}

// Options configures immutable server behavior.
type Options struct {
	Instructions    string
	Middleware      *middleware.Bundle
	PageSize        int
	MaxMessageBytes int64
	MaxResultBytes  int64
}

type normalizedOptions struct {
	implementation  Implementation
	instructions    string
	middleware      *middleware.Bundle
	pageSize        int
	maxMessageBytes int64
	maxResultBytes  int64
}

// HTTPOptions configures the stateless Streamable http handler.
type HTTPOptions struct {
	TrustedOrigins []string
	JSONResponse   bool
}

func normalizeOptions(
	implementation Implementation,
	options Options,
) (normalizedOptions, error) {
	implementation.Name = strings.TrimSpace(implementation.Name)
	implementation.Title = strings.TrimSpace(implementation.Title)
	implementation.Version = strings.TrimSpace(implementation.Version)
	if !validIdentifier(implementation.Name) ||
		len(implementation.Name) > maxImplementationBytes {
		return normalizedOptions{}, fmt.Errorf(
			"%w: implementation name is empty or malformed",
			ErrInvalidOption,
		)
	}
	if implementation.Version == "" ||
		!validText(implementation.Version, maxImplementationBytes, false) {
		return normalizedOptions{}, fmt.Errorf(
			"%w: implementation version is empty or malformed",
			ErrInvalidOption,
		)
	}
	if implementation.Title != "" &&
		!validText(implementation.Title, maxTitleBytes, false) {
		return normalizedOptions{}, fmt.Errorf(
			"%w: implementation title is malformed",
			ErrInvalidOption,
		)
	}
	if !validText(options.Instructions, maxInstructionsBytes, true) {
		return normalizedOptions{}, fmt.Errorf(
			"%w: instructions are malformed or oversized",
			ErrInvalidOption,
		)
	}
	if options.PageSize < 0 || options.PageSize > 1000 {
		return normalizedOptions{}, fmt.Errorf(
			"%w: page size must be between 0 and 1000",
			ErrInvalidOption,
		)
	}
	maxMessageBytes, err := normalizePayloadBudget(
		options.MaxMessageBytes,
		defaultMaxMessageBytes,
	)
	if err != nil {
		return normalizedOptions{}, fmt.Errorf(
			"%w: max message bytes: %w",
			ErrInvalidOption,
			err,
		)
	}
	maxResultBytes, err := normalizePayloadBudget(
		options.MaxResultBytes,
		defaultMaxResultBytes,
	)
	if err != nil {
		return normalizedOptions{}, fmt.Errorf(
			"%w: max result bytes: %w",
			ErrInvalidOption,
			err,
		)
	}
	return normalizedOptions{
		implementation:  implementation,
		instructions:    options.Instructions,
		middleware:      options.Middleware,
		pageSize:        options.PageSize,
		maxMessageBytes: maxMessageBytes,
		maxResultBytes:  maxResultBytes,
	}, nil
}

func normalizePayloadBudget(value, fallback int64) (int64, error) {
	if value == 0 {
		return fallback, nil
	}
	if value < 1 || value > hardMaxPayloadBytes {
		return 0, fmt.Errorf("must be between 1 and %d", hardMaxPayloadBytes)
	}
	return value, nil
}

func normalizeHTTPOptions(options HTTPOptions) (HTTPOptions, error) {
	snapshot := HTTPOptions{
		TrustedOrigins: append([]string(nil), options.TrustedOrigins...),
		JSONResponse:   options.JSONResponse,
	}
	seen := make(map[string]struct{}, len(snapshot.TrustedOrigins))
	for index, origin := range snapshot.TrustedOrigins {
		normalized, err := normalizeOrigin(origin)
		if err != nil {
			return HTTPOptions{}, fmt.Errorf(
				"%w: trusted origin %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
		if _, duplicate := seen[normalized]; duplicate {
			return HTTPOptions{}, fmt.Errorf(
				"%w: duplicate trusted origin",
				ErrInvalidOption,
			)
		}
		seen[normalized] = struct{}{}
		snapshot.TrustedOrigins[index] = normalized
	}
	return snapshot, nil
}

func normalizeOrigin(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" ||
		parsed.User != nil || parsed.Opaque != "" || parsed.ForceQuery ||
		parsed.RawPath != "" || parsed.RawQuery != "" ||
		parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", errors.New("origin must contain only scheme and authority")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return "", errors.New("origin scheme must be https or http")
	}
	parsed.Path = ""
	return parsed.String(), nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > maxImplementationBytes {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func validText(value string, maxBytes int, allowLines bool) bool {
	if len(value) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if !unicode.IsControl(character) {
			continue
		}
		if allowLines &&
			(character == '\n' || character == '\r' || character == '\t') {
			continue
		}
		return false
	}
	return true
}
