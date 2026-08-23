// Package genericrpc provides a bounded, experimental Kitex Generic RPC
// client for gateways and diagnostic tools.
//
// The package deliberately supports only descriptor-driven unary Proto JSON.
// It does not expose Kitex generic values to Keelith core or business
// contracts.
package genericrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	dproto "github.com/cloudwego/dynamicgo/proto"
	"github.com/keelab/keelith/operation"
)

const (
	defaultMaxRequestBytes  = 1 * 1024 * 1024
	defaultMaxResponseBytes = 1 * 1024 * 1024
	maxPayloadBytes         = 16 * 1024 * 1024
	maxMainIDLBytes         = 1 * 1024 * 1024
	maxTotalIDLBytes        = 4 * 1024 * 1024
	maxIncludes             = 64
	maxMethods              = 128
	maxTokenBytes           = 256
	defaultConnectTimeout   = 3 * time.Second
	maxConnectTimeout       = 30 * time.Second
)

var (
	// ErrInvalidConfig reports an incomplete or unsafe generic client
	// configuration.
	ErrInvalidConfig = errors.New("kitex generic: invalid config")
	// ErrMethodNotAllowed reports a method outside the frozen allowlist.
	ErrMethodNotAllowed = errors.New("kitex generic: method is not allowed")
	// ErrRequestTooLarge reports a request above its configured budget.
	ErrRequestTooLarge = errors.New("kitex generic: request is too large")
	// ErrResponseTooLarge reports a response above its configured budget.
	ErrResponseTooLarge = errors.New("kitex generic: response is too large")
	// ErrInvalidJSON reports a payload outside the unary Proto JSON contract.
	ErrInvalidJSON = errors.New("kitex generic: invalid JSON object")
	// ErrInvalidResponse reports an unexpected codec response.
	ErrInvalidResponse = errors.New("kitex generic: invalid response")
	// ErrAlreadyStarted reports a repeated Start.
	ErrAlreadyStarted = errors.New("kitex generic: client already started")
	// ErrNotReady reports Invoke before Start.
	ErrNotReady = errors.New("kitex generic: client is not ready")
	// ErrClosed reports Start or Invoke after shutdown began.
	ErrClosed = errors.New("kitex generic: client is closed")
)

// Config freezes one Proto JSON generic client contract.
//
// Service and Methods must match MainContent. Address is a host:port bootstrap
// endpoint and is never included in runtime diagnostics. Plaintext transport
// requires AllowInsecure; otherwise TLS 1.2 or newer is used.
type Config struct {
	Name             string
	Dependencies     []string
	Service          string
	Methods          []string
	Address          string
	MainPath         string
	MainContent      string
	Includes         map[string]string
	ConnectTimeout   time.Duration
	MaxRequestBytes  int
	MaxResponseBytes int
	AllowInsecure    bool
	TLSConfig        *tls.Config
}

type validatedConfig struct {
	name             string
	dependencies     []string
	service          string
	methods          []string
	operations       map[string]operation.Operation
	address          string
	mainPath         string
	mainContent      string
	includes         map[string]string
	connectTimeout   time.Duration
	maxRequestBytes  int
	maxResponseBytes int
	encrypted        bool
	tlsConfig        *tls.Config
}

func validateConfig(
	ctx context.Context,
	config Config,
) (validatedConfig, error) {
	if ctx == nil {
		return validatedConfig{}, invalidConfig("context is nil")
	}
	if cause := context.Cause(ctx); cause != nil {
		return validatedConfig{}, cause
	}
	name := strings.TrimSpace(config.Name)
	if !validToken(name) || name != config.Name {
		return validatedConfig{}, invalidConfig("name is malformed")
	}
	service := strings.TrimSpace(config.Service)
	if !validToken(service) || service != config.Service {
		return validatedConfig{}, invalidConfig("service is malformed")
	}
	address, err := validateAddress(config.Address)
	if err != nil {
		return validatedConfig{}, err
	}
	mainPath, err := validateVirtualPath(config.MainPath)
	if err != nil {
		return validatedConfig{}, invalidConfig("main IDL path is malformed")
	}
	if len(config.MainContent) == 0 ||
		len(config.MainContent) > maxMainIDLBytes {
		return validatedConfig{}, invalidConfig(
			"main IDL is empty or exceeds its budget",
		)
	}
	includes, err := snapshotIncludes(
		mainPath,
		len(config.MainContent),
		config.Includes,
	)
	if err != nil {
		return validatedConfig{}, err
	}
	requestBudget, err := payloadBudget(
		config.MaxRequestBytes,
		defaultMaxRequestBytes,
		"request",
	)
	if err != nil {
		return validatedConfig{}, err
	}
	responseBudget, err := payloadBudget(
		config.MaxResponseBytes,
		defaultMaxResponseBytes,
		"response",
	)
	if err != nil {
		return validatedConfig{}, err
	}
	connectTimeout := config.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = defaultConnectTimeout
	}
	if connectTimeout < 10*time.Millisecond ||
		connectTimeout > maxConnectTimeout {
		return validatedConfig{}, invalidConfig(
			"connect timeout is outside the supported range",
		)
	}
	dependencies, err := validateDependencies(name, config.Dependencies)
	if err != nil {
		return validatedConfig{}, err
	}
	tlsConfig, encrypted, err := validateTLS(
		config.AllowInsecure,
		config.TLSConfig,
	)
	if err != nil {
		return validatedConfig{}, err
	}

	descriptor, err := parseProtoDescriptor(
		ctx,
		mainPath,
		config.MainContent,
		snapshotStringMap(includes),
	)
	if err != nil {
		return validatedConfig{}, fmt.Errorf(
			"%w: Proto descriptor cannot be parsed",
			ErrInvalidConfig,
		)
	}
	if descriptor == nil ||
		descriptor.IsCombinedServices() ||
		descriptor.Name() != service {
		return validatedConfig{}, invalidConfig(
			"Proto descriptor service does not match the configured service",
		)
	}
	methods, operations, err := validateMethods(
		service,
		config.Methods,
		descriptor,
	)
	if err != nil {
		return validatedConfig{}, err
	}
	return validatedConfig{
		name:             name,
		dependencies:     dependencies,
		service:          service,
		methods:          methods,
		operations:       operations,
		address:          address,
		mainPath:         mainPath,
		mainContent:      config.MainContent,
		includes:         includes,
		connectTimeout:   connectTimeout,
		maxRequestBytes:  requestBudget,
		maxResponseBytes: responseBudget,
		encrypted:        encrypted,
		tlsConfig:        tlsConfig,
	}, nil
}

func validateMethods(
	service string,
	configured []string,
	descriptor *dproto.ServiceDescriptor,
) ([]string, map[string]operation.Operation, error) {
	if len(configured) == 0 || len(configured) > maxMethods {
		return nil, nil, invalidConfig(
			"method allowlist is empty or exceeds its limit",
		)
	}
	methods := make([]string, 0, len(configured))
	operations := make(map[string]operation.Operation, len(configured))
	for _, raw := range configured {
		method := strings.TrimSpace(raw)
		if !validToken(method) || method != raw {
			return nil, nil, invalidConfig("method allowlist is malformed")
		}
		if _, duplicate := operations[method]; duplicate {
			return nil, nil, invalidConfig("method allowlist contains duplicates")
		}
		methodDescriptor := descriptor.LookupMethodByName(method)
		if methodDescriptor == nil {
			return nil, nil, invalidConfig(
				"method allowlist is not present in the Proto descriptor",
			)
		}
		if methodDescriptor.IsClientStreaming() ||
			methodDescriptor.IsServerStreaming() {
			return nil, nil, invalidConfig(
				"streaming methods are not supported",
			)
		}
		target, err := operation.New(
			"kitex",
			service,
			method,
			operation.KindUnary,
		)
		if err != nil {
			return nil, nil, invalidConfig(
				"method cannot form a stable operation",
			)
		}
		methods = append(methods, method)
		operations[method] = target
	}
	sort.Strings(methods)
	return methods, operations, nil
}

func validateAddress(raw string) (string, error) {
	address := strings.TrimSpace(raw)
	if address == "" || address != raw {
		return "", invalidConfig("address is malformed")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil || !validEndpointHost(host) {
		return "", invalidConfig("address is malformed")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", invalidConfig("address port is malformed")
	}
	return address, nil
}

func validEndpointHost(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	if zoneIndex := strings.LastIndexByte(host, '%'); zoneIndex >= 0 {
		address := host[:zoneIndex]
		zone := host[zoneIndex+1:]
		return net.ParseIP(address) != nil && validDNSLabel(zone)
	}
	if net.ParseIP(host) != nil {
		return true
	}
	if strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if !validDNSLabel(label) {
			return false
		}
	}
	return true
}

func validDNSLabel(label string) bool {
	if len(label) == 0 || len(label) > 63 {
		return false
	}
	for index := range len(label) {
		character := label[index]
		alphanumeric := character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9'
		if !alphanumeric &&
			(character != '-' || index == 0 || index == len(label)-1) {
			return false
		}
	}
	return true
}

func validateVirtualPath(raw string) (string, error) {
	if raw == "" ||
		len(raw) > maxTokenBytes ||
		strings.Contains(raw, "\\") ||
		strings.HasPrefix(raw, "/") ||
		path.Clean(raw) != raw ||
		raw == "." ||
		raw == ".." ||
		strings.HasPrefix(raw, "../") ||
		path.Ext(raw) != ".proto" {
		return "", ErrInvalidConfig
	}
	return raw, nil
}

func snapshotIncludes(
	mainPath string,
	mainBytes int,
	source map[string]string,
) (map[string]string, error) {
	if len(source) > maxIncludes {
		return nil, invalidConfig("IDL include count exceeds its limit")
	}
	result := make(map[string]string, len(source))
	total := len(mainPath) + mainBytes
	for rawPath, content := range source {
		includePath, err := validateVirtualPath(rawPath)
		if err != nil || includePath == mainPath {
			return nil, invalidConfig("IDL include path is malformed")
		}
		if len(content) == 0 || len(content) > maxMainIDLBytes {
			return nil, invalidConfig(
				"IDL include is empty or exceeds its budget",
			)
		}
		total += len(includePath) + len(content)
		if total > maxTotalIDLBytes {
			return nil, invalidConfig("total IDL content exceeds its budget")
		}
		result[includePath] = content
	}
	return result, nil
}

func payloadBudget(value, defaultValue int, kind string) (int, error) {
	if value == 0 {
		return defaultValue, nil
	}
	if value < 1 || value > maxPayloadBytes {
		return 0, invalidConfig(kind + " budget is outside the supported range")
	}
	return value, nil
}

func validateDependencies(
	name string,
	source []string,
) ([]string, error) {
	result := make([]string, 0, len(source))
	seen := make(map[string]struct{}, len(source))
	for _, raw := range source {
		dependency := strings.TrimSpace(raw)
		if !validToken(dependency) ||
			dependency != raw ||
			dependency == name {
			return nil, invalidConfig("component dependency is malformed")
		}
		if _, duplicate := seen[dependency]; duplicate {
			return nil, invalidConfig("component dependency is duplicated")
		}
		seen[dependency] = struct{}{}
		result = append(result, dependency)
	}
	return result, nil
}

func validateTLS(
	allowInsecure bool,
	source *tls.Config,
) (*tls.Config, bool, error) {
	if allowInsecure {
		if source != nil {
			return nil, false, invalidConfig(
				"TLS config conflicts with insecure transport",
			)
		}
		return nil, false, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if source != nil {
		config = source.Clone()
		if config.InsecureSkipVerify {
			return nil, false, invalidConfig(
				"TLS certificate verification cannot be disabled",
			)
		}
		if config.MinVersion == 0 {
			config.MinVersion = tls.VersionTLS12
		}
		if config.MinVersion < tls.VersionTLS12 {
			return nil, false, invalidConfig(
				"TLS minimum version must be TLS 1.2 or newer",
			)
		}
	}
	return config, true, nil
}

func validToken(value string) bool {
	if value == "" ||
		len(value) > maxTokenBytes ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return false
		}
	}
	return true
}

func snapshotStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func invalidConfig(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, reason)
}
