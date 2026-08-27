// Package nacos provides shared, secret-safe construction for nacos SDK
// clients used by Keelith config and registry adapters.
package nacos

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	keelithconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/secret"
)

const (
	defaultPort          = 8848
	defaultTimeout       = 10 * time.Second
	defaultBeatInterval  = 5 * time.Second
	defaultUpdateThreads = 20
	maxServers           = 32
	maxTextBytes         = 512
)

var (
	// ErrInvalidConfig reports malformed nacos topology, storage, TLS, or
	// secret-reference configuration.
	ErrInvalidConfig = errors.New("nacos runtime: invalid config")
)

// Server identifies one nacos server without embedding credentials.
type Server struct {
	Scheme      string `config:"scheme"`
	Address     string `config:"address"`
	Port        uint64 `config:"port"`
	GRPCPort    uint64 `config:"grpc_port"`
	ContextPath string `config:"context_path"`
}

// TLSConfig contains SDK TLS file references. Trust-all mode is intentionally
// not represented.
type TLSConfig struct {
	Enabled    bool   `config:"enabled"`
	CAFile     string `config:"ca_file"`
	CertFile   string `config:"cert_file"`
	KeyFile    string `config:"key_file"`
	ServerName string `config:"server_name"`
}

// Config contains construction-time nacos SDK settings. Password is always a
// secret:// reference and never plaintext configuration.
type Config struct {
	Servers           []Server      `config:"servers"`
	Namespace         string        `config:"namespace"`
	AppName           string        `config:"app_name"`
	Username          string        `config:"username"`
	PasswordReference string        `config:"password_reference"`
	Timeout           time.Duration `config:"timeout"`
	BeatInterval      time.Duration `config:"beat_interval"`
	CacheDirectory    string        `config:"cache_directory"`
	LogDirectory      string        `config:"log_directory"`
	LogLevel          string        `config:"log_level"`
	UpdateThreads     int           `config:"update_threads"`
	UseSDKSnapshot    bool          `config:"use_sdk_snapshot"`
	UpdateCacheEmpty  bool          `config:"update_cache_when_empty"`
	AppendLogToStdout bool          `config:"append_log_to_stdout"`
	TLS               TLSConfig     `config:"tls"`
}

// SecretResolver resolves secret references without exposing provider
// implementation details.
type SecretResolver interface {
	Resolve(context.Context, secret.Reference) (secret.Value, error)
}

// Description is a value-free construction snapshot.
type Description struct {
	ServerCount   int
	NamespaceSet  bool
	Authenticated bool
	TLS           bool
	SDKSnapshot   bool
	Timeout       time.Duration
	BeatInterval  time.Duration
	UpdateThreads int
	LogToStdout   bool
}

// NewConfigBinding creates a strict typed nacos client configuration binding.
// All fields are construction-time and require rebuilding SDK clients.
func NewConfigBinding(
	name string,
	path string,
	options ...keelithconfig.ComponentOption[Config],
) (*keelithconfig.Component[Config], error) {
	all := make([]keelithconfig.ComponentOption[Config], 0, len(options)+1)
	all = append(all, keelithconfig.WithComponentValidator(ValidateConfig))
	all = append(all, options...)
	return keelithconfig.NewComponent[Config](name, path, all...)
}

// NormalizeConfig applies stable defaults, copies slices, and validates the
// complete construction contract.
func NormalizeConfig(input Config) (Config, error) {
	config := input
	config.Servers = append([]Server(nil), input.Servers...)
	config.Namespace = strings.TrimSpace(config.Namespace)
	config.AppName = strings.TrimSpace(config.AppName)
	config.Username = strings.TrimSpace(config.Username)
	config.PasswordReference = strings.TrimSpace(config.PasswordReference)
	config.CacheDirectory = filepath.Clean(
		strings.TrimSpace(config.CacheDirectory),
	)
	config.LogDirectory = filepath.Clean(
		strings.TrimSpace(config.LogDirectory),
	)
	config.LogLevel = strings.ToLower(strings.TrimSpace(config.LogLevel))
	config.TLS.CAFile = cleanOptionalPath(config.TLS.CAFile)
	config.TLS.CertFile = cleanOptionalPath(config.TLS.CertFile)
	config.TLS.KeyFile = cleanOptionalPath(config.TLS.KeyFile)
	config.TLS.ServerName = strings.TrimSpace(config.TLS.ServerName)
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	if config.BeatInterval == 0 {
		config.BeatInterval = defaultBeatInterval
	}
	if config.UpdateThreads == 0 {
		config.UpdateThreads = defaultUpdateThreads
	}
	if config.LogLevel == "" {
		config.LogLevel = "info"
	}
	for index := range config.Servers {
		server := &config.Servers[index]
		server.Scheme = strings.ToLower(strings.TrimSpace(server.Scheme))
		server.Address = strings.TrimSpace(server.Address)
		server.ContextPath = strings.TrimSpace(server.ContextPath)
		if server.Scheme == "" {
			server.Scheme = "http"
		}
		if server.Port == 0 {
			server.Port = defaultPort
		}
		if server.ContextPath == "" {
			server.ContextPath = "/nacos"
		}
	}
	if err := validateNormalized(config); err != nil {
		return Config{}, err
	}
	return config, nil
}

// ValidateConfig validates the normalized nacos client contract.
func ValidateConfig(config Config) error {
	_, err := NormalizeConfig(config)
	return err
}

// Describe returns a value-free diagnostic description.
func Describe(config Config) (Description, error) {
	normalized, err := NormalizeConfig(config)
	if err != nil {
		return Description{}, err
	}
	return Description{
		ServerCount:   len(normalized.Servers),
		NamespaceSet:  normalized.Namespace != "",
		Authenticated: normalized.PasswordReference != "",
		TLS:           normalized.TLS.Enabled,
		SDKSnapshot:   normalized.UseSDKSnapshot,
		Timeout:       normalized.Timeout,
		BeatInterval:  normalized.BeatInterval,
		UpdateThreads: normalized.UpdateThreads,
		LogToStdout:   normalized.AppendLogToStdout,
	}, nil
}

func validateNormalized(config Config) error {
	if len(config.Servers) == 0 || len(config.Servers) > maxServers {
		return invalidConfig("server count is outside 1..%d", maxServers)
	}
	if !validText(config.AppName, true) {
		return invalidConfig("app name is invalid")
	}
	if !validText(config.Namespace, false) ||
		!validText(config.Username, false) {
		return invalidConfig("namespace or username is invalid")
	}
	if (config.Username == "") != (config.PasswordReference == "") {
		return invalidConfig(
			"username and passwordRef must be configured together",
		)
	}
	if config.PasswordReference != "" {
		if _, err := secret.Parse(config.PasswordReference); err != nil {
			return invalidConfig("passwordRef: %v", err)
		}
	}
	if config.Timeout < 100*time.Millisecond ||
		config.Timeout > 2*time.Minute ||
		config.BeatInterval < time.Second ||
		config.BeatInterval > 5*time.Minute ||
		config.UpdateThreads < 1 ||
		config.UpdateThreads > 256 {
		return invalidConfig("timeout, beat interval, or update threads is invalid")
	}
	if err := validateDirectory(config.CacheDirectory, "cache"); err != nil {
		return err
	}
	if err := validateDirectory(config.LogDirectory, "log"); err != nil {
		return err
	}
	switch config.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return invalidConfig("log level %q is unsupported", config.LogLevel)
	}
	seen := make(map[string]struct{}, len(config.Servers))
	for index, server := range config.Servers {
		if err := validateServer(server); err != nil {
			return invalidConfig("server %d: %v", index, err)
		}
		key := fmt.Sprintf(
			"%s://%s:%d%s",
			server.Scheme,
			strings.ToLower(server.Address),
			server.Port,
			server.ContextPath,
		)
		if _, duplicate := seen[key]; duplicate {
			return invalidConfig("server %d is duplicated", index)
		}
		seen[key] = struct{}{}
		if config.TLS.Enabled && server.Scheme != "https" {
			return invalidConfig(
				"server %d must use https when TLS is enabled",
				index,
			)
		}
	}
	return validateTLS(config.TLS)
}

func validateServer(server Server) error {
	if server.Scheme != "http" && server.Scheme != "https" {
		return fmt.Errorf("scheme %q is unsupported", server.Scheme)
	}
	if !validHost(server.Address) {
		return fmt.Errorf("address %q is invalid", server.Address)
	}
	if server.Port == 0 || server.Port > 65535 ||
		server.GRPCPort > 65535 {
		return errors.New("port is outside 1..65535")
	}
	if !strings.HasPrefix(server.ContextPath, "/") ||
		strings.Contains(server.ContextPath, "..") ||
		strings.ContainsAny(server.ContextPath, "?#\r\n\x00") {
		return fmt.Errorf("context path %q is invalid", server.ContextPath)
	}
	return nil
}

func validateTLS(config TLSConfig) error {
	filesSet := config.CAFile != "" ||
		config.CertFile != "" ||
		config.KeyFile != "" ||
		config.ServerName != ""
	if !config.Enabled {
		if filesSet {
			return invalidConfig("tls material is set while TLS is disabled")
		}
		return nil
	}
	if config.CAFile == "" || !filepath.IsAbs(config.CAFile) {
		return invalidConfig("TLS caFile must be an absolute path")
	}
	if (config.CertFile == "") != (config.KeyFile == "") {
		return invalidConfig("TLS certFile and keyFile must be configured together")
	}
	for name, value := range map[string]string{
		"certFile": config.CertFile,
		"keyFile":  config.KeyFile,
	} {
		if value != "" && !filepath.IsAbs(value) {
			return invalidConfig("TLS %s must be an absolute path", name)
		}
	}
	if !validText(config.ServerName, false) {
		return invalidConfig("tls serverName is invalid")
	}
	return nil
}

func validateDirectory(path string, name string) error {
	if path == "" || path == "." || !filepath.IsAbs(path) {
		return invalidConfig("%s directory must be absolute", name)
	}
	return nil
}

func validHost(value string) bool {
	if value == "" || len(value) > 253 ||
		strings.ContainsAny(value, "/\\:@ \t\r\n\x00") {
		return false
	}
	if net.ParseIP(value) != nil {
		return true
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 ||
			label[0] == '-' ||
			label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if character >= 'a' && character <= 'z' ||
				character >= 'A' && character <= 'Z' ||
				character >= '0' && character <= '9' ||
				character == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func validText(value string, required bool) bool {
	if required && value == "" {
		return false
	}
	return len(value) <= maxTextBytes &&
		!strings.ContainsAny(value, "\r\n\x00")
}

func cleanOptionalPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return filepath.Clean(value)
}

func invalidConfig(format string, arguments ...any) error {
	return fmt.Errorf(
		"%w: %s",
		ErrInvalidConfig,
		fmt.Sprintf(format, arguments...),
	)
}
