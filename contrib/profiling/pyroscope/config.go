// Package pyroscope integrates Grafana Pyroscope continuous profiling with
// Keelith lifecycle, Secret, and Ops contracts.
package pyroscope

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/keelab/keelith/secret"
)

const (
	defaultUploadRate     = 15 * time.Second
	minimumUploadRate     = 5 * time.Second
	maximumUploadRate     = 5 * time.Minute
	defaultRequestTimeout = 30 * time.Second
	minimumRequestTimeout = time.Second
	maximumRequestTimeout = 2 * time.Minute
	maximumApplicationLen = 128
	maximumTenantLen      = 256
	maximumUserLen        = 256
	maximumTags           = 16
	maximumTagKeyLen      = 63
	maximumTagValueLen    = 256
)

var (
	// ErrInvalidConfig reports malformed or unsafe continuous profiling settings.
	ErrInvalidConfig = errors.New("pyroscope profiling: invalid config")
	// ErrCPUConflict reports ownership by another process-wide CPU profiler.
	ErrCPUConflict = errors.New("pyroscope profiling: CPU profiler is busy")
	// ErrAlreadyStarted reports a second start while the runtime is active.
	ErrAlreadyStarted = errors.New("pyroscope profiling: already started")
	// ErrStopped reports an attempt to restart a stopped runtime.
	ErrStopped = errors.New("pyroscope profiling: stopped")
)

// ProfileType is one bounded Pyroscope profile family.
type ProfileType string

const (
	// ProfileCPU enables CPU profiling.
	ProfileCPU ProfileType = "cpu"
	// ProfileInuseObjects enables in-use object heap profiling.
	ProfileInuseObjects ProfileType = "inuse_objects"
	// ProfileAllocObjects enables allocation object profiling.
	ProfileAllocObjects ProfileType = "alloc_objects"
	// ProfileInuseSpace enables in-use heap space profiling.
	ProfileInuseSpace ProfileType = "inuse_space"
	// ProfileAllocSpace enables allocation space profiling.
	ProfileAllocSpace ProfileType = "alloc_space"
	// ProfileGoroutines enables goroutine profiling.
	ProfileGoroutines ProfileType = "goroutines"
)

var allowedProfileTypes = map[ProfileType]struct{}{ //nolint:gochecknoglobals
	ProfileCPU:          {},
	ProfileInuseObjects: {},
	ProfileAllocObjects: {},
	ProfileInuseSpace:   {},
	ProfileAllocSpace:   {},
	ProfileGoroutines:   {},
}

// Config contains only construction-time, low-cardinality settings. Passwords
// are Secret references rather than values.
type Config struct {
	ApplicationName   string            `config:"application_name"`
	ServerAddress     string            `config:"server_address"`
	TenantID          string            `config:"tenant_id"`
	BasicAuthUser     string            `config:"basic_auth_user"`
	PasswordReference string            `config:"password_reference"`
	WatchPassword     bool              `config:"watch_password"`
	UploadRate        time.Duration     `config:"upload_rate"`
	RequestTimeout    time.Duration     `config:"request_timeout"`
	AllowInsecureHTTP bool              `config:"allow_insecure_http"`
	DisableGCRuns     bool              `config:"disable_gc_runs"`
	ProfileTypes      []ProfileType     `config:"profile_types"`
	Tags              map[string]string `config:"tags"`
}

// NormalizeConfig applies resource bounds and snapshots maps and slices.
func NormalizeConfig(input Config) (Config, error) {
	config := input
	if config.UploadRate == 0 {
		config.UploadRate = defaultUploadRate
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultRequestTimeout
	}
	if len(config.ProfileTypes) == 0 {
		config.ProfileTypes = []ProfileType{
			ProfileCPU,
			ProfileAllocObjects,
			ProfileAllocSpace,
			ProfileInuseObjects,
			ProfileInuseSpace,
		}
	} else {
		config.ProfileTypes = append([]ProfileType(nil), config.ProfileTypes...)
	}
	config.Tags = cloneTags(config.Tags)
	if err := validateNormalized(config); err != nil {
		return Config{}, err
	}
	return config, nil
}

// ValidateConfig validates a continuous profiling configuration.
func ValidateConfig(config Config) error {
	_, err := NormalizeConfig(config)
	return err
}

// UsesCPU reports whether the normalized configuration owns the process CPU
// profiler. Invalid configurations return false.
func UsesCPU(config Config) bool {
	normalized, err := NormalizeConfig(config)
	if err != nil {
		return false
	}
	for _, profileType := range normalized.ProfileTypes {
		if profileType == ProfileCPU {
			return true
		}
	}
	return false
}

func validateNormalized(config Config) error {
	if !validApplicationName(config.ApplicationName) {
		return fmt.Errorf("%w: application name", ErrInvalidConfig)
	}
	parsed, err := url.Parse(config.ServerAddress)
	if err != nil || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%w: server address", ErrInvalidConfig)
	}
	switch parsed.Scheme {
	case "https":
	case "http":
		if !config.AllowInsecureHTTP {
			return fmt.Errorf("%w: insecure http is not allowed", ErrInvalidConfig)
		}
	default:
		return fmt.Errorf("%w: server address scheme", ErrInvalidConfig)
	}
	if !validBoundedValue(config.TenantID, maximumTenantLen) ||
		!validBoundedValue(config.BasicAuthUser, maximumUserLen) {
		return fmt.Errorf("%w: tenant or basic auth user", ErrInvalidConfig)
	}
	if (config.BasicAuthUser == "") != (config.PasswordReference == "") {
		return fmt.Errorf("%w: incomplete basic authentication", ErrInvalidConfig)
	}
	if config.PasswordReference != "" {
		if _, err := secret.Parse(config.PasswordReference); err != nil {
			return fmt.Errorf("%w: password reference", ErrInvalidConfig)
		}
	}
	if config.WatchPassword && config.PasswordReference == "" {
		return fmt.Errorf(
			"%w: password watch requires basic authentication",
			ErrInvalidConfig,
		)
	}
	if config.UploadRate < minimumUploadRate ||
		config.UploadRate > maximumUploadRate ||
		config.RequestTimeout < minimumRequestTimeout ||
		config.RequestTimeout > maximumRequestTimeout {
		return fmt.Errorf("%w: upload or request duration", ErrInvalidConfig)
	}
	if err := validateProfileTypes(config.ProfileTypes); err != nil {
		return err
	}
	if err := validateTags(config.Tags); err != nil {
		return err
	}
	return nil
}

func validateProfileTypes(profileTypes []ProfileType) error {
	seen := make(map[ProfileType]struct{}, len(profileTypes))
	for _, profileType := range profileTypes {
		if _, allowed := allowedProfileTypes[profileType]; !allowed {
			return fmt.Errorf("%w: profile type %q", ErrInvalidConfig, profileType)
		}
		if _, duplicate := seen[profileType]; duplicate {
			return fmt.Errorf("%w: duplicate profile type", ErrInvalidConfig)
		}
		seen[profileType] = struct{}{}
	}
	return nil
}

func validateTags(tags map[string]string) error {
	if len(tags) > maximumTags {
		return fmt.Errorf("%w: too many tags", ErrInvalidConfig)
	}
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := tags[key]
		if !validTagKey(key) || !validBoundedValue(value, maximumTagValueLen) {
			return fmt.Errorf("%w: tag %q", ErrInvalidConfig, key)
		}
	}
	return nil
}

func validApplicationName(value string) bool {
	if value == "" || len(value) > maximumApplicationLen {
		return false
	}
	for _, character := range value {
		valid := (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '_' || character == '-' || character == '.' ||
			character == '/'
		if !valid {
			return false
		}
	}
	return true
}

func validTagKey(value string) bool {
	if value == "" || len(value) > maximumTagKeyLen {
		return false
	}
	for _, character := range value {
		valid := (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '_' || character == '.'
		if !valid {
			return false
		}
	}
	return true
}

func validBoundedValue(value string, maximum int) bool {
	if len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func cloneTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	result := make(map[string]string, len(tags))
	for key, value := range tags {
		result[key] = value
	}
	return result
}
