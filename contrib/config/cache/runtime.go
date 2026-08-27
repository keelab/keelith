package cache

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/config"
	"github.com/keelab/keelith/secret"
)

const (
	minimumRuntimettl      = time.Minute
	maximumRuntimettl      = 7 * 24 * time.Hour
	minimumRuntimeMaxBytes = 1024
	defaultRuntimeMaxBytes = 2 << 20
	maximumRuntimeMaxBytes = 16 << 20
	maximumRuntimePath     = 4096
)

// RuntimeConfig defines an optional encrypted LKG wrapper for one remote
// configuration Source. KeyReference must use the file Secret provider.
type RuntimeConfig struct {
	Enabled      bool          `config:"enabled"`
	Path         string        `config:"path"`
	TTL          time.Duration `config:"ttl"`
	MaxBytes     int64         `config:"max_bytes"`
	KeyReference string        `config:"key_reference"`
}

// ManagedSource is an App-owned remote configuration Source.
type ManagedSource interface {
	config.Source
	Shutdown(context.Context) error
}

// SecretManager resolves the encryption key without exposing provider
// implementation types.
type SecretManager interface {
	Resolve(context.Context, secret.Reference) (secret.Value, error)
}

// ValidateRuntimeConfig checks a disabled zero value or a complete, bounded,
// Secret-backed LKG configuration.
func ValidateRuntimeConfig(runtime RuntimeConfig) error {
	runtime = normalizeRuntimeConfig(runtime)
	if !runtime.Enabled {
		if runtime.Path != "" || runtime.TTL != 0 || runtime.MaxBytes != 0 ||
			runtime.KeyReference != "" {
			return fmt.Errorf(
				"%w: disabled LKG contains active settings",
				ErrInvalidOption,
			)
		}
		return nil
	}
	if !validRuntimePath(runtime.Path) {
		return fmt.Errorf("%w: LKG path must be a clean absolute path", ErrInvalidOption)
	}
	if runtime.TTL < minimumRuntimettl || runtime.TTL > maximumRuntimettl {
		return fmt.Errorf("%w: LKG ttl must be between 1m and 168h", ErrInvalidOption)
	}
	maxBytes := runtime.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultRuntimeMaxBytes
	}
	if maxBytes < minimumRuntimeMaxBytes || maxBytes > maximumRuntimeMaxBytes {
		return fmt.Errorf(
			"%w: LKG max bytes must be between %d and %d",
			ErrInvalidOption,
			minimumRuntimeMaxBytes,
			maximumRuntimeMaxBytes,
		)
	}
	reference, err := secret.Parse(runtime.KeyReference)
	if err != nil || reference.Provider() != "file" {
		return fmt.Errorf(
			"%w: LKG key reference must use the file Secret provider",
			ErrInvalidOption,
		)
	}
	return nil
}

// OpenRuntime resolves one construction-time AES key, constructs the
// authenticated Store, and transfers upstream ownership to Source.
func OpenRuntime(
	ctx context.Context,
	upstream ManagedSource,
	runtime RuntimeConfig,
	manager SecretManager,
) (*Source, error) {
	if ctx == nil || isNil(upstream) {
		return nil, fmt.Errorf("%w: context and upstream are required", ErrInvalidOption)
	}
	fail := func(cause error) (*Source, error) {
		shutdownErr := upstream.Shutdown(context.WithoutCancel(ctx))
		return nil, errors.Join(cause, shutdownErr)
	}
	runtime = normalizeRuntimeConfig(runtime)
	if err := ValidateRuntimeConfig(runtime); err != nil {
		return fail(err)
	}
	if !runtime.Enabled {
		return fail(fmt.Errorf("%w: LKG runtime is disabled", ErrInvalidOption))
	}
	if isNil(manager) {
		return fail(fmt.Errorf("%w: Secret manager is required", ErrInvalidOption))
	}
	reference, _ := secret.Parse(runtime.KeyReference)
	value, err := manager.Resolve(ctx, reference)
	if err != nil {
		return fail(fmt.Errorf("config cache: resolve encryption key: %w", err))
	}
	if value.Expired(time.Now()) {
		return fail(fmt.Errorf("%w: encryption key is expired", ErrInvalidOption))
	}
	key := value.Bytes()
	trimmedKey := secret.TrimLineBreaks(key)
	if len(trimmedKey) != 16 && len(trimmedKey) != 24 && len(trimmedKey) != 32 {
		clear(key)
		return fail(fmt.Errorf("%w: encryption key must contain 16, 24, or 32 bytes", ErrInvalidOption))
	}
	cipher, err := NewAESGCM(trimmedKey)
	clear(key)
	if err != nil {
		return fail(err)
	}
	maxBytes := runtime.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultRuntimeMaxBytes
	}
	store, err := NewStore(StoreConfig{
		Path: runtime.Path, TTL: runtime.TTL,
		MaxBytes: maxBytes, Cipher: cipher,
	})
	if err != nil {
		return fail(err)
	}
	source, err := Wrap(upstream, store)
	if err != nil {
		return fail(err)
	}
	return source, nil
}

func normalizeRuntimeConfig(runtime RuntimeConfig) RuntimeConfig {
	runtime.Path = strings.TrimSpace(runtime.Path)
	runtime.KeyReference = strings.TrimSpace(runtime.KeyReference)
	return runtime
}

func validRuntimePath(path string) bool {
	if path == "" || len(path) > maximumRuntimePath || !utf8.ValidString(path) ||
		!filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	for _, character := range path {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

var _ ManagedSource = (*Source)(nil)
