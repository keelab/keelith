package sql

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"

	keelithconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/secret"
)

// ConnectionConfig combines secret-safe connection construction with the
// reloadable pool configuration.
type ConnectionConfig struct {
	Driver       string `config:"driver"`
	DSNReference string `config:"dsnReference"`
	Pool         Config `config:"pool"`
}

// SecretResolver resolves a DSN reference without storing secret material in
// a Config Snapshot.
type SecretResolver interface {
	Resolve(context.Context, secret.Reference) (secret.Value, error)
}

// ValidateConnectionConfig validates construction identity, Secret reference,
// ownership, and pool limits.
func ValidateConnectionConfig(config ConnectionConfig) error {
	if !validDriverName(config.Driver) {
		return fmt.Errorf("%w: SQL driver name is invalid", ErrInvalidOption)
	}
	if _, err := secret.Parse(config.DSNReference); err != nil {
		return fmt.Errorf(
			"%w: SQL DSN reference is invalid",
			ErrInvalidOption,
		)
	}
	if !config.Pool.Owns {
		return fmt.Errorf(
			"%w: configured SQL pool must be application-owned",
			ErrInvalidOption,
		)
	}
	return ValidateConfig(config.Pool)
}

// OpenConfigured resolves the DSN and creates an owned Database. Secret bytes
// are cleared after database/sql has consumed the construction string.
func OpenConfigured(
	ctx context.Context,
	config ConnectionConfig,
	resolver SecretResolver,
	optionList ...Option,
) (*Database, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := ValidateConnectionConfig(config); err != nil {
		return nil, err
	}
	if isNil(resolver) {
		return nil, fmt.Errorf("%w: secret resolver is nil", ErrInvalidOption)
	}
	reference, _ := secret.Parse(config.DSNReference)
	value, err := resolver.Resolve(ctx, reference)
	if err != nil {
		return nil, fmt.Errorf(
			"sql data: resolve DSN %s: %w",
			reference.String(),
			err,
		)
	}
	if err := value.Validate(); err != nil || value.Expired(time.Now()) {
		return nil, fmt.Errorf(
			"%w: resolved SQL DSN is invalid",
			ErrInvalidOption,
		)
	}
	content := value.Bytes()
	defer clear(content)
	dataSourceName := string(secret.TrimLineBreaks(content))
	database, err := Open(
		config.Driver,
		dataSourceName,
		config.Pool,
		optionList...,
	)
	if err != nil {
		// Driver construction errors may echo their DSN. Preserve a stable,
		// secret-free boundary instead of returning the SDK text.
		return nil, fmt.Errorf(
			"%w: configured SQL driver %q could not be opened",
			ErrInvalidOption,
			config.Driver,
		)
	}
	database.driverName = config.Driver
	database.dsnReference = config.DSNReference
	return database, nil
}

// ApplyConnectionConfig hot-applies pool settings while requiring restart for
// driver or DSN reference changes.
func (database *Database) ApplyConnectionConfig(
	ctx context.Context,
	config ConnectionConfig,
) error {
	if database == nil {
		return fmt.Errorf("%w: database is nil", ErrInvalidOption)
	}
	if err := ValidateConnectionConfig(config); err != nil {
		return err
	}
	if database.driverName == "" ||
		database.driverName != config.Driver ||
		database.dsnReference != config.DSNReference {
		return fmt.Errorf(
			"%w: SQL driver or DSN reference changed",
			keelithconfig.ErrRestartRequired,
		)
	}
	return database.ApplyConfig(ctx, config.Pool)
}

func validDriverName(name string) bool {
	if name == "" ||
		len(name) > 128 ||
		strings.TrimSpace(name) != name {
		return false
	}
	for _, character := range name {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return false
		}
	}
	return true
}
