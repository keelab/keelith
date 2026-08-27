package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/secret"
	"github.com/keelab/keelith/transport/tlsconfig"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const maxsaslCredentialsBytes = 64 * 1024

// saslMechanism identifies one explicitly supported Kafka authentication
// mechanism.
type saslMechanism string

const (
	// saslPlain uses RFC 4616 PLAIN over mandatory TLS.
	saslPlain saslMechanism = "plain"
	// saslSCRAMSHA256 uses SCRAM-SHA-256.
	saslSCRAMSHA256 saslMechanism = "scram-sha-256"
	// saslSCRAMSHA512 uses SCRAM-SHA-512.
	saslSCRAMSHA512 saslMechanism = "scram-sha-512"
)

// TLSRuntimeConfig defines Kafka server trust and optional mtls material.
// BundleReference points to one atomic tlsconfig json secret.
type TLSRuntimeConfig struct {
	Enabled         bool   `config:"enabled"`
	BundleReference string `config:"bundleReference"`
	ServerName      string `config:"serverName"`
	MutualTLS       bool   `config:"mutualTLS"`
}

// saslRuntimeConfig defines one secret-backed authentication mechanism.
type saslRuntimeConfig struct {
	Mechanism            saslMechanism `config:"mechanism"`
	CredentialsReference string        `config:"credentialsReference"`
}

// ClientSecurityConfig is shared by generated Kafka producers and consumers.
type ClientSecurityConfig struct {
	TLS           TLSRuntimeConfig  `config:"tls"`
	AllowInsecure bool              `config:"allowInsecure"`
	sasl          saslRuntimeConfig `config:"sasl"`
}

// SecretManager resolves credentials and watches atomic tls replacements.
type SecretManager interface {
	tlsconfig.SecretManager
}

type clientSecurity struct {
	options       []kgo.Opt
	tlsWatcher    *tlsconfig.SecretWatcher
	saslMechanism sasl.Mechanism
}

type saslCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// ValidateClientSecurityConfig validates transport, trust, authentication, and
// secret-reference invariants without resolving secret material.
func ValidateClientSecurityConfig(config ClientSecurityConfig) error {
	if config.TLS.Enabled == config.AllowInsecure {
		return fmt.Errorf(
			"%w: exactly one of tls or allow-insecure must be selected",
			ErrInvalidOption,
		)
	}
	if !validOptionalText(config.TLS.ServerName, 253) {
		return fmt.Errorf("%w: tls server name is invalid", ErrInvalidOption)
	}
	if config.TLS.BundleReference != "" {
		if !config.TLS.Enabled {
			return fmt.Errorf(
				"%w: tls bundle requires TLS",
				ErrInvalidOption,
			)
		}
		if _, err := secret.Parse(config.TLS.BundleReference); err != nil {
			return fmt.Errorf("%w: tls bundle reference: %w", ErrInvalidOption, err)
		}
	}
	if config.TLS.MutualTLS && config.TLS.BundleReference == "" {
		return fmt.Errorf(
			"%w: mtls requires an atomic tls bundle reference",
			ErrInvalidOption,
		)
	}
	if !config.TLS.Enabled &&
		(config.TLS.BundleReference != "" ||
			config.TLS.ServerName != "" ||
			config.TLS.MutualTLS) {
		return fmt.Errorf(
			"%w: disabled TLS contains active settings",
			ErrInvalidOption,
		)
	}
	switch config.sasl.Mechanism {
	case "":
		if config.sasl.CredentialsReference != "" {
			return fmt.Errorf(
				"%w: sasl credentials require a mechanism",
				ErrInvalidOption,
			)
		}
	case saslPlain, saslSCRAMSHA256, saslSCRAMSHA512:
		if !config.TLS.Enabled {
			return fmt.Errorf(
				"%w: sasl requires TLS",
				ErrInvalidOption,
			)
		}
		if _, err := secret.Parse(
			config.sasl.CredentialsReference,
		); err != nil {
			return fmt.Errorf(
				"%w: sasl credentials reference: %w",
				ErrInvalidOption,
				err,
			)
		}
	default:
		return fmt.Errorf(
			"%w: unsupported sasl mechanism %q",
			ErrInvalidOption,
			config.sasl.Mechanism,
		)
	}
	return nil
}

func newClientSecurity(
	config ClientSecurityConfig,
	manager SecretManager,
) (*clientSecurity, error) {
	if err := ValidateClientSecurityConfig(config); err != nil {
		return nil, err
	}
	result := &clientSecurity{options: make([]kgo.Opt, 0, 2)}
	if config.TLS.Enabled {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: config.TLS.ServerName,
		}
		if config.TLS.BundleReference != "" {
			if isNil(manager) {
				return nil, fmt.Errorf(
					"%w: tls bundle requires a secret manager",
					ErrInvalidOption,
				)
			}
			reference, err := secret.Parse(config.TLS.BundleReference)
			if err != nil {
				return nil, err
			}
			reloader := tlsconfig.NewEmpty()
			watcher, err := tlsconfig.NewSecretWatcher(
				manager,
				reference,
				reloader,
			)
			if err != nil {
				return nil, fmt.Errorf("kafka: tls watcher: %w", err)
			}
			tlsConfig, err = reloader.ClientConfig(
				tlsconfig.ClientOptions{
					ServerName: config.TLS.ServerName,
					MutualTLS:  config.TLS.MutualTLS,
				},
			)
			if err != nil {
				return nil, fmt.Errorf("kafka: tls client config: %w", err)
			}
			result.tlsWatcher = watcher
		}
		result.options = append(result.options, kgo.DialTLSConfig(tlsConfig))
	}
	if config.sasl.Mechanism != "" {
		if isNil(manager) {
			return nil, fmt.Errorf(
				"%w: sasl requires a secret manager",
				ErrInvalidOption,
			)
		}
		reference, err := secret.Parse(config.sasl.CredentialsReference)
		if err != nil {
			return nil, err
		}
		load := func(ctx context.Context) (saslCredentials, error) {
			return resolvesaslCredentials(ctx, manager, reference)
		}
		switch config.sasl.Mechanism {
		case saslPlain:
			result.saslMechanism = plain.Plain(
				func(ctx context.Context) (plain.Auth, error) {
					credentials, err := load(ctx)
					if err != nil {
						return plain.Auth{}, err
					}
					return plain.Auth{
						User: credentials.Username,
						Pass: credentials.Password,
					}, nil
				},
			)
		case saslSCRAMSHA256:
			result.saslMechanism = scram.Sha256(
				func(ctx context.Context) (scram.Auth, error) {
					credentials, err := load(ctx)
					if err != nil {
						return scram.Auth{}, err
					}
					return scram.Auth{
						User: credentials.Username,
						Pass: credentials.Password,
					}, nil
				},
			)
		case saslSCRAMSHA512:
			result.saslMechanism = scram.Sha512(
				func(ctx context.Context) (scram.Auth, error) {
					credentials, err := load(ctx)
					if err != nil {
						return scram.Auth{}, err
					}
					return scram.Auth{
						User: credentials.Username,
						Pass: credentials.Password,
					}, nil
				},
			)
		}
		result.options = append(
			result.options,
			kgo.SASL(result.saslMechanism),
		)
	}
	return result, nil
}

func (security *clientSecurity) Start(ctx context.Context) error {
	if security == nil || security.tlsWatcher == nil {
		return nil
	}
	return security.tlsWatcher.Start(ctx)
}

func (security *clientSecurity) Shutdown(ctx context.Context) error {
	if security == nil || security.tlsWatcher == nil {
		return nil
	}
	return security.tlsWatcher.Shutdown(ctx)
}

func resolvesaslCredentials(
	ctx context.Context,
	manager SecretManager,
	reference secret.Reference,
) (saslCredentials, error) {
	if ctx == nil || isNil(manager) {
		return saslCredentials{}, fmt.Errorf(
			"%w: sasl context or secret manager is nil",
			ErrInvalidOption,
		)
	}
	value, err := manager.Resolve(ctx, reference)
	if err != nil {
		return saslCredentials{}, fmt.Errorf(
			"kafka: resolve sasl credentials %s: %w",
			reference.String(),
			err,
		)
	}
	if value.Expired(time.Now()) {
		return saslCredentials{}, fmt.Errorf(
			"kafka: sasl credentials %s: %w",
			reference.String(),
			secret.ErrInvalidValue,
		)
	}
	content := value.Bytes()
	defer clear(content)
	if len(content) == 0 || len(content) > maxsaslCredentialsBytes {
		return saslCredentials{}, fmt.Errorf(
			"%w: sasl credentials exceed byte budget",
			ErrInvalidOption,
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var credentials saslCredentials
	if err := decoder.Decode(&credentials); err != nil {
		return saslCredentials{}, fmt.Errorf(
			"%w: decode sasl credentials json",
			ErrInvalidOption,
		)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return saslCredentials{}, fmt.Errorf(
			"%w: sasl credentials contain trailing json",
			ErrInvalidOption,
		)
	}
	if !validsaslUsername(credentials.Username) ||
		credentials.Password == "" ||
		len(credentials.Password) > 16*1024 ||
		!utf8.ValidString(credentials.Password) ||
		strings.ContainsRune(credentials.Password, '\x00') {
		return saslCredentials{}, fmt.Errorf(
			"%w: sasl credentials are invalid",
			ErrInvalidOption,
		)
	}
	return credentials, nil
}

func validsaslUsername(value string) bool {
	return value != "" &&
		len(value) <= 1024 &&
		utf8.ValidString(value) &&
		strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00') &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func validOptionalText(value string, maxBytes int) bool {
	return len(value) <= maxBytes &&
		utf8.ValidString(value) &&
		strings.TrimSpace(value) == value &&
		!strings.ContainsFunc(value, func(character rune) bool {
			return unicode.IsControl(character) || unicode.IsSpace(character)
		})
}
