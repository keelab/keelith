// Package tlsconfig provides shared TLS and mTLS profiles for transports.
package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/keelab/keelith/secret"
)

var (
	// ErrInvalidMaterial reports an unusable certificate, key, or CA bundle.
	ErrInvalidMaterial = errors.New("tlsconfig: invalid material")
	// ErrUnavailable reports that required hot-reloaded material is absent.
	ErrUnavailable = errors.New("tlsconfig: material unavailable")
	// ErrInvalidOption reports an invalid TLS profile option.
	ErrInvalidOption = errors.New("tlsconfig: invalid option")
)

// Material is one atomic certificate/key/CA replacement.
type Material struct {
	CertificatePEM []byte
	PrivateKeyPEM  []byte
	CAPEM          []byte
}

type state struct {
	certificate *tls.Certificate
	roots       *x509.CertPool
}

// Reloader atomically publishes immutable TLS material.
type Reloader struct {
	current atomic.Pointer[state]

	mu             sync.Mutex
	requirements   materialRequirements
	generation     uint64
	nextSubscriber uint64
	subscribers    map[uint64]chan uint64
}

// UpdateSubscription observes successful atomic material replacements.
// Notifications are coalesced to the latest generation so a slow consumer
// cannot block TLS handshakes or SecretWatcher progress.
type UpdateSubscription struct {
	reloader  *Reloader
	id        uint64
	baseline  uint64
	updates   <-chan uint64
	closeOnce sync.Once
}

// Baseline returns the generation active when the subscription was created.
func (sub *UpdateSubscription) Baseline() uint64 {
	if sub == nil {
		return 0
	}
	return sub.baseline
}

// Updates returns a single-slot stream of successful material generations.
func (sub *UpdateSubscription) Updates() <-chan uint64 {
	if sub == nil {
		return nil
	}
	return sub.updates
}

// Close unregisters the subscription. It is safe to call repeatedly.
func (sub *UpdateSubscription) Close() {
	if sub == nil || sub.reloader == nil {
		return
	}
	sub.closeOnce.Do(func() {
		sub.reloader.removeSubscriber(sub.id)
	})
}

type materialRequirements uint8

const (
	requireCertificate materialRequirements = 1 << iota
	requireRoots
)

// New validates initial material and constructs a Reloader.
func New(material Material) (*Reloader, error) {
	reloader := &Reloader{}
	if err := reloader.Update(material); err != nil {
		return nil, err
	}
	return reloader, nil
}

// NewEmpty creates a Reloader that must be populated before handshakes begin.
//
// This is intended for a SecretWatcher component, which App starts before
// transport servers.
func NewEmpty() *Reloader {
	return &Reloader{}
}

// Update validates all material before atomically replacing the current set.
func (reloader *Reloader) Update(material Material) error {
	if reloader == nil {
		return fmt.Errorf("%w: reloader is nil", ErrInvalidOption)
	}
	next, err := parseMaterial(material)
	if err != nil {
		return err
	}
	reloader.mu.Lock()
	defer reloader.mu.Unlock()
	if err := validateRequirements(next, reloader.requirements); err != nil {
		return err
	}
	reloader.current.Store(next)
	reloader.generation++
	for _, updates := range reloader.subscribers {
		publishGeneration(updates, reloader.generation)
	}
	return nil
}

// Subscribe observes future successful material replacements without exposing
// certificates, keys, roots, file paths, or secret versions. The returned
// baseline lets connection owners distinguish the already-active material from
// subsequent replacements.
func (reloader *Reloader) Subscribe() (*UpdateSubscription, error) {
	if reloader == nil {
		return nil, fmt.Errorf("%w: reloader is nil", ErrInvalidOption)
	}
	reloader.mu.Lock()
	defer reloader.mu.Unlock()
	if reloader.subscribers == nil {
		reloader.subscribers = make(map[uint64]chan uint64)
	}
	reloader.nextSubscriber++
	id := reloader.nextSubscriber
	updates := make(chan uint64, 1)
	reloader.subscribers[id] = updates
	return &UpdateSubscription{
		reloader: reloader,
		id:       id,
		baseline: reloader.generation,
		updates:  updates,
	}, nil
}

// SubscribeUpdates implements secret.UpdateSource without exposing TLS
// material through the provider-neutral rotation contract.
func (reloader *Reloader) SubscribeUpdates() (secret.UpdateSubscription, error) {
	return reloader.Subscribe()
}

func (reloader *Reloader) removeSubscriber(id uint64) {
	reloader.mu.Lock()
	updates, exists := reloader.subscribers[id]
	if exists {
		delete(reloader.subscribers, id)
		close(updates)
	}
	reloader.mu.Unlock()
}

func publishGeneration(updates chan uint64, generation uint64) {
	select {
	case <-updates:
	default:
	}
	select {
	case updates <- generation:
	default:
	}
}

var _ secret.UpdateSource = (*Reloader)(nil)

// Ready reports whether any valid material has been published.
func (reloader *Reloader) Ready() bool {
	return reloader != nil && reloader.current.Load() != nil
}

// ServerOptions controls the server-side TLS profile.
type ServerOptions struct {
	MutualTLS  bool
	NextProtos []string
}

// ServerConfig returns a TLS 1.2+ server config with hot certificate reload.
func (reloader *Reloader) ServerConfig(
	options ServerOptions,
) (*tls.Config, error) {
	if reloader == nil {
		return nil, fmt.Errorf("%w: reloader is nil", ErrInvalidOption)
	}
	requirements := requireCertificate
	if options.MutualTLS {
		requirements |= requireRoots
	}
	if err := reloader.require(requirements); err != nil {
		return nil, err
	}
	nextProtos := normalizedProtocols(options.NextProtos)
	base := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: nextProtos,
		GetCertificate: func(
			*tls.ClientHelloInfo,
		) (*tls.Certificate, error) {
			current := reloader.current.Load()
			if current == nil || current.certificate == nil {
				return nil, ErrUnavailable
			}
			return current.certificate, nil
		},
	}
	if options.MutualTLS {
		base.ClientAuth = tls.RequireAndVerifyClientCert
		base.GetConfigForClient = func(
			*tls.ClientHelloInfo,
		) (*tls.Config, error) {
			current := reloader.current.Load()
			if current == nil ||
				current.certificate == nil ||
				current.roots == nil {
				return nil, ErrUnavailable
			}
			config := base.Clone()
			config.GetConfigForClient = nil
			config.ClientCAs = current.roots
			return config, nil
		}
	}
	return base, nil
}

// ClientOptions controls server identity and optional client authentication.
type ClientOptions struct {
	ServerName string
	MutualTLS  bool
	NextProtos []string
}

// ClientConfig returns a TLS 1.2+ client config with hot CA/certificate reload.
func (reloader *Reloader) ClientConfig(
	options ClientOptions,
) (*tls.Config, error) {
	if reloader == nil {
		return nil, fmt.Errorf("%w: reloader is nil", ErrInvalidOption)
	}
	requirements := requireRoots
	if options.MutualTLS {
		requirements |= requireCertificate
	}
	if err := reloader.require(requirements); err != nil {
		return nil, err
	}
	serverName := strings.TrimSpace(options.ServerName)
	config := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         serverName,
		NextProtos:         normalizedProtocols(options.NextProtos),
		InsecureSkipVerify: true, // Full dynamic verification is performed below.
		VerifyConnection: func(connection tls.ConnectionState) error {
			current := reloader.current.Load()
			if current == nil || current.roots == nil {
				return ErrUnavailable
			}
			if len(connection.PeerCertificates) == 0 {
				return fmt.Errorf("%w: peer sent no certificate", ErrUnavailable)
			}
			name := serverName
			if name == "" {
				name = connection.ServerName
			}
			if name == "" {
				return fmt.Errorf("%w: server name is empty", ErrInvalidOption)
			}
			intermediates := x509.NewCertPool()
			for _, certificate := range connection.PeerCertificates[1:] {
				intermediates.AddCert(certificate)
			}
			_, err := connection.PeerCertificates[0].Verify(x509.VerifyOptions{
				DNSName:       name,
				Roots:         current.roots,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			})
			if err != nil {
				return fmt.Errorf("tlsconfig: verify server: %w", err)
			}
			return nil
		},
	}
	if options.MutualTLS {
		config.GetClientCertificate = func(
			*tls.CertificateRequestInfo,
		) (*tls.Certificate, error) {
			current := reloader.current.Load()
			if current == nil || current.certificate == nil {
				return nil, ErrUnavailable
			}
			return current.certificate, nil
		}
	}
	return config, nil
}

func (reloader *Reloader) require(
	requirements materialRequirements,
) error {
	reloader.mu.Lock()
	defer reloader.mu.Unlock()
	combined := reloader.requirements | requirements
	if err := validateRequirements(
		reloader.current.Load(),
		combined,
	); err != nil {
		return err
	}
	reloader.requirements = combined
	return nil
}

func validateRequirements(
	current *state,
	requirements materialRequirements,
) error {
	// NewEmpty intentionally declares a profile before SecretWatcher publishes
	// the initial complete replacement.
	if current == nil {
		return nil
	}
	if requirements&requireCertificate != 0 &&
		current.certificate == nil {
		return fmt.Errorf(
			"%w: active TLS profile requires a certificate and private key",
			ErrInvalidMaterial,
		)
	}
	if requirements&requireRoots != 0 && current.roots == nil {
		return fmt.Errorf(
			"%w: active TLS profile requires a CA bundle",
			ErrInvalidMaterial,
		)
	}
	return nil
}

func parseMaterial(material Material) (*state, error) {
	hasCertificate := len(material.CertificatePEM) > 0 ||
		len(material.PrivateKeyPEM) > 0
	var certificate *tls.Certificate
	if hasCertificate {
		if len(material.CertificatePEM) == 0 ||
			len(material.PrivateKeyPEM) == 0 {
			return nil, fmt.Errorf(
				"%w: certificate and private key must be replaced together",
				ErrInvalidMaterial,
			)
		}
		parsed, err := tls.X509KeyPair(
			append([]byte(nil), material.CertificatePEM...),
			append([]byte(nil), material.PrivateKeyPEM...),
		)
		if err != nil {
			return nil, fmt.Errorf("%w: key pair: %w", ErrInvalidMaterial, err)
		}
		certificate = &parsed
	}

	var roots *x509.CertPool
	if len(material.CAPEM) > 0 {
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(material.CAPEM) {
			return nil, fmt.Errorf("%w: CA bundle", ErrInvalidMaterial)
		}
	}
	if certificate == nil && roots == nil {
		return nil, fmt.Errorf("%w: material is empty", ErrInvalidMaterial)
	}
	return &state{certificate: certificate, roots: roots}, nil
}

func normalizedProtocols(protocols []string) []string {
	if len(protocols) == 0 {
		return []string{"h2", "http/1.1"}
	}
	result := make([]string, 0, len(protocols))
	seen := make(map[string]struct{}, len(protocols))
	for _, protocol := range protocols {
		normalized := strings.TrimSpace(protocol)
		if normalized == "" {
			continue
		}
		if _, duplicate := seen[normalized]; duplicate {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
