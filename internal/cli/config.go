package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	core "github.com/keelab/keelith/config/versioned"
)

const (
	maxConfigEndpoints = 16
	maxConfigFileBytes = 1024 * 1024
)

type configConnectionOptions struct {
	endpoints     []string
	prefix        string
	dialTimeout   time.Duration
	username      string
	passwordEnv   string
	caFile        string
	certFile      string
	keyFile       string
	serverName    string
	allowInsecure bool
}

type configCommandOptions struct {
	connection configConnectionOptions
	command    string
	format     string

	file           string
	documentFormat core.Format
	actor          string
	message        string
	reason         string
	revision       string
	limit          int

	expectedGeneration    uint64
	expectedGenerationSet bool
}

type configStoreOpener func(
	context.Context,
	configConnectionOptions,
) (core.Store, error)

func executeConfigWithOpener(
	ctx context.Context,
	options configCommandOptions,
	stdout io.Writer,
	stderr io.Writer,
	opener configStoreOpener,
) int {
	if ctx == nil || opener == nil {
		_, _ = fmt.Fprintln(stderr, "keelith config: invalid runtime dependency")
		return 1
	}
	store, err := opener(ctx, options.connection)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith config: connect: %v\n", err)
		return 1
	}
	if store == nil {
		_, _ = fmt.Fprintln(stderr, "keelith config: connect returned nil store")
		return 1
	}
	code := executeConfigOperation(ctx, store, options, stdout, stderr)
	if closeErr := store.Close(); closeErr != nil && code == 0 {
		_, _ = fmt.Fprintf(stderr, "keelith config: close: %v\n", closeErr)
		return 1
	}
	return code
}

func executeConfigOperation(
	ctx context.Context,
	store core.Store,
	options configCommandOptions,
	stdout io.Writer,
	stderr io.Writer,
) int {
	var result any
	var err error
	switch options.command {
	case "stage":
		var content []byte
		content, err = readConfigCandidate(options.file)
		if err == nil {
			format := options.documentFormat
			if format == "" {
				format, err = configFormatFromPath(options.file)
			}
			if err == nil {
				result, err = store.Stage(ctx, core.StageRequest{
					Content: content, Format: format,
					Actor: options.actor, Message: options.message,
				})
			}
		}
	case "active":
		result, err = store.Active(ctx)
	case "history":
		result, err = store.History(ctx, options.limit)
	case "activate", "rollback":
		result, err = store.Activate(ctx, core.ActivateRequest{
			Revision: options.revision, ExpectedGeneration: options.expectedGeneration,
			Actor: options.actor, Reason: options.reason,
		})
	default:
		err = errUsage
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith config %s: %v\n", options.command, err)
		return 1
	}
	if err := renderConfigResult(stdout, options.command, options.format, result); err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith config %s: render: %v\n", options.command, err)
		return 1
	}
	return 0
}

func validateConfigConnection(options configConnectionOptions) error {
	if len(options.endpoints) == 0 || len(options.endpoints) > maxConfigEndpoints {
		return fmt.Errorf("--endpoint count must be between 1 and %d", maxConfigEndpoints)
	}
	seen := make(map[string]struct{}, len(options.endpoints))
	hasTLS := false
	for _, endpoint := range options.endpoints {
		parsed, err := url.ParseRequestURI(strings.TrimSpace(endpoint))
		if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
			return fmt.Errorf("invalid Etcd endpoint %q", endpoint)
		}
		if parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
			return fmt.Errorf("etcd endpoint %q must not contain credentials, path, query, or fragment", endpoint)
		}
		if parsed.Scheme == "http" && !options.allowInsecure {
			return fmt.Errorf("etcd endpoint %q requires --allow-insecure", endpoint)
		}
		hasTLS = hasTLS || parsed.Scheme == "https"
		normalized := parsed.String()
		if _, duplicate := seen[normalized]; duplicate {
			return fmt.Errorf("duplicate Etcd endpoint %q", endpoint)
		}
		seen[normalized] = struct{}{}
	}
	if hasTLS && options.allowInsecure && len(options.endpoints) > 1 {
		for _, endpoint := range options.endpoints {
			if strings.HasPrefix(endpoint, "http://") {
				return errors.New("HTTP and HTTPS Etcd endpoints cannot be mixed")
			}
		}
	}
	if strings.TrimSpace(options.prefix) == "" || !strings.HasPrefix(options.prefix, "/") {
		return errors.New("--prefix must be an absolute Etcd path")
	}
	if options.dialTimeout < time.Second || options.dialTimeout > time.Minute {
		return errors.New("--dial-timeout must be between 1s and 1m")
	}
	if (options.username == "") != (options.passwordEnv == "") {
		return errors.New("--username and --password-env must be configured together")
	}
	if options.passwordEnv != "" && !validEnvironmentName(options.passwordEnv) {
		return errors.New("--password-env is not a valid environment variable name")
	}
	if (options.certFile == "") != (options.keyFile == "") {
		return errors.New("--cert-file and --key-file must be configured together")
	}
	if !hasTLS && (options.caFile != "" || options.certFile != "" || options.keyFile != "" || options.serverName != "") {
		return errors.New("TLS files and --server-name require HTTPS endpoints")
	}
	return nil
}

func loadConfigTLS(options configConnectionOptions) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: options.serverName}
	if options.caFile != "" {
		payload, err := os.ReadFile(options.caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(payload) {
			return nil, errors.New("CA file contains no certificates")
		}
		config.RootCAs = roots
	}
	if options.certFile != "" {
		certificate, err := tls.LoadX509KeyPair(options.certFile, options.keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func readConfigCandidate(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("candidate must be a regular non-symlink file")
	}
	if info.Size() < 1 || info.Size() > maxConfigFileBytes {
		return nil, fmt.Errorf("candidate size must be between 1 and %d bytes", maxConfigFileBytes)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func configFormatFromPath(path string) (core.Format, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return core.FormatJSON, nil
	case ".yaml", ".yml":
		return core.FormatYAML, nil
	default:
		return "", errors.New("cannot infer document format; use --document-format")
	}
}

func renderConfigResult(writer io.Writer, command, format string, value any) error {
	if format == "json" {
		encoder := json.NewEncoder(writer)
		encoder.SetEscapeHTML(false)
		return encoder.Encode(value)
	}
	switch result := value.(type) {
	case core.Revision:
		_, err := fmt.Fprintf(writer, "staged revision=%s format=%s size=%d\n", result.ID, result.Format, result.Size)
		return err
	case core.Activation:
		_, err := fmt.Fprintf(writer, "%s generation=%d revision=%s previous=%s actor=%s reason=%s\n", command, result.Generation, result.Revision, result.Previous, result.Actor, result.Reason)
		return err
	case []core.Activation:
		for _, activation := range result {
			if _, err := fmt.Fprintf(writer, "generation=%d revision=%s previous=%s activated_at=%s actor=%s reason=%s\n", activation.Generation, activation.Revision, activation.Previous, activation.ActivatedAt.UTC().Format(time.RFC3339), activation.Actor, activation.Reason); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported result %T", value)
	}
}

func validEnvironmentName(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, character := range value {
		if index == 0 {
			if character != '_' && !unicode.IsLetter(character) {
				return false
			}
			continue
		}
		if character != '_' && !unicode.IsLetter(character) && !unicode.IsDigit(character) {
			return false
		}
	}
	return true
}
