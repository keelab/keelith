// Package vault resolves read-only HashiCorp Vault KV v2 fields without
// storing Vault tokens or secret material in Keelith configuration snapshots.
package vault

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/secret"
)

// Credentials resolves a Vault token through an already bootstrapped Secret
// Manager. The token reference itself is safe configuration.
type Credentials struct {
	Manager *secret.Manager
	Token   secret.Reference
}

// Description is a material-free operational snapshot.
type Description struct {
	PollInterval time.Duration
	MaxBytes     int
	Closed       bool
	Watchers     int
	Degraded     bool
}

// Provider implements secret.Provider for one Vault KV v2 mount.
//
// Provider-local keys use <secret-path>/<field>. For example,
// secret://vault/orders/database/password reads field "password" from the KV
// v2 secret "orders/database".
type Provider struct {
	client      *http.Client
	credentials Credentials
	options     Options
	endpoint    *url.URL

	mu        sync.Mutex
	closed    bool
	watchers  map[*watcher]struct{}
	lastError error
	closeOnce sync.Once
}

type keySpec struct {
	path  string
	field string
}

type readResponse struct {
	Data struct {
		Data     map[string]json.RawMessage `json:"data"`
		Metadata struct {
			DeletionTime string `json:"deletion_time"`
			Destroyed    bool   `json:"destroyed"`
			Version      int64  `json:"version"`
		} `json:"metadata"`
	} `json:"data"`
}

// New constructs a read-only Vault KV v2 Provider.
func New(
	client *http.Client,
	credentials Credentials,
	options Options,
) (*Provider, error) {
	if client == nil || credentials.Manager == nil {
		return nil, fmt.Errorf(
			"%w: http client and credential manager are required",
			ErrInvalidOption,
		)
	}
	if _, err := secret.NewReference(
		credentials.Token.Provider(),
		credentials.Token.Key(),
	); err != nil {
		return nil, fmt.Errorf("%w: token reference", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	endpoint, err := url.Parse(normalized.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("%w: endpoint", ErrInvalidOption)
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(
		_ *http.Request,
		_ []*http.Request,
	) error {
		return http.ErrUseLastResponse
	}
	return &Provider{
		client:      &clientCopy,
		credentials: credentials,
		options:     normalized,
		endpoint:    endpoint,
		watchers:    make(map[*watcher]struct{}),
	}, nil
}

// Resolve reads one exact string field from the latest KV v2 secret version.
func (provider *Provider) Resolve(
	ctx context.Context,
	key string,
) (secret.Value, error) {
	if provider == nil || ctx == nil {
		return secret.Value{}, fmt.Errorf(
			"%w: provider or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return secret.Value{}, cause
	}
	if err := provider.requireOpen(); err != nil {
		return secret.Value{}, err
	}
	target, err := parseKey(key)
	if err != nil {
		provider.recordError(err)
		return secret.Value{}, err
	}
	value, err := provider.read(ctx, target)
	provider.recordError(err)
	return value, err
}

// Watch polls the latest KV v2 version. The first Next returns the value read
// while establishing the watch, closing the Resolve-to-Watch race.
func (provider *Provider) Watch(
	ctx context.Context,
	key string,
) (secret.Watcher, error) {
	if provider == nil || ctx == nil {
		return nil, fmt.Errorf(
			"%w: provider or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := provider.requireOpen(); err != nil {
		return nil, err
	}
	target, err := parseKey(key)
	if err != nil {
		return nil, err
	}
	initial, err := provider.read(ctx, target)
	if err != nil {
		provider.recordError(err)
		return nil, err
	}
	revision, err := vaultRevision(initial.Version())
	if err != nil {
		provider.recordError(err)
		return nil, err
	}
	current := &watcher{
		provider: provider,
		target:   target,
		version:  initial.Version(),
		revision: revision,
		initial:  &initial,
		ticker:   time.NewTicker(provider.options.PollInterval),
		closed:   make(chan struct{}),
	}
	provider.mu.Lock()
	if provider.closed {
		provider.mu.Unlock()
		current.ticker.Stop()
		return nil, ErrClosed
	}
	provider.watchers[current] = struct{}{}
	provider.lastError = nil
	provider.mu.Unlock()
	return current, nil
}

// Shutdown closes active watchers and rejects new operations.
func (provider *Provider) Shutdown(ctx context.Context) error {
	if provider == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	provider.closeOnce.Do(func() {
		provider.mu.Lock()
		provider.closed = true
		watchers := make([]*watcher, 0, len(provider.watchers))
		for current := range provider.watchers {
			watchers = append(watchers, current)
		}
		provider.mu.Unlock()
		for _, current := range watchers {
			_ = current.Close()
		}
	})
	return context.Cause(ctx)
}

// LastError returns the latest request, credential, response, or watch error.
func (provider *Provider) LastError() error {
	if provider == nil {
		return ErrClosed
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.lastError
}

// WatcherCount returns active logical watchers.
func (provider *Provider) WatcherCount() int {
	if provider == nil {
		return 0
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return len(provider.watchers)
}

// Describe returns material-free runtime state.
func (provider *Provider) Describe() Description {
	if provider == nil {
		return Description{Closed: true, Degraded: true}
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return Description{
		PollInterval: provider.options.PollInterval,
		MaxBytes:     provider.options.MaxBytes,
		Closed:       provider.closed,
		Watchers:     len(provider.watchers),
		Degraded:     provider.lastError != nil,
	}
}

func (provider *Provider) read(
	ctx context.Context,
	target keySpec,
) (secret.Value, error) {
	if err := provider.requireOpen(); err != nil {
		return secret.Value{}, err
	}
	tokenValue, err := provider.credentials.Manager.Resolve(
		ctx,
		provider.credentials.Token,
	)
	if err != nil {
		return secret.Value{}, fmt.Errorf(
			"secret/vault: resolve token: %w",
			err,
		)
	}
	token := tokenValue.Bytes()
	defer clear(token)
	trimmedToken := secret.TrimLineBreaks(token)
	if !validToken(trimmedToken, provider.options.MaxTokenBytes) {
		return secret.Value{}, fmt.Errorf(
			"%w: token material is invalid",
			ErrInvalidOption,
		)
	}

	requestContext, cancel := context.WithTimeout(
		ctx,
		provider.options.RequestTimeout,
	)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestContext,
		http.MethodGet,
		provider.readurl(target),
		nil,
	)
	if err != nil {
		return secret.Value{}, fmt.Errorf(
			"%w: construct request",
			ErrInvalidOption,
		)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Vault-Request", "true")
	request.Header.Set("X-Vault-Token", string(trimmedToken))
	if provider.options.Namespace != "" {
		request.Header.Set("X-Vault-Namespace", provider.options.Namespace)
	}
	response, err := provider.client.Do(request)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return secret.Value{}, cause
		}
		if cause := context.Cause(requestContext); cause != nil {
			return secret.Value{}, fmt.Errorf(
				"%w: %w",
				ErrUnavailable,
				cause,
			)
		}
		return secret.Value{}, fmt.Errorf("%w: request failed", ErrUnavailable)
	}
	if response == nil || response.Body == nil {
		return secret.Value{}, fmt.Errorf(
			"%w: response is nil",
			ErrInvalidResponse,
		)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(
		response.Body,
		int64(provider.options.MaxResponseBytes)+1,
	))
	if err != nil {
		return secret.Value{}, fmt.Errorf("%w: read response", ErrUnavailable)
	}
	if len(body) > provider.options.MaxResponseBytes {
		return secret.Value{}, ErrTooLarge
	}
	defer clear(body)
	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return secret.Value{}, secret.ErrNotFound
	case http.StatusUnauthorized, http.StatusForbidden:
		return secret.Value{}, ErrUnauthorized
	case http.StatusTooManyRequests:
		return secret.Value{}, ErrUnavailable
	default:
		if response.StatusCode >= 500 {
			return secret.Value{}, ErrUnavailable
		}
		return secret.Value{}, fmt.Errorf(
			"%w: http status %d",
			ErrInvalidResponse,
			response.StatusCode,
		)
	}
	return provider.decode(body, target.field)
}

func (provider *Provider) decode(
	body []byte,
	field string,
) (secret.Value, error) {
	var envelope readResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&envelope); err != nil {
		return secret.Value{}, fmt.Errorf(
			"%w: decode json",
			ErrInvalidResponse,
		)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return secret.Value{}, fmt.Errorf(
			"%w: trailing json",
			ErrInvalidResponse,
		)
	}
	metadata := envelope.Data.Metadata
	if metadata.Version < 1 ||
		metadata.Destroyed ||
		strings.TrimSpace(metadata.DeletionTime) != "" {
		return secret.Value{}, secret.ErrNotFound
	}
	raw, exists := envelope.Data.Data[field]
	if !exists {
		return secret.Value{}, secret.ErrNotFound
	}
	var content string
	if err := json.Unmarshal(raw, &content); err != nil {
		return secret.Value{}, fmt.Errorf(
			"%w: selected field is not a string",
			ErrInvalidResponse,
		)
	}
	if len(content) > provider.options.MaxBytes {
		return secret.Value{}, ErrTooLarge
	}
	digest := sha256.Sum256([]byte(content))
	return secret.NewValue(
		[]byte(content),
		"vault-kv2:"+
			strconv.FormatInt(metadata.Version, 10)+
			":sha256:"+
			hex.EncodeToString(digest[:]),
		time.Time{},
	)
}

func (provider *Provider) readurl(target keySpec) string {
	path := "/v1/" +
		provider.options.Mount +
		"/data/" +
		target.path
	result := *provider.endpoint
	result.Path = path
	result.RawPath = ""
	return result.String()
}

func (provider *Provider) requireOpen() error {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.closed {
		return ErrClosed
	}
	return nil
}

func (provider *Provider) recordError(err error) {
	provider.mu.Lock()
	provider.lastError = err
	provider.mu.Unlock()
}

func (provider *Provider) removeWatcher(current *watcher) {
	provider.mu.Lock()
	delete(provider.watchers, current)
	provider.mu.Unlock()
}

type watcher struct {
	provider *Provider
	target   keySpec
	version  string
	revision int64
	initial  *secret.Value
	ticker   *time.Ticker
	closed   chan struct{}
	nextMu   sync.Mutex
	once     sync.Once
}

func (watcher *watcher) Next(ctx context.Context) (secret.Value, error) {
	if watcher == nil || ctx == nil {
		return secret.Value{}, fmt.Errorf(
			"%w: watcher or context is nil",
			ErrInvalidOption,
		)
	}
	watcher.nextMu.Lock()
	defer watcher.nextMu.Unlock()
	select {
	case <-watcher.closed:
		return secret.Value{}, secret.ErrWatcherClosed
	default:
	}
	if watcher.initial != nil {
		value := *watcher.initial
		watcher.initial = nil
		return value, nil
	}
	for {
		select {
		case <-ctx.Done():
			return secret.Value{}, context.Cause(ctx)
		case <-watcher.closed:
			return secret.Value{}, secret.ErrWatcherClosed
		case <-watcher.ticker.C:
			value, err := watcher.provider.read(ctx, watcher.target)
			if err != nil {
				watcher.provider.recordError(err)
				_ = watcher.Close()
				return secret.Value{}, err
			}
			revision, err := vaultRevision(value.Version())
			if err != nil ||
				revision < watcher.revision ||
				revision == watcher.revision &&
					value.Version() != watcher.version {
				watchErr := fmt.Errorf(
					"%w: KV version is not monotonic",
					ErrInvalidResponse,
				)
				watcher.provider.recordError(watchErr)
				_ = watcher.Close()
				return secret.Value{}, watchErr
			}
			if value.Version() == watcher.version {
				continue
			}
			watcher.version = value.Version()
			watcher.revision = revision
			watcher.provider.recordError(nil)
			return value, nil
		}
	}
}

func (watcher *watcher) Close() error {
	if watcher == nil {
		return nil
	}
	watcher.once.Do(func() {
		watcher.ticker.Stop()
		close(watcher.closed)
		watcher.provider.removeWatcher(watcher)
	})
	return nil
}

func parseKey(key string) (keySpec, error) {
	if !validPath(key, 1024) {
		return keySpec{}, fmt.Errorf(
			"%w: key must be <secret-path>/<field>",
			ErrInvalidOption,
		)
	}
	segments := strings.Split(key, "/")
	if len(segments) < 2 {
		return keySpec{}, fmt.Errorf(
			"%w: key must be <secret-path>/<field>",
			ErrInvalidOption,
		)
	}
	return keySpec{
		path:  strings.Join(segments[:len(segments)-1], "/"),
		field: segments[len(segments)-1],
	}, nil
}

func vaultRevision(version string) (int64, error) {
	remainder, found := strings.CutPrefix(version, "vault-kv2:")
	if !found {
		return 0, ErrInvalidResponse
	}
	revisionText, _, found := strings.Cut(remainder, ":")
	if !found {
		return 0, ErrInvalidResponse
	}
	revision, err := strconv.ParseInt(revisionText, 10, 64)
	if err != nil || revision < 1 {
		return 0, ErrInvalidResponse
	}
	return revision, nil
}

func validToken(value []byte, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character > 0x7e {
			return false
		}
	}
	return true
}

var _ secret.Provider = (*Provider)(nil)
