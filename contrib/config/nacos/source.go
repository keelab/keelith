// Package nacos provides a revisioned nacos config.Source with local LKG
// fallback.
package nacos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	nacosruntime "github.com/keelab/contrib/nacos"
	"github.com/keelab/keelith/config"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"gopkg.in/yaml.v3"
)

var (
	// ErrInvalidOption reports an invalid client, key, format, or cache path.
	ErrInvalidOption = errors.New("nacos config: invalid option")
)

// Format identifies the serialized nacos content.
type Format string

const (
	// FormatJSON decodes a json object.
	FormatJSON Format = "json"
	// FormatYAML decodes a yaml object.
	FormatYAML Format = "yaml"
)

// Backend isolates Source from the concrete nacos SDK.
type Backend interface {
	Get(context.Context, string, string) (string, error)
	Listen(
		context.Context,
		string,
		string,
		func(string, error),
	) (func() error, error)
	Close()
}

// Options identify one nacos config and its local fallback.
type Options struct {
	Dataid    string
	Group     string
	Format    Format
	CachePath string
	Owns      bool
}

// Description is a value-free runtime snapshot.
type Description struct {
	Format     Format
	Cache      bool
	OwnsClient bool
	Watchers   int
	Closed     bool
	Degraded   bool
}

// Source implements config.Source and owns watcher fan-out.
type Source struct {
	backend   Backend
	dataid    string
	group     string
	format    Format
	cachePath string
	owns      bool

	mu           sync.Mutex
	watchers     map[*watcher]struct{}
	cancelListen func() error
	lastError    error
	generation   uint64
	closed       bool

	closeOnce sync.Once
	closeErr  error
}

// New creates a Source around an official nacos config client.
func New(
	client config_client.IConfigClient,
	options Options,
) (*Source, error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	return Wrap(&sdkBackend{client: client}, options)
}

// Open constructs and owns an official nacos SDK client through the shared,
// secret-safe runtime configuration.
func Open(
	ctx context.Context,
	clientConfig nacosruntime.Config,
	resolver nacosruntime.SecretResolver,
	options Options,
) (*Source, error) {
	client, err := nacosruntime.OpenConfigClient(
		ctx,
		clientConfig,
		resolver,
	)
	if err != nil {
		return nil, err
	}
	options.Owns = true
	source, err := New(client, options)
	if err != nil {
		client.CloseClient()
		return nil, err
	}
	return source, nil
}

// Wrap creates a Source around a custom Backend.
func Wrap(backend Backend, options Options) (*Source, error) {
	if isNil(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return &Source{
		backend:   backend,
		dataid:    normalized.Dataid,
		group:     normalized.Group,
		format:    normalized.Format,
		cachePath: normalized.CachePath,
		owns:      normalized.Owns,
		watchers:  make(map[*watcher]struct{}),
	}, nil
}

// NormalizeOptions applies stable defaults and validates one remote source.
func NormalizeOptions(options Options) (Options, error) {
	normalized := options
	normalized.Dataid = strings.TrimSpace(normalized.Dataid)
	normalized.Group = strings.TrimSpace(normalized.Group)
	normalized.CachePath = strings.TrimSpace(normalized.CachePath)
	if normalized.Dataid == "" || normalized.Group == "" {
		return Options{}, fmt.Errorf(
			"%w: data id and group are required",
			ErrInvalidOption,
		)
	}
	if normalized.Format == "" {
		normalized.Format = FormatYAML
	}
	if normalized.Format != FormatJSON && normalized.Format != FormatYAML {
		return Options{}, fmt.Errorf(
			"%w: format %q",
			ErrInvalidOption,
			normalized.Format,
		)
	}
	if normalized.CachePath != "" {
		normalized.CachePath = filepath.Clean(normalized.CachePath)
		if !filepath.IsAbs(normalized.CachePath) {
			return Options{}, fmt.Errorf(
				"%w: cache path must be absolute",
				ErrInvalidOption,
			)
		}
	}
	return normalized, nil
}

// ValidateOptions validates one remote source without constructing a client.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

// Start loads a remote or last-known-good snapshot.
func (source *Source) Start(ctx context.Context) error {
	_, err := source.Load(ctx)
	return err
}

// Shutdown stops listeners and closes an owned SDK client.
func (source *Source) Shutdown(context.Context) error {
	if source == nil {
		return nil
	}
	source.closeOnce.Do(func() {
		source.mu.Lock()
		source.closed = true
		cancel := source.cancelListen
		source.cancelListen = nil
		watchers := make([]*watcher, 0, len(source.watchers))
		for current := range source.watchers {
			watchers = append(watchers, current)
		}
		source.watchers = make(map[*watcher]struct{})
		source.mu.Unlock()
		if cancel != nil {
			source.closeErr = errors.Join(source.closeErr, cancel())
		}
		for _, current := range watchers {
			current.signalClose()
		}
		if source.owns {
			source.backend.Close()
		}
	})
	return source.closeErr
}

// Load prefers nacos and falls back to the local last-known-good content.
func (source *Source) Load(ctx context.Context) (config.Snapshot, error) {
	if source == nil || isNil(source.backend) {
		return config.Snapshot{}, fmt.Errorf("%w: source is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return config.Snapshot{}, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	content, err := source.backend.Get(ctx, source.dataid, source.group)
	if err == nil {
		snapshot, decodeErr := source.decode(content)
		if decodeErr != nil {
			source.recordError(decodeErr)
			return config.Snapshot{}, decodeErr
		}
		cacheErr := source.writeCache(content)
		source.recordLoad(cacheErr)
		return snapshot, nil
	}
	source.recordError(err)
	cached, cacheErr := source.readCache()
	if cacheErr != nil {
		return config.Snapshot{}, errors.Join(
			fmt.Errorf("nacos config: get: %w", err),
			cacheErr,
		)
	}
	snapshot, decodeErr := source.decode(cached)
	if decodeErr != nil {
		return config.Snapshot{}, errors.Join(err, decodeErr)
	}
	return snapshot, nil
}

// Watch registers one full-snapshot watcher and shares a single SDK listener.
func (source *Source) Watch(ctx context.Context) (config.Watcher, error) {
	if source == nil || isNil(source.backend) {
		return nil, fmt.Errorf("%w: source is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	current := &watcher{
		source:  source,
		updates: make(chan update, 1),
		done:    make(chan struct{}),
	}
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		return nil, config.ErrWatcherClosed
	}
	source.watchers[current] = struct{}{}
	needsListen := source.cancelListen == nil
	source.mu.Unlock()

	if needsListen {
		cancel, err := source.backend.Listen(
			ctx,
			source.dataid,
			source.group,
			source.onUpdate,
		)
		if err != nil {
			source.removeWatcher(current)
			current.signalClose()
			return nil, fmt.Errorf("nacos config: listen: %w", err)
		}
		source.mu.Lock()
		if source.cancelListen == nil {
			source.cancelListen = cancel
			cancel = nil
		}
		source.mu.Unlock()
		if cancel != nil {
			_ = cancel()
		}
	}
	return current, nil
}

// LastError returns the most recent remote, decode, or cache failure.
func (source *Source) LastError() error {
	if source == nil {
		return nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.lastError
}

// WatcherCount returns the number of open logical watchers.
func (source *Source) WatcherCount() int {
	if source == nil {
		return 0
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return len(source.watchers)
}

// Describe returns value-free source health and ownership state.
func (source *Source) Describe() Description {
	if source == nil {
		return Description{}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return Description{
		Format:     source.format,
		Cache:      source.cachePath != "",
		OwnsClient: source.owns,
		Watchers:   len(source.watchers),
		Closed:     source.closed,
		Degraded:   source.lastError != nil,
	}
}

func (source *Source) onUpdate(content string, err error) {
	if err != nil {
		source.recordError(err)
		return
	}
	snapshot, err := source.decode(content)
	if err != nil {
		source.recordError(err)
		return
	}
	cacheErr := source.writeCache(content)
	source.mu.Lock()
	source.generation++
	generation := source.generation
	source.lastError = cacheErr
	watchers := make([]*watcher, 0, len(source.watchers))
	for current := range source.watchers {
		watchers = append(watchers, current)
	}
	source.mu.Unlock()
	for _, current := range watchers {
		current.publish(update{
			generation: generation,
			snapshot:   snapshot,
		})
	}
}

func (source *Source) decode(content string) (config.Snapshot, error) {
	values := make(map[string]any)
	switch source.format {
	case FormatJSON:
		decoder := json.NewDecoder(strings.NewReader(content))
		decoder.UseNumber()
		if err := decoder.Decode(&values); err != nil {
			return config.Snapshot{}, fmt.Errorf("nacos config: decode json: %w", err)
		}
	case FormatYAML:
		if err := yaml.Unmarshal([]byte(content), &values); err != nil {
			return config.Snapshot{}, fmt.Errorf("nacos config: decode yaml: %w", err)
		}
	default:
		return config.Snapshot{}, fmt.Errorf("%w: format %q", ErrInvalidOption, source.format)
	}
	hash := sha256.Sum256([]byte(content))
	return config.NewSnapshot(hex.EncodeToString(hash[:]), values)
}

func (source *Source) writeCache(content string) error {
	if source.cachePath == "" {
		return nil
	}
	directory := filepath.Dir(source.cachePath)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("nacos config: create cache directory: %w", err)
	}
	file, err := os.CreateTemp(directory, ".keelith-nacos-*")
	if err != nil {
		return fmt.Errorf("nacos config: create cache file: %w", err)
	}
	name := file.Name()
	defer func() {
		_ = os.Remove(name)
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("nacos config: chmod cache: %w", err)
	}
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		return fmt.Errorf("nacos config: write cache: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("nacos config: sync cache: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("nacos config: close cache: %w", err)
	}
	if err := os.Rename(name, source.cachePath); err != nil {
		return fmt.Errorf("nacos config: replace cache: %w", err)
	}
	return nil
}

func (source *Source) readCache() (string, error) {
	if source.cachePath == "" {
		return "", errors.New("nacos config: no local cache configured")
	}
	content, err := os.ReadFile(source.cachePath)
	if err != nil {
		return "", fmt.Errorf("nacos config: read cache: %w", err)
	}
	return string(content), nil
}

func (source *Source) removeWatcher(current *watcher) {
	source.mu.Lock()
	delete(source.watchers, current)
	shouldCancel := len(source.watchers) == 0
	cancel := source.cancelListen
	if shouldCancel {
		source.cancelListen = nil
	}
	source.mu.Unlock()
	if shouldCancel && cancel != nil {
		if err := cancel(); err != nil {
			source.recordError(err)
		}
	}
}

func (source *Source) recordError(err error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.lastError = err
}

func (source *Source) recordLoad(err error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.generation++
	source.lastError = err
}

type update struct {
	generation uint64
	snapshot   config.Snapshot
}

type watcher struct {
	source  *Source
	updates chan update
	done    chan struct{}
	once    sync.Once
}

func (watcher *watcher) Next(ctx context.Context) (config.Snapshot, error) {
	for {
		select {
		case current := <-watcher.updates:
			watcher.source.mu.Lock()
			generation := watcher.source.generation
			watcher.source.mu.Unlock()
			if current.generation < generation {
				continue
			}
			return current.snapshot.Clone(), nil
		case <-watcher.done:
			return config.Snapshot{}, config.ErrWatcherClosed
		case <-ctx.Done():
			return config.Snapshot{}, context.Cause(ctx)
		}
	}
}

func (watcher *watcher) Close() error {
	watcher.once.Do(func() {
		watcher.source.removeWatcher(watcher)
		close(watcher.done)
	})
	return nil
}

func (watcher *watcher) publish(current update) {
	select {
	case <-watcher.done:
		return
	default:
	}
	select {
	case <-watcher.updates:
	default:
	}
	select {
	case watcher.updates <- update{
		generation: current.generation,
		snapshot:   current.snapshot.Clone(),
	}:
	default:
	}
}

func (watcher *watcher) signalClose() {
	watcher.once.Do(func() {
		close(watcher.done)
	})
}

type sdkBackend struct {
	client config_client.IConfigClient
}

func (backend *sdkBackend) Get(
	ctx context.Context,
	dataid string,
	group string,
) (string, error) {
	if cause := context.Cause(ctx); cause != nil {
		return "", cause
	}
	content, err := backend.client.GetConfig(vo.ConfigParam{
		DataId: dataid,
		Group:  group,
	})
	if err == nil {
		err = context.Cause(ctx)
	}
	return content, err
}

func (backend *sdkBackend) Listen(
	ctx context.Context,
	dataid string,
	group string,
	callback func(string, error),
) (func() error, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	param := vo.ConfigParam{
		DataId: dataid,
		Group:  group,
		OnChange: func(_, _, _, data string) {
			callback(data, nil)
		},
	}
	if err := backend.client.ListenConfig(param); err != nil {
		return nil, err
	}
	return func() error {
		return backend.client.CancelListenConfig(param)
	}, nil
}

func (backend *sdkBackend) Close() {
	backend.client.CloseClient()
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
