package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"
)

const (
	defaultMaxBytes = 1024 * 1024
	maxMaxBytes     = 1024 * 1024
)

var (
	// ErrInvalidOption reports an invalid backend, key, format, or byte budget.
	ErrInvalidOption = errors.New("etcd config: invalid option")
	// ErrClosed reports an operation after Source shutdown.
	ErrClosed = errors.New("etcd config: source closed")
	// ErrNotFound reports a required key that does not exist.
	ErrNotFound = errors.New("etcd config: key not found")
	// ErrTooLarge reports a document beyond the configured budget.
	ErrTooLarge = errors.New("etcd config: document is too large")
	// ErrInvalidDocument reports malformed json/yaml or a non-object root.
	ErrInvalidDocument = errors.New("etcd config: invalid document")
	// ErrWatchClosed reports an unexpected backend watch channel closure.
	ErrWatchClosed = errors.New("etcd config: backend watch closed")
)

// Format identifies one complete stored document.
type Format string

const (
	// FormatJSON decodes one json object.
	FormatJSON Format = "json"
	// FormatYAML decodes one yaml document.
	FormatYAML Format = "yaml"
)

// Options configure one exact etcd config key.
type Options struct {
	Key          string
	Format       Format
	AllowMissing bool
	MaxBytes     int
	OwnsClient   bool
}

// Description is a bounded operational Source snapshot.
type Description struct {
	Key          string
	Format       Format
	AllowMissing bool
	MaxBytes     int
	Closed       bool
	Watchers     int
	LastError    string
}

// Source implements config.Source for one complete etcd document.
type Source struct {
	backend      Backend
	key          string
	format       Format
	allowMissing bool
	maxBytes     int
	ownsClient   bool

	opMu     sync.Mutex
	mu       sync.Mutex
	closed   bool
	watchers map[*watcher]struct{}
	lastErr  error
	latest   int64

	closeOnce sync.Once
	closeErr  error
}

// New constructs a Source around an official etcd v3 client.
func New(client *clientv3.Client, options Options) (*Source, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	return Wrap(&sdkBackend{client: client}, options)
}

// Wrap constructs a Source around a custom Backend.
func Wrap(backend Backend, options Options) (*Source, error) {
	if isNilBackend(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	key := strings.TrimSpace(options.Key)
	if !validKey(key) {
		return nil, fmt.Errorf("%w: key %q", ErrInvalidOption, key)
	}
	format := options.Format
	if format == "" {
		format = formatFromKey(key)
	}
	if format != FormatJSON && format != FormatYAML {
		return nil, fmt.Errorf("%w: format %q", ErrInvalidOption, format)
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxBytes
	}
	if maxBytes <= 0 || maxBytes > maxMaxBytes {
		return nil, fmt.Errorf("%w: max bytes %d", ErrInvalidOption, maxBytes)
	}
	return &Source{
		backend:      backend,
		key:          key,
		format:       format,
		allowMissing: options.AllowMissing,
		maxBytes:     maxBytes,
		ownsClient:   options.OwnsClient,
		watchers:     make(map[*watcher]struct{}),
	}, nil
}

// Start verifies that the initial remote document is loadable.
func (source *Source) Start(ctx context.Context) error {
	_, err := source.Load(ctx)
	return err
}

// Load reads and decodes one linearizable exact-key value.
func (source *Source) Load(ctx context.Context) (config.Snapshot, error) {
	if source == nil || ctx == nil {
		return config.Snapshot{}, fmt.Errorf(
			"%w: source or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return config.Snapshot{}, cause
	}
	if err := source.requireOpen(); err != nil {
		return config.Snapshot{}, err
	}
	value, err := source.backend.Get(ctx, source.key)
	if err != nil {
		source.recordError(err)
		return config.Snapshot{}, fmt.Errorf("etcd config: get %q: %w", source.key, err)
	}
	snapshot, err := source.snapshot(value)
	if err == nil {
		source.acceptRevision(value.Revision)
	} else {
		source.recordError(err)
	}
	return snapshot, err
}

// Watch observes future exact-key revisions without replaying the initial value.
func (source *Source) Watch(ctx context.Context) (config.Watcher, error) {
	if source == nil || ctx == nil {
		return nil, fmt.Errorf(
			"%w: source or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	source.opMu.Lock()
	defer source.opMu.Unlock()
	if err := source.requireOpen(); err != nil {
		return nil, err
	}
	value, err := source.backend.Get(ctx, source.key)
	if err != nil {
		source.recordError(err)
		return nil, fmt.Errorf("etcd config: establish watch: %w", err)
	}
	if _, err := source.snapshot(value); err != nil {
		source.recordError(err)
		return nil, err
	}
	source.recordError(nil)
	watchCtx, cancel := context.WithCancel(ctx)
	updates := source.backend.Watch(watchCtx, source.key, value.Revision+1)
	if updates == nil {
		cancel()
		return nil, fmt.Errorf("%w: backend returned nil watch", ErrInvalidOption)
	}
	current := &watcher{
		source:   source,
		context:  watchCtx,
		cancel:   cancel,
		updates:  updates,
		revision: value.Revision,
		results:  make(chan result, 1),
		done:     make(chan struct{}),
		terminal: config.ErrWatcherClosed,
	}
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	source.watchers[current] = struct{}{}
	source.mu.Unlock()
	go current.run()
	return current, nil
}

// Shutdown closes logical watchers and optionally the SDK client.
func (source *Source) Shutdown(ctx context.Context) error {
	if source == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	source.closeOnce.Do(func() {
		source.opMu.Lock()
		defer source.opMu.Unlock()
		source.mu.Lock()
		source.closed = true
		watchers := make([]*watcher, 0, len(source.watchers))
		for current := range source.watchers {
			watchers = append(watchers, current)
		}
		source.mu.Unlock()
		for _, current := range watchers {
			source.closeErr = errors.Join(source.closeErr, current.Close())
		}
		if cause := context.Cause(ctx); cause != nil {
			source.closeErr = errors.Join(source.closeErr, cause)
		}
		if source.ownsClient {
			source.closeErr = errors.Join(source.closeErr, source.backend.Close())
		}
	})
	return source.closeErr
}

// LastError returns the latest load, decode, required-delete, or watch error.
func (source *Source) LastError() error {
	if source == nil {
		return ErrClosed
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.lastErr
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

// Describe returns bounded Source diagnostics.
func (source *Source) Describe() Description {
	if source == nil {
		return Description{Closed: true, LastError: "source is nil"}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	description := Description{
		Key:          source.key,
		Format:       source.format,
		AllowMissing: source.allowMissing,
		MaxBytes:     source.maxBytes,
		Closed:       source.closed,
		Watchers:     len(source.watchers),
	}
	if source.lastErr != nil {
		description.LastError = sanitizeError(source.lastErr.Error(), 512)
	}
	return description
}

func (source *Source) snapshot(value Value) (config.Snapshot, error) {
	if value.Revision < 0 {
		return config.Snapshot{}, fmt.Errorf(
			"%w: negative revision %d",
			ErrInvalidDocument,
			value.Revision,
		)
	}
	if !value.Found {
		if !source.allowMissing {
			return config.Snapshot{}, fmt.Errorf(
				"%w: %s",
				ErrNotFound,
				source.key,
			)
		}
		return config.NewSnapshot(
			fmt.Sprintf("etcd:%d", value.Revision),
			map[string]any{},
		)
	}
	values, err := decodeDocument(value.Content, source.format, source.maxBytes)
	if err != nil {
		return config.Snapshot{}, fmt.Errorf(
			"etcd config: decode %q: %w",
			source.key,
			err,
		)
	}
	return config.NewSnapshot(
		fmt.Sprintf("etcd:%d", value.Revision),
		values,
	)
}

func (source *Source) requireOpen() error {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return ErrClosed
	}
	return nil
}

func (source *Source) removeWatcher(current *watcher) {
	source.mu.Lock()
	delete(source.watchers, current)
	source.mu.Unlock()
}

func (source *Source) recordError(err error) {
	source.mu.Lock()
	source.lastErr = err
	source.mu.Unlock()
}

func (source *Source) acceptRevision(revision int64) bool {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.lastErr = nil
	if revision <= source.latest {
		return false
	}
	source.latest = revision
	return true
}

func (source *Source) staleRevision(revision int64) bool {
	source.mu.Lock()
	defer source.mu.Unlock()
	return revision < source.latest
}

func decodeDocument(
	content []byte,
	format Format,
	maxBytes int,
) (map[string]any, error) {
	if len(content) > maxBytes {
		return nil, fmt.Errorf(
			"%w: %d bytes exceeds %d",
			ErrTooLarge,
			len(content),
			maxBytes,
		)
	}
	var values map[string]any
	switch format {
	case FormatJSON:
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.UseNumber()
		if err := decoder.Decode(&values); err != nil {
			return nil, fmt.Errorf("%w: json: %w", ErrInvalidDocument, err)
		}
		if err := requireEOF(decoder.Decode(new(any))); err != nil {
			return nil, fmt.Errorf("%w: json: %w", ErrInvalidDocument, err)
		}
	case FormatYAML:
		decoder := yaml.NewDecoder(bytes.NewReader(content))
		if err := decoder.Decode(&values); err != nil {
			return nil, fmt.Errorf("%w: yaml: %w", ErrInvalidDocument, err)
		}
		var extra any
		if err := requireEOF(decoder.Decode(&extra)); err != nil {
			return nil, fmt.Errorf("%w: yaml: %w", ErrInvalidDocument, err)
		}
	default:
		return nil, fmt.Errorf("%w: format %q", ErrInvalidOption, format)
	}
	if values == nil {
		return nil, fmt.Errorf("%w: root must be an object", ErrInvalidDocument)
	}
	return values, nil
}

func requireEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple documents are not allowed")
	}
	return err
}

func formatFromKey(key string) Format {
	lower := strings.ToLower(key)
	switch {
	case strings.HasSuffix(lower, ".json"):
		return FormatJSON
	case strings.HasSuffix(lower, ".yaml"),
		strings.HasSuffix(lower, ".yml"):
		return FormatYAML
	default:
		return ""
	}
}

func validKey(key string) bool {
	if key == "" || len(key) > 512 || !utf8.ValidString(key) {
		return false
	}
	for _, character := range key {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func isNilBackend(backend Backend) bool {
	if backend == nil {
		return true
	}
	value := reflect.ValueOf(backend)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func sanitizeError(value string, limit int) string {
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, value)
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

var _ config.Source = (*Source)(nil)
