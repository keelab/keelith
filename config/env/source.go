// Package env provides prefix-scoped environment configuration snapshots.
package env

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/config"
)

const defaultPollInterval = time.Second

var (
	// ErrInvalidOption reports an invalid environment source option.
	ErrInvalidOption = errors.New("config/env: invalid option")
	// ErrDuplicatePath reports environment keys that normalize to one path.
	ErrDuplicatePath = errors.New("config/env: duplicate normalized path")
	// ErrPathConflict reports a scalar key that is also used as a parent path.
	ErrPathConflict = errors.New("config/env: conflicting normalized path")
)

// Parser converts one environment string into a configuration value.
type Parser func(path []string, value string) (any, error)

// Option configures a Source.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (function optionFunc) apply(options *options) error {
	return function(options)
}

type options struct {
	separator    string
	pollInterval time.Duration
	environ      func() []string
	parser       Parser
}

// WithSeparator sets the nested-key separator. The default is "__".
func WithSeparator(separator string) Option {
	return optionFunc(func(options *options) error {
		if separator == "" || strings.Contains(separator, "=") {
			return fmt.Errorf("%w: separator is invalid", ErrInvalidOption)
		}
		options.separator = separator
		return nil
	})
}

// WithPollInterval sets the interval used by Watch.
func WithPollInterval(interval time.Duration) Option {
	return optionFunc(func(options *options) error {
		if interval <= 0 {
			return fmt.Errorf("%w: poll interval must be positive", ErrInvalidOption)
		}
		options.pollInterval = interval
		return nil
	})
}

// WithEnviron replaces os.Environ, primarily for deterministic hosts and
// tests.
func WithEnviron(environ func() []string) Option {
	return optionFunc(func(options *options) error {
		if environ == nil {
			return fmt.Errorf("%w: environ function is nil", ErrInvalidOption)
		}
		options.environ = environ
		return nil
	})
}

// WithParser converts selected values instead of retaining strings.
func WithParser(parser Parser) Option {
	return optionFunc(func(options *options) error {
		if parser == nil {
			return fmt.Errorf("%w: parser is nil", ErrInvalidOption)
		}
		options.parser = parser
		return nil
	})
}

// Source maps PREFIX_FOO__BAR to the path foo.bar.
type Source struct {
	prefix       string
	separator    string
	pollInterval time.Duration
	environ      func() []string
	parser       Parser
}

// New constructs a prefix-scoped environment Source.
func New(prefix string, optionList ...Option) (*Source, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || strings.Contains(prefix, "=") {
		return nil, fmt.Errorf("%w: prefix is invalid", ErrInvalidOption)
	}
	settings := options{
		separator:    "__",
		pollInterval: defaultPollInterval,
		environ:      defaultEnviron,
		parser: func(_ []string, value string) (any, error) {
			return value, nil
		},
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("config/env: option %d: %w", index, err)
		}
	}
	return &Source{
		prefix:       prefix,
		separator:    settings.separator,
		pollInterval: settings.pollInterval,
		environ:      settings.environ,
		parser:       settings.parser,
	}, nil
}

// Load captures a complete deterministic snapshot of selected variables.
func (source *Source) Load(ctx context.Context) (config.Snapshot, error) {
	if ctx == nil {
		return config.Snapshot{}, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if err := context.Cause(ctx); err != nil {
		return config.Snapshot{}, err
	}
	type entry struct {
		key   string
		value string
	}
	entries := make([]entry, 0)
	for _, raw := range source.environ() {
		key, value, found := strings.Cut(raw, "=")
		if !found || !strings.HasPrefix(key, source.prefix) {
			continue
		}
		entries = append(entries, entry{key: key, value: value})
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].key < entries[right].key
	})

	values := make(map[string]any)
	seen := make(map[string]string, len(entries))
	hasher := sha256.New()
	for _, entry := range entries {
		path, err := source.path(entry.key)
		if err != nil {
			return config.Snapshot{}, err
		}
		normalized := strings.Join(path, ".")
		if previous, duplicate := seen[normalized]; duplicate {
			return config.Snapshot{}, fmt.Errorf("%w: %s and %s become %s", ErrDuplicatePath, previous, entry.key, normalized)
		}
		for existing, previous := range seen {
			if strings.HasPrefix(existing, normalized+".") ||
				strings.HasPrefix(normalized, existing+".") {
				return config.Snapshot{}, fmt.Errorf("%w: %s and %s conflict at %s/%s", ErrPathConflict, previous, entry.key, existing, normalized)
			}
		}
		seen[normalized] = entry.key
		parsed, err := source.parser(append([]string(nil), path...), entry.value)
		if err != nil {
			return config.Snapshot{}, fmt.Errorf(
				"config/env: parse %s: %w",
				entry.key,
				err,
			)
		}
		set(values, path, parsed)
		_, _ = hasher.Write([]byte(entry.key))
		_, _ = hasher.Write([]byte{0})
		_, _ = hasher.Write([]byte(entry.value))
		_, _ = hasher.Write([]byte{0})
	}
	revision := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	return config.NewSnapshot(revision, values)
}

// Watch polls the selected environment snapshot and emits revisions.
func (source *Source) Watch(ctx context.Context) (config.Watcher, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	initial, err := source.Load(ctx)
	if err != nil {
		return nil, err
	}
	return &watcher{
		source:       source,
		lastRevision: initial.Revision(),
		closed:       make(chan struct{}),
	}, nil
}

func (source *Source) path(key string) ([]string, error) {
	relative := strings.TrimPrefix(key, source.prefix)
	relative = strings.TrimPrefix(relative, "_")
	if relative == "" {
		return nil, fmt.Errorf("%w: %s has an empty path", ErrInvalidOption, key)
	}
	segments := strings.Split(relative, source.separator)
	for index, segment := range segments {
		segment = strings.ToLower(strings.TrimSpace(segment))
		if segment == "" {
			return nil, fmt.Errorf(
				"%w: %s has an empty path segment",
				ErrInvalidOption,
				key,
			)
		}
		segments[index] = segment
	}
	return segments, nil
}

type watcher struct {
	source       *Source
	lastRevision string
	closed       chan struct{}
	closeOnce    sync.Once
}

func (watcher *watcher) Next(ctx context.Context) (config.Snapshot, error) {
	if ctx == nil {
		return config.Snapshot{}, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	ticker := time.NewTicker(watcher.source.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return config.Snapshot{}, context.Cause(ctx)
		case <-watcher.closed:
			return config.Snapshot{}, config.ErrWatcherClosed
		case <-ticker.C:
			snapshot, err := watcher.source.Load(ctx)
			if err != nil {
				return config.Snapshot{}, err
			}
			if snapshot.Revision() == watcher.lastRevision {
				continue
			}
			watcher.lastRevision = snapshot.Revision()
			return snapshot, nil
		}
	}
}

func (watcher *watcher) Close() error {
	watcher.closeOnce.Do(func() { close(watcher.closed) })
	return nil
}

func set(values map[string]any, path []string, value any) {
	current := values
	for _, segment := range path[:len(path)-1] {
		next, ok := current[segment].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[segment] = next
		}
		current = next
	}
	current[path[len(path)-1]] = value
}
