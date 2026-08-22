// Package file provides JSON and YAML file-backed configuration snapshots.
package file

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/config"
	"gopkg.in/yaml.v3"
)

const (
	defaultMaxBytes     = 4 * 1024 * 1024
	defaultPollInterval = 500 * time.Millisecond
)

var (
	// ErrInvalidOption reports an invalid file source option.
	ErrInvalidOption = errors.New("config/file: invalid option")
	// ErrTooLarge reports a file exceeding the configured byte budget.
	ErrTooLarge = errors.New("config/file: file is too large")
	// ErrUnsupportedFormat reports an unknown or ambiguous file format.
	ErrUnsupportedFormat = errors.New("config/file: unsupported format")
)

// Format identifies the on-disk configuration syntax.
type Format string

const (
	// FormatAuto derives JSON or YAML from the file extension.
	FormatAuto Format = ""
	// FormatJSON decodes one JSON object.
	FormatJSON Format = "json"
	// FormatYAML decodes one YAML document.
	FormatYAML Format = "yaml"
)

// Option configures a Source.
type Option interface {
	apply(*options) error
}

type optionFunc func(*options) error

func (function optionFunc) apply(options *options) error {
	return function(options)
}

type options struct {
	format       Format
	maxBytes     int64
	pollInterval time.Duration
}

// WithFormat explicitly selects JSON or YAML.
func WithFormat(format Format) Option {
	return optionFunc(func(options *options) error {
		switch format {
		case FormatAuto, FormatJSON, FormatYAML:
			options.format = format
			return nil
		default:
			return fmt.Errorf("%w: %q", ErrUnsupportedFormat, format)
		}
	})
}

// WithMaxBytes sets the maximum accepted file size.
func WithMaxBytes(maxBytes int64) Option {
	return optionFunc(func(options *options) error {
		if maxBytes <= 0 {
			return fmt.Errorf("%w: max bytes must be positive", ErrInvalidOption)
		}
		options.maxBytes = maxBytes
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

// Source reads complete immutable snapshots from one file.
type Source struct {
	path         string
	format       Format
	maxBytes     int64
	pollInterval time.Duration
}

// New constructs a file Source without reading the file.
func New(path string, optionList ...Option) (*Source, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: path is empty", ErrInvalidOption)
	}
	settings := options{
		maxBytes:     defaultMaxBytes,
		pollInterval: defaultPollInterval,
	}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.apply(&settings); err != nil {
			return nil, fmt.Errorf("config/file: option %d: %w", index, err)
		}
	}
	format := settings.format
	if format == FormatAuto {
		var err error
		format, err = formatFromPath(path)
		if err != nil {
			return nil, err
		}
	}
	return &Source{
		path:         path,
		format:       format,
		maxBytes:     settings.maxBytes,
		pollInterval: settings.pollInterval,
	}, nil
}

// Load reads, decodes, and fingerprints the complete file.
func (source *Source) Load(ctx context.Context) (config.Snapshot, error) {
	if ctx == nil {
		return config.Snapshot{}, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if err := context.Cause(ctx); err != nil {
		return config.Snapshot{}, err
	}
	content, err := readLimited(source.path, source.maxBytes)
	if err != nil {
		return config.Snapshot{}, err
	}
	if err := context.Cause(ctx); err != nil {
		return config.Snapshot{}, err
	}
	values, err := decode(content, source.format)
	if err != nil {
		return config.Snapshot{}, fmt.Errorf(
			"config/file: decode %s: %w",
			source.path,
			err,
		)
	}
	sum := sha256.Sum256(content)
	revision := "sha256:" + hex.EncodeToString(sum[:])
	return config.NewSnapshot(revision, values)
}

// Watch returns a polling watcher that emits only complete changed snapshots.
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

type watcher struct {
	source *Source

	mu           sync.RWMutex
	lastRevision string
	lastError    error
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
				watcher.recordError(err)
				continue
			}
			watcher.mu.Lock()
			watcher.lastError = nil
			if snapshot.Revision() == watcher.lastRevision {
				watcher.mu.Unlock()
				continue
			}
			watcher.lastRevision = snapshot.Revision()
			watcher.mu.Unlock()
			return snapshot, nil
		}
	}
}

func (watcher *watcher) Close() error {
	watcher.closeOnce.Do(func() { close(watcher.closed) })
	return nil
}

// LastError returns the most recent transient read/decode failure observed
// while Watch retained the last-good snapshot.
func (watcher *watcher) LastError() error {
	watcher.mu.RLock()
	defer watcher.mu.RUnlock()
	return watcher.lastError
}

func (watcher *watcher) recordError(err error) {
	watcher.mu.Lock()
	watcher.lastError = err
	watcher.mu.Unlock()
}

func formatFromPath(path string) (Format, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return FormatJSON, nil
	case ".yaml", ".yml":
		return FormatYAML, nil
	default:
		return FormatAuto, fmt.Errorf(
			"%w: cannot infer from %q",
			ErrUnsupportedFormat,
			path,
		)
	}
}

func readLimited(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config/file: open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("config/file: read %s: %w", path, err)
	}
	if int64(len(content)) > maxBytes {
		return nil, fmt.Errorf("%w: %s exceeds %d bytes", ErrTooLarge, path, maxBytes)
	}
	return content, nil
}

func decode(content []byte, format Format) (map[string]any, error) {
	values := make(map[string]any)
	switch format {
	case FormatJSON:
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.UseNumber()
		if err := decoder.Decode(&values); err != nil {
			return nil, err
		}
		if err := requireEOFJSON(decoder); err != nil {
			return nil, err
		}
	case FormatYAML:
		decoder := yaml.NewDecoder(bytes.NewReader(content))
		if err := decoder.Decode(&values); err != nil {
			return nil, err
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			if err == nil {
				return nil, errors.New("multiple YAML documents are not supported")
			}
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedFormat, format)
	}
	if values == nil {
		return map[string]any{}, nil
	}
	return values, nil
}

func requireEOFJSON(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not supported")
		}
		return err
	}
	return nil
}
