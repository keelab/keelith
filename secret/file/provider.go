// Package file provides bounded filesystem-backed secrets with polling watch.
package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/secret"
)

const (
	defaultPollInterval = time.Second
	defaultMaxBytes     = int64(1 * 1024 * 1024)
	maxSecretBytes      = int64(16 * 1024 * 1024)
)

var (
	// ErrInvalidOption reports an unsafe root, key, or resource budget.
	ErrInvalidOption = errors.New("secret/file: invalid option")
)

// Config controls one root-confined Provider.
type Config struct {
	Root         string
	PollInterval time.Duration
	MaxBytes     int64
}

// Provider resolves keys as paths below one canonical root.
type Provider struct {
	root         string
	pollInterval time.Duration
	maxBytes     int64
}

// New validates and canonicalizes the secret root.
func New(config Config) (*Provider, error) {
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = defaultMaxBytes
	}
	if config.PollInterval < 100*time.Millisecond ||
		config.PollInterval > time.Minute ||
		config.MaxBytes <= 0 ||
		config.MaxBytes > maxSecretBytes {
		return nil, fmt.Errorf("%w: resource budgets", ErrInvalidOption)
	}
	root := strings.TrimSpace(config.Root)
	if root == "" {
		return nil, fmt.Errorf("%w: root is empty", ErrInvalidOption)
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("%w: absolute root: %w", ErrInvalidOption, err)
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve root: %w", ErrInvalidOption, err)
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("%w: root is not a directory", ErrInvalidOption)
	}
	return &Provider{
		root:         filepath.Clean(canonical),
		pollInterval: config.PollInterval,
		maxBytes:     config.MaxBytes,
	}, nil
}

// Resolve reads one regular file through a root-confined symlink chain.
func (provider *Provider) Resolve(
	ctx context.Context,
	key string,
) (secret.Value, error) {
	if provider == nil || ctx == nil || !validKey(key) {
		return secret.Value{}, fmt.Errorf(
			"%w: provider, context, or key",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return secret.Value{}, cause
	}
	root, err := os.OpenRoot(provider.root)
	if err != nil {
		return secret.Value{}, fmt.Errorf("secret/file: open root: %w", err)
	}
	file, err := root.Open(filepath.FromSlash(key))
	if errors.Is(err, os.ErrNotExist) {
		_ = root.Close()
		return secret.Value{}, secret.ErrNotFound
	}
	if err != nil {
		_ = root.Close()
		return secret.Value{}, fmt.Errorf(
			"%w: root-confined open: %w",
			ErrInvalidOption,
			err,
		)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		_ = root.Close()
		return secret.Value{}, fmt.Errorf("secret/file: stat: %w", statErr)
	}
	if !info.Mode().IsRegular() || info.Size() > provider.maxBytes {
		_ = file.Close()
		_ = root.Close()
		return secret.Value{}, fmt.Errorf(
			"%w: secret is not a bounded regular file",
			ErrInvalidOption,
		)
	}
	content, readErr := io.ReadAll(io.LimitReader(file, provider.maxBytes+1))
	closeErr := file.Close()
	rootCloseErr := root.Close()
	if err := errors.Join(readErr, closeErr, rootCloseErr); err != nil {
		return secret.Value{}, fmt.Errorf("secret/file: read: %w", err)
	}
	if int64(len(content)) > provider.maxBytes {
		return secret.Value{}, fmt.Errorf("%w: secret is too large", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return secret.Value{}, cause
	}
	sum := sha256.Sum256(content)
	value, err := secret.NewValue(
		content,
		"sha256:"+hex.EncodeToString(sum[:]),
		time.Time{},
	)
	if err != nil {
		return secret.Value{}, err
	}
	return value, nil
}

// Watch observes complete content replacements. The first Next blocks until
// the resolved content digest changes.
func (provider *Provider) Watch(
	ctx context.Context,
	key string,
) (secret.Watcher, error) {
	if provider == nil || ctx == nil {
		return nil, fmt.Errorf("%w: provider or context", ErrInvalidOption)
	}
	current, err := provider.Resolve(ctx, key)
	if err != nil {
		return nil, err
	}
	return &watcher{
		provider: provider,
		key:      key,
		version:  current.Version(),
		ticker:   time.NewTicker(provider.pollInterval),
		closed:   make(chan struct{}),
	}, nil
}

type watcher struct {
	provider *Provider
	key      string
	version  string
	ticker   *time.Ticker
	closed   chan struct{}
	once     sync.Once
}

func (watcher *watcher) Next(ctx context.Context) (secret.Value, error) {
	if watcher == nil || ctx == nil {
		return secret.Value{}, fmt.Errorf("%w: watcher or context", ErrInvalidOption)
	}
	for {
		select {
		case <-ctx.Done():
			return secret.Value{}, context.Cause(ctx)
		case <-watcher.closed:
			return secret.Value{}, secret.ErrWatcherClosed
		case <-watcher.ticker.C:
			value, err := watcher.provider.Resolve(ctx, watcher.key)
			if err != nil {
				return secret.Value{}, err
			}
			if value.Version() == watcher.version {
				continue
			}
			watcher.version = value.Version()
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
	})
	return nil
}

func validKey(key string) bool {
	if key == "" ||
		len(key) > 512 ||
		filepath.IsAbs(key) ||
		strings.ContainsRune(key, '\x00') ||
		strings.Contains(key, `\`) {
		return false
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	return clean != "." &&
		clean != ".." &&
		!strings.HasPrefix(clean, ".."+string(filepath.Separator)) &&
		filepath.ToSlash(clean) == key
}
