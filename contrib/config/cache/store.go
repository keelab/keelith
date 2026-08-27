// Package cache provides encrypted, atomic last-good config snapshots.
package cache

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
	"time"

	"github.com/keelab/keelith/config"
)

var (
	// ErrInvalidOption reports an invalid store dependency or setting.
	ErrInvalidOption = errors.New("config cache: invalid option")
	// ErrNotFound reports that no last-good snapshot exists.
	ErrNotFound = errors.New("config cache: snapshot not found")
	// ErrExpired reports that a cached snapshot exceeded its allowed age.
	ErrExpired = errors.New("config cache: snapshot expired")
	// ErrCorrupt reports failed format, checksum, or cipher validation.
	ErrCorrupt = errors.New("config cache: corrupt snapshot")
	// ErrClosed reports an operation after Source shutdown.
	ErrClosed = errors.New("config cache: closed")
)

const (
	storeFormatVersion = 1
	defaultMaxBytes    = 16 << 20
)

// Clock supplies deterministic cache age checks.
type Clock interface {
	Now() time.Time
}

// StoreConfig controls encrypted snapshot persistence.
type StoreConfig struct {
	Path     string
	TTL      time.Duration
	MaxBytes int64
	Cipher   Cipher
	Clock    Clock
}

// Store persists exactly one encrypted last-good Snapshot.
type Store struct {
	path     string
	TTL      time.Duration
	maxBytes int64
	cipher   Cipher
	clock    Clock
}

type envelope struct {
	Version    int    `json:"version"`
	SavedAt    int64  `json:"saved_at_unix_nano"`
	Revision   string `json:"revision"`
	Checksum   string `json:"sha256"`
	Nonce      []byte `json:"nonce"`
	Ciphertext []byte `json:"ciphertext"`
}

// NewStore constructs an encrypted last-good Snapshot store.
func NewStore(settings StoreConfig) (*Store, error) {
	path := filepath.Clean(strings.TrimSpace(settings.Path))
	if path == "." || path == "" {
		return nil, fmt.Errorf("%w: path is empty", ErrInvalidOption)
	}
	if settings.TTL <= 0 {
		return nil, fmt.Errorf("%w: ttl must be positive", ErrInvalidOption)
	}
	if isNil(settings.Cipher) {
		return nil, fmt.Errorf("%w: cipher is required", ErrInvalidOption)
	}
	maxBytes := settings.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxBytes
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("%w: max bytes must be positive", ErrInvalidOption)
	}
	clock := settings.Clock
	if isNil(clock) {
		clock = systemClock{}
	}
	return &Store{
		path:     path,
		TTL:      settings.TTL,
		maxBytes: maxBytes,
		cipher:   settings.Cipher,
		clock:    clock,
	}, nil
}

// Save encrypts and atomically replaces the last-good Snapshot.
func (store *Store) Save(
	ctx context.Context,
	snapshot config.Snapshot,
) error {
	if store == nil {
		return fmt.Errorf("%w: store is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	plaintext, err := config.MarshalSnapshot(snapshot)
	if err != nil {
		return err
	}
	if int64(len(plaintext)) > store.maxBytes {
		return fmt.Errorf("%w: snapshot exceeds max bytes", ErrInvalidOption)
	}
	checksum := sha256.Sum256(plaintext)
	nonce, ciphertext, err := store.cipher.Seal(plaintext)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(envelope{
		Version:    storeFormatVersion,
		SavedAt:    store.clock.Now().UnixNano(),
		Revision:   snapshot.Revision(),
		Checksum:   hex.EncodeToString(checksum[:]),
		Nonce:      nonce,
		Ciphertext: ciphertext,
	})
	if err != nil {
		return fmt.Errorf("config cache: encode envelope: %w", err)
	}
	if int64(len(payload)) > store.maxBytes {
		return fmt.Errorf("%w: encrypted snapshot exceeds max bytes", ErrInvalidOption)
	}
	return atomicWrite(store.path, payload)
}

// Load authenticates, validates, and age-checks the last-good Snapshot.
func (store *Store) Load(ctx context.Context) (config.Snapshot, error) {
	if store == nil {
		return config.Snapshot{}, fmt.Errorf("%w: store is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return config.Snapshot{}, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return config.Snapshot{}, cause
	}
	info, err := os.Lstat(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return config.Snapshot{}, ErrNotFound
	}
	if err != nil {
		return config.Snapshot{}, fmt.Errorf("config cache: stat: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return config.Snapshot{}, fmt.Errorf("%w: cache path is not a regular file", ErrCorrupt)
	}
	if info.Size() > store.maxBytes {
		return config.Snapshot{}, fmt.Errorf("%w: file exceeds max bytes", ErrCorrupt)
	}
	payload, err := os.ReadFile(store.path)
	if err != nil {
		return config.Snapshot{}, fmt.Errorf("config cache: read: %w", err)
	}
	var cached envelope
	if err := json.Unmarshal(payload, &cached); err != nil {
		return config.Snapshot{}, fmt.Errorf("%w: envelope: %w", ErrCorrupt, err)
	}
	if cached.Version != storeFormatVersion ||
		cached.SavedAt <= 0 ||
		strings.TrimSpace(cached.Revision) == "" {
		return config.Snapshot{}, fmt.Errorf("%w: invalid envelope fields", ErrCorrupt)
	}
	savedAt := time.Unix(0, cached.SavedAt)
	now := store.clock.Now()
	if savedAt.After(now.Add(time.Minute)) {
		return config.Snapshot{}, fmt.Errorf("%w: saved time is in the future", ErrCorrupt)
	}
	if now.Sub(savedAt) > store.TTL {
		return config.Snapshot{}, ErrExpired
	}
	plaintext, err := store.cipher.Open(cached.Nonce, cached.Ciphertext)
	if err != nil {
		return config.Snapshot{}, err
	}
	checksum := sha256.Sum256(plaintext)
	if !strings.EqualFold(cached.Checksum, hex.EncodeToString(checksum[:])) {
		return config.Snapshot{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	snapshot, err := config.UnmarshalSnapshot(plaintext)
	if err != nil {
		return config.Snapshot{}, fmt.Errorf("%w: payload: %w", ErrCorrupt, err)
	}
	if snapshot.Revision() != cached.Revision {
		return config.Snapshot{}, fmt.Errorf("%w: revision mismatch", ErrCorrupt)
	}
	return snapshot, nil
}

func atomicWrite(path string, payload []byte) (resultErr error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("config cache: create directory: %w", err)
	}
	file, err := os.CreateTemp(directory, ".keelith-config-*")
	if err != nil {
		return fmt.Errorf("config cache: create temporary file: %w", err)
	}
	temporary := file.Name()
	defer func() {
		if resultErr != nil {
			_ = file.Close()
			_ = os.Remove(temporary)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("config cache: chmod temporary file: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		return fmt.Errorf("config cache: write temporary file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("config cache: sync temporary file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("config cache: close temporary file: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("config cache: atomic rename: %w", err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("config cache: open directory: %w", err)
	}
	defer func() { _ = directoryFile.Close() }()
	if err := directoryFile.Sync(); err != nil {
		return fmt.Errorf("config cache: sync directory: %w", err)
	}
	return nil
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

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}
