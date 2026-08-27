// Package redis provides a lifecycle-owned Redis cache adapter.
package redis

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/cache"
	goredis "github.com/redis/go-redis/v9"
)

var (
	// ErrInvalidOption reports an invalid backend, option, or cache key.
	ErrInvalidOption = errors.New("redis cache: invalid option")
	// ErrNotFound reports an absent cache key without leaking go-redis types.
	ErrNotFound = cache.ErrMiss
	// ErrHashTagRequired reports a versioned key that is not cluster-safe.
	ErrHashTagRequired = errors.New(
		"redis cache: versioned key requires a hash tag",
	)
)

const defaultVersionttl = 7 * 24 * time.Hour

const setIfVersionScript = `
local current = redis.call('GET', KEYS[2])
if not current then
  current = '00000000000000000000'
end
if string.len(current) ~= 20 then
  return redis.error_reply('keelith cache version is corrupt')
end
if current ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
if current ~= '00000000000000000000' then
  redis.call('PEXPIRE', KEYS[2], ARGV[4])
end
return 1
`

const applyInvalidationScript = `
local current = redis.call('GET', KEYS[2])
if not current then
  current = '00000000000000000000'
end
if string.len(current) ~= 20 then
  return redis.error_reply('keelith cache version is corrupt')
end
if current > ARGV[1] then
  return 3
end
if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[2], ARGV[2])
  return 2
end
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[2])
redis.call('DEL', KEYS[1])
return 1
`

var _ cache.VersionedBackend = (*Client)(nil)

// Backend is the deliberately small Redis command surface used by Client.
type Backend interface {
	Ping(context.Context) error
	Get(context.Context, string) ([]byte, error)
	Set(context.Context, string, []byte, time.Duration) error
	Delete(context.Context, ...string) (int64, error)
	Eval(context.Context, string, []string, ...any) (any, error)
	Close() error
}

// Config controls key namespacing and backend ownership.
type Config struct {
	Prefix     string        `config:"prefix"`
	Owns       bool          `config:"owns"`
	Versionttl time.Duration `config:"versionttl"`
}

// Client is a small cache contract with explicit lifecycle ownership.
type Client struct {
	backend    Backend
	prefix     string
	owns       bool
	versionttl time.Duration

	closeOnce sync.Once
	closeErr  error
}

// New creates and owns an official go-redis UniversalClient.
func New(options *goredis.UniversalOptions, prefix string) (*Client, error) {
	if options == nil {
		return nil, fmt.Errorf("%w: options are nil", ErrInvalidOption)
	}
	raw := goredis.NewUniversalClient(options)
	client, err := Wrap(&goRedisBackend{client: raw}, Config{
		Prefix: prefix,
		Owns:   true,
	})
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return client, nil
}

// FromClient reuses an externally owned UniversalClient. The returned cache
// adapter never closes the shared Redis connection pool.
func FromClient(
	client goredis.UniversalClient,
	config Config,
) (*Client, error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	config.Owns = false
	return Wrap(&goRedisBackend{client: client}, config)
}

// Wrap adapts a custom backend. Callers explicitly choose whether Shutdown
// closes the backend.
func Wrap(backend Backend, config Config) (*Client, error) {
	if isNil(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	prefix := strings.TrimSpace(config.Prefix)
	versionttl := config.Versionttl
	if versionttl == 0 {
		versionttl = defaultVersionttl
	}
	return &Client{
		backend:    backend,
		prefix:     prefix,
		owns:       config.Owns,
		versionttl: versionttl,
	}, nil
}

// ValidateConfig validates cache key namespacing and ownership settings.
func ValidateConfig(config Config) error {
	prefix := strings.TrimSpace(config.Prefix)
	if prefix != config.Prefix ||
		len(prefix) > 512 ||
		!utf8.ValidString(prefix) {
		return fmt.Errorf("%w: prefix is invalid", ErrInvalidOption)
	}
	for _, character := range prefix {
		if unicode.IsControl(character) {
			return fmt.Errorf(
				"%w: prefix contains control characters",
				ErrInvalidOption,
			)
		}
	}
	if config.Versionttl < 0 ||
		config.Versionttl > 0 && config.Versionttl < time.Millisecond {
		return fmt.Errorf("%w: version ttl is invalid", ErrInvalidOption)
	}
	return nil
}

// Start verifies connectivity inside the App startup rollback boundary.
func (client *Client) Start(ctx context.Context) error {
	if client == nil {
		return fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	if err := client.backend.Ping(ctx); err != nil {
		return fmt.Errorf("redis cache: ping: %w", err)
	}
	return nil
}

// Shutdown closes an owned backend exactly once.
func (client *Client) Shutdown(context.Context) error {
	if client == nil || !client.owns {
		return nil
	}
	client.closeOnce.Do(func() {
		client.closeErr = client.backend.Close()
	})
	return client.closeErr
}

// Get returns a defensive copy of a cached value.
func (client *Client) Get(ctx context.Context, key string) ([]byte, error) {
	qualified, err := client.key(key)
	if err != nil {
		return nil, err
	}
	value, err := client.backend.Get(ctx, qualified)
	if errors.Is(err, goredis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("redis cache: get: %w", err)
	}
	return append([]byte(nil), value...), nil
}

// Set writes a defensive copy with an optional ttl.
func (client *Client) Set(
	ctx context.Context,
	key string,
	value []byte,
	ttl time.Duration,
) error {
	qualified, err := client.key(key)
	if err != nil {
		return err
	}
	if ttl < 0 {
		return fmt.Errorf("%w: ttl is negative", ErrInvalidOption)
	}
	if err := client.backend.Set(
		ctx,
		qualified,
		append([]byte(nil), value...),
		ttl,
	); err != nil {
		return fmt.Errorf("redis cache: set: %w", err)
	}
	return nil
}

// Delete removes keys and returns the number removed.
func (client *Client) Delete(
	ctx context.Context,
	keys ...string,
) (int64, error) {
	if len(keys) == 0 {
		return 0, fmt.Errorf("%w: no keys", ErrInvalidOption)
	}
	qualified := make([]string, len(keys))
	for index, key := range keys {
		value, err := client.key(key)
		if err != nil {
			return 0, err
		}
		qualified[index] = value
	}
	count, err := client.backend.Delete(ctx, qualified...)
	if err != nil {
		return 0, fmt.Errorf("redis cache: delete: %w", err)
	}
	return count, nil
}

// CurrentVersion returns a per-key monotonic invalidation watermark.
func (client *Client) CurrentVersion(
	ctx context.Context,
	key string,
) (uint64, error) {
	_, versionKey, err := client.versionKeys(key)
	if err != nil {
		return 0, err
	}
	value, err := client.backend.Get(ctx, versionKey)
	if errors.Is(err, goredis.Nil) || errors.Is(err, cache.ErrMiss) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("redis cache: get version: %w", err)
	}
	if len(value) != 20 {
		return 0, fmt.Errorf("redis cache: version is corrupt")
	}
	version, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("redis cache: parse version: %w", err)
	}
	return version, nil
}

// SetIfVersion atomically stores only while the remote watermark is unchanged.
func (client *Client) SetIfVersion(
	ctx context.Context,
	key string,
	value []byte,
	ttl time.Duration,
	expected uint64,
) (bool, error) {
	if ttl <= 0 || ttl > client.versionttl {
		return false, fmt.Errorf(
			"%w: ttl must be positive and not exceed version ttl",
			ErrInvalidOption,
		)
	}
	dataKey, versionKey, err := client.versionKeys(key)
	if err != nil {
		return false, err
	}
	result, err := client.backend.Eval(
		ctx,
		setIfVersionScript,
		[]string{dataKey, versionKey},
		formatVersion(expected),
		append([]byte(nil), value...),
		ttl.Milliseconds(),
		client.versionttl.Milliseconds(),
	)
	if err != nil {
		return false, fmt.Errorf("redis cache: set if version: %w", err)
	}
	number, err := integerResult(result)
	if err != nil {
		return false, err
	}
	switch number {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("redis cache: invalid set result")
	}
}

// ApplyInvalidation advances the watermark and deletes a value atomically.
func (client *Client) ApplyInvalidation(
	ctx context.Context,
	key string,
	version uint64,
) (cache.InvalidationState, error) {
	if version == 0 {
		return 0, fmt.Errorf("%w: version is zero", ErrInvalidOption)
	}
	dataKey, versionKey, err := client.versionKeys(key)
	if err != nil {
		return 0, err
	}
	result, err := client.backend.Eval(
		ctx,
		applyInvalidationScript,
		[]string{dataKey, versionKey},
		formatVersion(version),
		client.versionttl.Milliseconds(),
	)
	if err != nil {
		return 0, fmt.Errorf("redis cache: apply invalidation: %w", err)
	}
	number, err := integerResult(result)
	if err != nil {
		return 0, err
	}
	state := cache.InvalidationState(number)
	switch state {
	case cache.InvalidationApplied,
		cache.InvalidationCurrent,
		cache.InvalidationStale:
		return state, nil
	default:
		return 0, fmt.Errorf("redis cache: invalid invalidation result")
	}
}

func (client *Client) key(key string) (string, error) {
	if client == nil || isNil(client.backend) {
		return "", fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	normalized := strings.TrimSpace(key)
	if normalized == "" ||
		normalized != key ||
		len(normalized) > 1024 ||
		!utf8.ValidString(normalized) {
		return "", fmt.Errorf("%w: cache key is invalid", ErrInvalidOption)
	}
	for _, character := range normalized {
		if unicode.IsControl(character) {
			return "", fmt.Errorf("%w: cache key is invalid", ErrInvalidOption)
		}
	}
	return client.prefix + normalized, nil
}

func (client *Client) versionKeys(key string) (string, string, error) {
	dataKey, err := client.key(key)
	if err != nil {
		return "", "", err
	}
	tag, ok := redisHashTag(dataKey)
	if !ok {
		return "", "", ErrHashTagRequired
	}
	sum := sha256.Sum256([]byte(dataKey))
	versionKey := fmt.Sprintf(
		"__keelith_cache_version__{%s}:%x",
		tag,
		sum[:8],
	)
	return dataKey, versionKey, nil
}

type goRedisBackend struct {
	client goredis.UniversalClient
}

func (backend *goRedisBackend) Ping(ctx context.Context) error {
	return backend.client.Ping(ctx).Err()
}

func (backend *goRedisBackend) Get(
	ctx context.Context,
	key string,
) ([]byte, error) {
	return backend.client.Get(ctx, key).Bytes()
}

func (backend *goRedisBackend) Set(
	ctx context.Context,
	key string,
	value []byte,
	ttl time.Duration,
) error {
	return backend.client.Set(ctx, key, value, ttl).Err()
}

func (backend *goRedisBackend) Delete(
	ctx context.Context,
	keys ...string,
) (int64, error) {
	return backend.client.Del(ctx, keys...).Result()
}

func (backend *goRedisBackend) Eval(
	ctx context.Context,
	script string,
	keys []string,
	arguments ...any,
) (any, error) {
	return backend.client.Eval(ctx, script, keys, arguments...).Result()
}

func (backend *goRedisBackend) Close() error {
	return backend.client.Close()
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

func redisHashTag(key string) (string, bool) {
	start := strings.IndexByte(key, '{')
	if start < 0 || start+1 >= len(key) {
		return "", false
	}
	remainder := key[start+1:]
	end := strings.IndexByte(remainder, '}')
	if end <= 0 {
		return "", false
	}
	return remainder[:end], true
}

func formatVersion(version uint64) string {
	return fmt.Sprintf("%020d", version)
}

func integerResult(result any) (int64, error) {
	switch value := result.(type) {
	case int64:
		return value, nil
	case uint64:
		if value > math.MaxInt64 {
			return 0, fmt.Errorf("redis cache: integer result overflows")
		}
		return int64(value), nil
	default:
		return 0, fmt.Errorf("redis cache: invalid integer result")
	}
}
