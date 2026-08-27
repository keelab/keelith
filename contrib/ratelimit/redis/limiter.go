// Package redis provides an atomic Redis token bucket for Keelith's
// distributed rate-limit contract.
package redis

import (
	"context"
	"encoding/base64"
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

	"github.com/keelab/keelith/governance/ratelimit"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultPrefix = "keelith:ratelimit:"
	tokenScale    = int64(1_000_000)
	maxSubjectLen = 256
	maxStatettl   = 30 * 24 * time.Hour
)

var (
	// ErrInvalidOption reports an invalid backend or key prefix.
	ErrInvalidOption = errors.New("redis ratelimit: invalid option")
	// ErrInvalidQuota reports an unsupported key, rate, burst, or cost.
	ErrInvalidQuota = errors.New("redis ratelimit: invalid quota")
	// ErrInvalidReply reports a malformed Lua script response.
	ErrInvalidReply = errors.New("redis ratelimit: invalid script reply")
	// ErrClosed reports an operation after Client shutdown.
	ErrClosed = errors.New("redis ratelimit: client closed")
)

// Backend is the deliberately small Redis command surface used by Client.
type Backend interface {
	Ping(context.Context) error
	Time(context.Context) (time.Time, error)
	RunTokenBucket(context.Context, string, ...int64) (any, error)
	Close() error
}

// Config controls Redis key namespacing and backend ownership.
type Config struct {
	Prefix string
	Owns   bool
}

// Description is a low-cardinality backend snapshot.
type Description struct {
	Closed   bool
	Allowed  uint64
	Rejected uint64
	Errors   uint64
}

// Client implements ratelimit.DistributedBackend.
type Client struct {
	backend Backend
	prefix  string
	owns    bool

	mu       sync.Mutex
	closed   bool
	allowed  uint64
	rejected uint64
	failures uint64

	closeOnce sync.Once
	closeErr  error
}

// New creates an owned go-redis UniversalClient.
func New(
	options *goredis.UniversalOptions,
	config Config,
) (*Client, error) {
	if options == nil {
		return nil, fmt.Errorf("%w: Redis options are nil", ErrInvalidOption)
	}
	raw := goredis.NewUniversalClient(options)
	config.Owns = true
	client, err := Wrap(&goRedisBackend{client: raw}, config)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return client, nil
}

// FromClient reuses an externally owned UniversalClient. The returned quota
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

// Wrap adapts a custom backend with explicit lifecycle ownership.
func Wrap(backend Backend, config Config) (*Client, error) {
	if isNil(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	prefix := strings.TrimSpace(config.Prefix)
	if prefix == "" {
		prefix = defaultPrefix
	}
	if len(prefix) > 128 ||
		!utf8.ValidString(prefix) ||
		containsControl(prefix) {
		return nil, fmt.Errorf("%w: prefix is invalid", ErrInvalidOption)
	}
	return &Client{
		backend: backend,
		prefix:  prefix,
		owns:    config.Owns,
	}, nil
}

// Start verifies Redis connectivity inside App startup rollback.
func (client *Client) Start(ctx context.Context) error {
	if client == nil || ctx == nil {
		return fmt.Errorf("%w: client or context is nil", ErrInvalidOption)
	}
	if err := client.requireOpen(); err != nil {
		return err
	}
	if err := client.backend.Ping(ctx); err != nil {
		client.recordError(err)
		return fmt.Errorf("redis ratelimit: ping: %w", err)
	}
	return nil
}

// Shutdown rejects future decisions and closes an owned backend once.
func (client *Client) Shutdown(ctx context.Context) error {
	if client == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	client.closeOnce.Do(func() {
		client.mu.Lock()
		client.closed = true
		client.mu.Unlock()
		if client.owns {
			client.closeErr = client.backend.Close()
		}
	})
	return client.closeErr
}

// Allow atomically refills and consumes one distributed token bucket.
func (client *Client) Allow(
	ctx context.Context,
	quota ratelimit.Quota,
) (ratelimit.Decision, error) {
	if client == nil || ctx == nil {
		return ratelimit.Decision{}, fmt.Errorf(
			"%w: client or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return ratelimit.Decision{}, cause
	}
	if err := client.requireOpen(); err != nil {
		return ratelimit.Decision{}, err
	}
	parameters, err := tokenParameters(quota)
	if err != nil {
		return ratelimit.Decision{}, err
	}
	now, err := client.backend.Time(ctx)
	if err != nil {
		client.recordError(err)
		return ratelimit.Decision{}, fmt.Errorf("redis ratelimit: TIME: %w", err)
	}
	if now.UnixMicro() < 0 {
		err := fmt.Errorf("%w: server time is before Unix epoch", ErrInvalidReply)
		client.recordError(err)
		return ratelimit.Decision{}, err
	}
	key := client.prefix + base64.RawURLEncoding.EncodeToString(
		[]byte(quota.Key),
	)
	response, err := client.backend.RunTokenBucket(
		ctx,
		key,
		now.UnixMicro(),
		parameters.rate,
		parameters.capacity,
		parameters.cost,
		parameters.ttlMilliseconds,
	)
	if err != nil {
		client.recordError(err)
		return ratelimit.Decision{}, fmt.Errorf("redis ratelimit: script: %w", err)
	}
	decision, err := parseDecision(response)
	if err != nil {
		client.recordError(err)
		return ratelimit.Decision{}, err
	}
	client.recordDecision(decision.Allowed)
	return decision, nil
}

// Describe returns counters without quota Subject keys.
func (client *Client) Describe() Description {
	if client == nil {
		return Description{Closed: true}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return Description{
		Closed:   client.closed,
		Allowed:  client.allowed,
		Rejected: client.rejected,
		Errors:   client.failures,
	}
}

type tokenArgs struct {
	rate            int64
	capacity        int64
	cost            int64
	ttlMilliseconds int64
}

func tokenParameters(quota ratelimit.Quota) (tokenArgs, error) {
	if strings.TrimSpace(quota.Key) != quota.Key ||
		quota.Key == "" ||
		len(quota.Key) > maxSubjectLen ||
		!utf8.ValidString(quota.Key) ||
		containsControl(quota.Key) ||
		quota.Rate <= 0 ||
		math.IsNaN(quota.Rate) ||
		math.IsInf(quota.Rate, 0) ||
		quota.Burst <= 0 ||
		quota.Cost <= 0 ||
		quota.Cost > quota.Burst {
		return tokenArgs{}, fmt.Errorf("%w: quota fields are invalid", ErrInvalidQuota)
	}
	rateValue := quota.Rate * float64(tokenScale)
	if rateValue < 1 || rateValue > float64(math.MaxInt64) {
		return tokenArgs{}, fmt.Errorf("%w: rate precision is unsupported", ErrInvalidQuota)
	}
	if int64(quota.Burst) > math.MaxInt64/tokenScale ||
		int64(quota.Cost) > math.MaxInt64/tokenScale {
		return tokenArgs{}, fmt.Errorf("%w: burst or cost overflows", ErrInvalidQuota)
	}
	refillMilliseconds := math.Ceil(
		float64(quota.Burst) / quota.Rate * 1000,
	)
	ttlMilliseconds := refillMilliseconds*2 + 1000
	if math.IsInf(ttlMilliseconds, 0) ||
		ttlMilliseconds <= 0 ||
		ttlMilliseconds > float64(maxStatettl.Milliseconds()) {
		return tokenArgs{}, fmt.Errorf("%w: state ttl is unsupported", ErrInvalidQuota)
	}
	return tokenArgs{
		rate:            int64(math.Round(rateValue)),
		capacity:        int64(quota.Burst) * tokenScale,
		cost:            int64(quota.Cost) * tokenScale,
		ttlMilliseconds: int64(math.Ceil(ttlMilliseconds)),
	}, nil
}

func parseDecision(response any) (ratelimit.Decision, error) {
	values, ok := response.([]any)
	if !ok || len(values) != 3 {
		return ratelimit.Decision{}, fmt.Errorf(
			"%w: expected three-element array, got %T",
			ErrInvalidReply,
			response,
		)
	}
	allowed, err := integer(values[0])
	if err != nil || allowed != 0 && allowed != 1 {
		return ratelimit.Decision{}, fmt.Errorf(
			"%w: allowed value",
			ErrInvalidReply,
		)
	}
	remaining, err := integer(values[1])
	if err != nil || remaining < 0 {
		return ratelimit.Decision{}, fmt.Errorf(
			"%w: remaining value",
			ErrInvalidReply,
		)
	}
	retryMicroseconds, err := integer(values[2])
	if err != nil || retryMicroseconds < 0 {
		return ratelimit.Decision{}, fmt.Errorf(
			"%w: retry value",
			ErrInvalidReply,
		)
	}
	return ratelimit.Decision{
		Allowed:    allowed == 1,
		Remaining:  float64(remaining) / float64(tokenScale),
		RetryAfter: time.Duration(retryMicroseconds) * time.Microsecond,
	}, nil
}

func integer(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case int:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unsupported integer %T", value)
	}
}

func (client *Client) requireOpen() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return ErrClosed
	}
	return nil
}

func (client *Client) recordDecision(allowed bool) {
	client.mu.Lock()
	if allowed {
		client.allowed++
	} else {
		client.rejected++
	}
	client.mu.Unlock()
}

func (client *Client) recordError(error) {
	client.mu.Lock()
	client.failures++
	client.mu.Unlock()
}

func containsControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ ratelimit.DistributedBackend = (*Client)(nil)
