// Package redis implements renewable distributed leases with Redis.
package redis

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/coordination"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultPrefix   = "keelith:lease:"
	minimumLeaseTTL = 30 * time.Millisecond
)

var (
	// ErrInvalidOption reports an invalid Redis backend or lease config.
	ErrInvalidOption = errors.New("redis coordination: invalid option")
	// ErrClosed reports acquisition after coordinator shutdown.
	ErrClosed = errors.New("redis coordination: closed")
)

var (
	acquireScript = goredis.NewScript(`
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
  return redis.call("INCR", KEYS[2])
end
return 0
`)
	renewScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)
	releaseScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)
)

// Backend isolates lease state from the Redis SDK.
type Backend interface {
	Ping(context.Context) error
	TryAcquire(
		context.Context,
		string,
		string,
		string,
		time.Duration,
	) (fence uint64, acquired bool, err error)
	Renew(context.Context, string, string, time.Duration) (bool, error)
	Release(context.Context, string, string) (bool, error)
	Close() error
}

// Config controls key namespacing and backend ownership.
type Config struct {
	Prefix string `config:"prefix"`
	Owns   bool   `config:"owns"`
}

// Description is a value-free aggregate lease snapshot.
type Description struct {
	Active          int
	Acquired        uint64
	Contended       uint64
	Lost            uint64
	Released        uint64
	BackendFailures uint64
	Closed          bool
}

// Coordinator maintains renewable token-checked Redis leases.
type Coordinator struct {
	backend Backend
	prefix  string
	owns    bool

	mu              sync.Mutex
	active          map[*lease]struct{}
	acquired        uint64
	contended       uint64
	lost            uint64
	released        uint64
	backendFailures uint64
	closed          bool

	closeOnce sync.Once
	closeErr  error
}

// New adapts an official go-redis UniversalClient.
func New(
	client goredis.UniversalClient,
	config Config,
) (*Coordinator, error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	return Wrap(&goRedisBackend{client: client}, config)
}

// Wrap adapts a custom Redis lease Backend.
func Wrap(backend Backend, config Config) (*Coordinator, error) {
	if isNil(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	prefix := strings.TrimSpace(config.Prefix)
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &Coordinator{
		backend: backend,
		prefix:  prefix,
		owns:    config.Owns,
		active:  make(map[*lease]struct{}),
	}, nil
}

// ValidateConfig validates Redis lease namespacing and ownership.
func ValidateConfig(config Config) error {
	prefix := strings.TrimSpace(config.Prefix)
	if prefix == "" {
		prefix = defaultPrefix
	}
	if !validPrefix(prefix) {
		return fmt.Errorf("%w: prefix is malformed", ErrInvalidOption)
	}
	return nil
}

// Start verifies Redis connectivity inside App startup rollback.
func (coordinator *Coordinator) Start(ctx context.Context) error {
	if coordinator == nil || isNil(coordinator.backend) {
		return fmt.Errorf("%w: coordinator is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	coordinator.mu.Lock()
	closed := coordinator.closed
	coordinator.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if err := coordinator.backend.Ping(ctx); err != nil {
		return fmt.Errorf("redis coordination: ping: %w", err)
	}
	return nil
}

// TryAcquire uses SET NX PX and starts automatic token-checked renewal.
func (coordinator *Coordinator) TryAcquire(
	ctx context.Context,
	key string,
	ttl time.Duration,
) (coordination.Lease, bool, error) {
	if coordinator == nil || isNil(coordinator.backend) {
		return nil, false, fmt.Errorf("%w: coordinator is nil", ErrInvalidOption)
	}
	if ctx == nil ||
		!validKey(key) ||
		ttl < minimumLeaseTTL {
		return nil, false, fmt.Errorf(
			"%w: context, key, or ttl",
			coordination.ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, false, cause
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return nil, false, ErrClosed
	}
	coordinator.mu.Unlock()
	token, err := randomToken()
	if err != nil {
		return nil, false, fmt.Errorf("redis coordination: token: %w", err)
	}
	ownerKey, fenceKey := coordinator.keys(key)
	fence, acquired, err := coordinator.backend.TryAcquire(
		ctx,
		ownerKey,
		fenceKey,
		token,
		ttl,
	)
	if err != nil {
		coordinator.recordBackendFailure()
		return nil, false, fmt.Errorf("redis coordination: acquire: %w", err)
	}
	if !acquired {
		coordinator.mu.Lock()
		coordinator.contended++
		coordinator.mu.Unlock()
		return nil, false, nil
	}
	if fence == 0 {
		releaseCtx, cancelRelease := context.WithTimeout(
			context.Background(),
			min(ttl, 5*time.Second),
		)
		_, releaseErr := coordinator.backend.Release(
			releaseCtx,
			ownerKey,
			token,
		)
		cancelRelease()
		return nil, false, errors.Join(
			fmt.Errorf("%w: backend returned an empty fence", ErrInvalidOption),
			releaseErr,
		)
	}
	result := &lease{
		coordinator: coordinator,
		backend:     coordinator.backend,
		key:         ownerKey,
		token:       token,
		fence:       fence,
		TTL:         ttl,
		done:        make(chan struct{}),
		stop:        make(chan struct{}),
		loopDone:    make(chan struct{}),
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		releaseCtx, cancelRelease := context.WithTimeout(
			context.Background(),
			min(ttl, 5*time.Second),
		)
		_, releaseErr := coordinator.backend.Release(
			releaseCtx,
			ownerKey,
			token,
		)
		cancelRelease()
		if releaseErr != nil {
			coordinator.recordBackendFailure()
		}
		return nil, false, errors.Join(ErrClosed, releaseErr)
	}
	coordinator.active[result] = struct{}{}
	coordinator.acquired++
	coordinator.mu.Unlock()
	go result.renew()
	return result, true, nil
}

func (coordinator *Coordinator) keys(key string) (string, string) {
	digest := sha256.Sum256([]byte(key))
	tag := hex.EncodeToString(digest[:16])
	base := coordinator.prefix + "{" + tag + "}"
	return base + ":owner", base + ":fence"
}

// Shutdown releases active leases and closes an owned backend.
func (coordinator *Coordinator) Shutdown(ctx context.Context) error {
	if coordinator == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	coordinator.mu.Lock()
	coordinator.closed = true
	active := make([]*lease, 0, len(coordinator.active))
	for lease := range coordinator.active {
		active = append(active, lease)
	}
	coordinator.mu.Unlock()
	var result error
	for index, lease := range active {
		if err := lease.Release(ctx); err != nil {
			result = errors.Join(result, err)
		}
		if context.Cause(ctx) != nil {
			for _, remaining := range active[index+1:] {
				remaining.abandon(context.Cause(ctx))
			}
			break
		}
	}
	if coordinator.owns {
		coordinator.closeOnce.Do(func() {
			coordinator.closeErr = coordinator.backend.Close()
		})
		result = errors.Join(result, coordinator.closeErr)
	}
	return result
}

// Description returns aggregate counters without keys, tokens, or errors.
func (coordinator *Coordinator) Description() Description {
	if coordinator == nil {
		return Description{}
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return Description{
		Active:          len(coordinator.active),
		Acquired:        coordinator.acquired,
		Contended:       coordinator.contended,
		Lost:            coordinator.lost,
		Released:        coordinator.released,
		BackendFailures: coordinator.backendFailures,
		Closed:          coordinator.closed,
	}
}

func (coordinator *Coordinator) finish(
	subject *lease,
	lost bool,
	released bool,
) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if _, exists := coordinator.active[subject]; !exists {
		return
	}
	delete(coordinator.active, subject)
	if lost {
		coordinator.lost++
	}
	if released {
		coordinator.released++
	}
}

func (coordinator *Coordinator) recordBackendFailure() {
	coordinator.mu.Lock()
	coordinator.backendFailures++
	coordinator.mu.Unlock()
}

type lease struct {
	coordinator *Coordinator
	backend     Backend
	key         string
	token       string
	fence       uint64
	TTL         time.Duration
	done        chan struct{}
	stop        chan struct{}
	loopDone    chan struct{}

	mu          sync.Mutex
	err         error
	releaseErr  error
	doneOnce    sync.Once
	stopOnce    sync.Once
	releaseOnce sync.Once
}

func (lease *lease) Fence() uint64 {
	if lease == nil {
		return 0
	}
	return lease.fence
}

func (lease *lease) Done() <-chan struct{} {
	if lease == nil {
		return closedSignal()
	}
	return lease.done
}

func (lease *lease) Err() error {
	if lease == nil {
		return coordination.ErrLeaseLost
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.err
}

func (lease *lease) Release(ctx context.Context) error {
	if lease == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	lease.releaseOnce.Do(func() {
		lease.stopOnce.Do(func() { close(lease.stop) })
		select {
		case <-lease.loopDone:
		case <-ctx.Done():
			lease.mu.Lock()
			lease.releaseErr = context.Cause(ctx)
			lease.mu.Unlock()
			lease.doneOnce.Do(func() { close(lease.done) })
			lease.coordinator.finish(lease, false, false)
			return
		}
		released, err := lease.backend.Release(ctx, lease.key, lease.token)
		if err != nil {
			lease.coordinator.recordBackendFailure()
			err = fmt.Errorf("redis coordination: release: %w", err)
		} else if !released {
			err = coordination.ErrLeaseLost
		}
		lease.mu.Lock()
		if errors.Is(err, coordination.ErrLeaseLost) && lease.err == nil {
			lease.err = coordination.ErrLeaseLost
		}
		lease.releaseErr = err
		lost := lease.err != nil
		lease.mu.Unlock()
		lease.doneOnce.Do(func() { close(lease.done) })
		lease.coordinator.finish(lease, lost, err == nil)
	})
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return errors.Join(lease.err, lease.releaseErr)
}

func (lease *lease) abandon(cause error) {
	if lease == nil {
		return
	}
	lease.stopOnce.Do(func() { close(lease.stop) })
	lease.mu.Lock()
	if lease.releaseErr == nil {
		lease.releaseErr = cause
	}
	lease.mu.Unlock()
	lease.doneOnce.Do(func() { close(lease.done) })
	lease.coordinator.finish(lease, false, false)
}

func (lease *lease) renew() {
	defer close(lease.loopDone)
	interval := lease.TTL / 3
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastConfirmed := time.Now()
	for {
		select {
		case <-lease.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(
				context.Background(),
				interval,
			)
			renewed, err := lease.backend.Renew(
				ctx,
				lease.key,
				lease.token,
				lease.TTL,
			)
			cancel()
			if err == nil && renewed {
				lastConfirmed = time.Now()
				continue
			}
			if err != nil {
				lease.coordinator.recordBackendFailure()
				safeUntil := lastConfirmed.Add(lease.TTL)
				if time.Until(safeUntil) > interval {
					continue
				}
			}
			lease.lose()
			return
		}
	}
}

func (lease *lease) lose() {
	lease.mu.Lock()
	if lease.err == nil {
		lease.err = coordination.ErrLeaseLost
	}
	lease.mu.Unlock()
	lease.doneOnce.Do(func() { close(lease.done) })
	lease.coordinator.finish(lease, true, false)
}

type goRedisBackend struct {
	client goredis.UniversalClient
}

func (backend *goRedisBackend) Ping(ctx context.Context) error {
	return backend.client.Ping(ctx).Err()
}

func (backend *goRedisBackend) TryAcquire(
	ctx context.Context,
	ownerKey string,
	fenceKey string,
	token string,
	ttl time.Duration,
) (uint64, bool, error) {
	result, err := acquireScript.Run(
		ctx,
		backend.client,
		[]string{ownerKey, fenceKey},
		token,
		ttl.Milliseconds(),
	).Uint64()
	return result, result > 0, err
}

func (backend *goRedisBackend) Renew(
	ctx context.Context,
	key string,
	token string,
	ttl time.Duration,
) (bool, error) {
	result, err := renewScript.Run(
		ctx,
		backend.client,
		[]string{key},
		token,
		ttl.Milliseconds(),
	).Int64()
	return result == 1, err
}

func (backend *goRedisBackend) Release(
	ctx context.Context,
	key string,
	token string,
) (bool, error) {
	result, err := releaseScript.Run(
		ctx,
		backend.client,
		[]string{key},
		token,
	).Int64()
	return result == 1, err
}

func (backend *goRedisBackend) Close() error {
	return backend.client.Close()
}

func randomToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func validPrefix(value string) bool {
	return value != "" &&
		len(value) <= 256 &&
		strings.TrimSpace(value) == value &&
		utf8.ValidString(value) &&
		!strings.ContainsAny(value, "\r\n\x00{}")
}

func validKey(value string) bool {
	if value == "" ||
		len(value) > 512 ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func closedSignal() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
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
