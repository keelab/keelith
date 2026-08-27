// Package redis provides an atomic Redis Store for Keelith request
// idempotency.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/governance/idempotency"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultPrefix         = "keelith:idempotency:"
	defaultMaxResultBytes = 1024 * 1024
)

var (
	// ErrInvalidOption reports an invalid backend or resource bound.
	ErrInvalidOption = errors.New("redis idempotency: invalid option")
	// ErrInvalidReply reports a malformed Lua response.
	ErrInvalidReply = errors.New("redis idempotency: invalid script reply")
	// ErrClosed reports use after Shutdown.
	ErrClosed = errors.New("redis idempotency: closed")
)

// Backend is the deliberately small Redis command surface used by Client.
type Backend interface {
	Ping(context.Context) error
	Eval(context.Context, string, []string, ...any) (any, error)
	Close() error
}

// Config controls Redis namespacing, ownership, and result budget.
type Config struct {
	Prefix         string
	Owns           bool
	MaxResultBytes int
}

// Description is a low-cardinality operational snapshot.
type Description struct {
	Closed      bool
	Acquired    uint64
	InProgress  uint64
	Replayed    uint64
	Conflicts   uint64
	Completed   uint64
	Abandoned   uint64
	StaleOwners uint64
	Errors      uint64
}

// Client implements idempotency.Store and App lifecycle.
type Client struct {
	backend        Backend
	prefix         string
	owns           bool
	maxResultBytes int

	mu          sync.Mutex
	closed      bool
	acquired    uint64
	inProgress  uint64
	replayed    uint64
	conflicts   uint64
	completed   uint64
	abandoned   uint64
	staleOwners uint64
	failures    uint64

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

// FromClient reuses an externally owned UniversalClient.
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
	if len(prefix) > 128 || !utf8.ValidString(prefix) || containsControl(prefix) {
		return nil, fmt.Errorf("%w: prefix", ErrInvalidOption)
	}
	maxResultBytes := config.MaxResultBytes
	if maxResultBytes == 0 {
		maxResultBytes = defaultMaxResultBytes
	}
	if maxResultBytes <= 0 || maxResultBytes > defaultMaxResultBytes {
		return nil, fmt.Errorf("%w: max result bytes", ErrInvalidOption)
	}
	return &Client{
		backend:        backend,
		prefix:         prefix,
		owns:           config.Owns,
		maxResultBytes: maxResultBytes,
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
		client.recordError()
		return fmt.Errorf("redis idempotency: ping: %w", err)
	}
	return nil
}

// Shutdown rejects future operations and closes an owned backend once.
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

// Claim atomically creates or observes one idempotency record.
func (client *Client) Claim(
	ctx context.Context,
	request idempotency.ClaimRequest,
) (idempotency.Claim, error) {
	if client == nil || ctx == nil {
		return idempotency.Claim{}, fmt.Errorf(
			"%w: client or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return idempotency.Claim{}, cause
	}
	if err := client.requireOpen(); err != nil {
		return idempotency.Claim{}, err
	}
	if err := idempotency.ValidateClaimRequest(request); err != nil {
		return idempotency.Claim{}, err
	}
	response, err := client.backend.Eval(
		ctx,
		claimScript,
		[]string{client.key(request.Operation, request.Namespace, request.Key)},
		request.Fingerprint,
		request.Owner,
		request.ProcessingTTL.Milliseconds(),
	)
	if err != nil {
		client.recordError()
		return idempotency.Claim{}, fmt.Errorf("redis idempotency: claim: %w", err)
	}
	claim, err := parseClaim(response, client.maxResultBytes)
	if err != nil {
		client.recordError()
		return idempotency.Claim{}, err
	}
	client.recordClaim(claim.State)
	return claim, nil
}

// Complete publishes one successful result only for the current owner.
func (client *Client) Complete(
	ctx context.Context,
	completion idempotency.Completion,
) error {
	if client == nil || ctx == nil {
		return fmt.Errorf("%w: client or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := client.requireOpen(); err != nil {
		return err
	}
	if err := idempotency.ValidateCompletion(
		completion,
		client.maxResultBytes,
	); err != nil {
		return err
	}
	lease := completion.Lease
	response, err := client.backend.Eval(
		ctx,
		completeScript,
		[]string{client.key(lease.Operation, lease.Namespace, lease.Key)},
		lease.Fingerprint,
		lease.Owner,
		append([]byte(nil), completion.Result...),
		completion.ResultTTL.Milliseconds(),
	)
	if err != nil {
		client.recordError()
		return fmt.Errorf("redis idempotency: complete: %w", err)
	}
	updated, err := integer(response)
	if err != nil || updated != 0 && updated != 1 {
		client.recordError()
		return fmt.Errorf("%w: complete", ErrInvalidReply)
	}
	if updated == 0 {
		client.recordStaleOwner()
		return idempotency.ErrStaleOwner
	}
	client.mu.Lock()
	client.completed++
	client.mu.Unlock()
	return nil
}

// Abandon removes a failed processing claim only for the current owner.
func (client *Client) Abandon(
	ctx context.Context,
	lease idempotency.Lease,
) error {
	if client == nil || ctx == nil {
		return fmt.Errorf("%w: client or context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := client.requireOpen(); err != nil {
		return err
	}
	if err := idempotency.ValidateLease(lease); err != nil {
		return err
	}
	response, err := client.backend.Eval(
		ctx,
		abandonScript,
		[]string{client.key(lease.Operation, lease.Namespace, lease.Key)},
		lease.Fingerprint,
		lease.Owner,
	)
	if err != nil {
		client.recordError()
		return fmt.Errorf("redis idempotency: abandon: %w", err)
	}
	removed, err := integer(response)
	if err != nil || removed != 0 && removed != 1 {
		client.recordError()
		return fmt.Errorf("%w: abandon", ErrInvalidReply)
	}
	if removed == 0 {
		client.recordStaleOwner()
		return idempotency.ErrStaleOwner
	}
	client.mu.Lock()
	client.abandoned++
	client.mu.Unlock()
	return nil
}

// Describe returns counters without keys, fingerprints, owners, or results.
func (client *Client) Describe() Description {
	if client == nil {
		return Description{Closed: true}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return Description{
		Closed:      client.closed,
		Acquired:    client.acquired,
		InProgress:  client.inProgress,
		Replayed:    client.replayed,
		Conflicts:   client.conflicts,
		Completed:   client.completed,
		Abandoned:   client.abandoned,
		StaleOwners: client.staleOwners,
		Errors:      client.failures,
	}
}

func (client *Client) key(operationKey, namespace, identityKey string) string {
	digest := sha256.Sum256([]byte(
		operationKey + "\x00" + namespace + "\x00" + identityKey,
	))
	return client.prefix + base64.RawURLEncoding.EncodeToString(digest[:])
}

func parseClaim(value any, maximumBytes int) (idempotency.Claim, error) {
	values, ok := value.([]any)
	if !ok || len(values) != 3 {
		return idempotency.Claim{}, fmt.Errorf(
			"%w: expected three-element array, got %T",
			ErrInvalidReply,
			value,
		)
	}
	state, err := integer(values[0])
	if err != nil {
		return idempotency.Claim{}, fmt.Errorf("%w: state", ErrInvalidReply)
	}
	result, err := bytesValue(values[1])
	if err != nil || len(result) > maximumBytes {
		return idempotency.Claim{}, fmt.Errorf("%w: result", ErrInvalidReply)
	}
	retryMilliseconds, err := integer(values[2])
	if err != nil || retryMilliseconds < 0 || retryMilliseconds > 15*time.Minute.Milliseconds() {
		return idempotency.Claim{}, fmt.Errorf("%w: retry-after", ErrInvalidReply)
	}
	claim := idempotency.Claim{Result: result}
	switch state {
	case 0:
		claim.State = idempotency.ClaimAcquired
	case 1:
		claim.State = idempotency.ClaimInProgress
		claim.RetryAfter = time.Duration(retryMilliseconds) * time.Millisecond
	case 2:
		claim.State = idempotency.ClaimReplayed
	case 3:
		claim.State = idempotency.ClaimConflict
	default:
		return idempotency.Claim{}, fmt.Errorf("%w: state %d", ErrInvalidReply, state)
	}
	if claim.State != idempotency.ClaimReplayed && len(claim.Result) != 0 ||
		claim.State != idempotency.ClaimInProgress && claim.RetryAfter != 0 {
		return idempotency.Claim{}, fmt.Errorf("%w: state fields", ErrInvalidReply)
	}
	return claim, nil
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

func bytesValue(value any) ([]byte, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case string:
		return []byte(typed), nil
	case []byte:
		return append([]byte(nil), typed...), nil
	default:
		return nil, fmt.Errorf("unsupported bytes %T", value)
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

func (client *Client) recordClaim(state idempotency.ClaimState) {
	client.mu.Lock()
	switch state {
	case idempotency.ClaimAcquired:
		client.acquired++
	case idempotency.ClaimInProgress:
		client.inProgress++
	case idempotency.ClaimReplayed:
		client.replayed++
	case idempotency.ClaimConflict:
		client.conflicts++
	}
	client.mu.Unlock()
}

func (client *Client) recordStaleOwner() {
	client.mu.Lock()
	client.staleOwners++
	client.mu.Unlock()
}

func (client *Client) recordError() {
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
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

type goRedisBackend struct {
	client goredis.UniversalClient
}

func (backend *goRedisBackend) Ping(ctx context.Context) error {
	return backend.client.Ping(ctx).Err()
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

var _ idempotency.Store = (*Client)(nil)
