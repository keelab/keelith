package projection

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	goruntime "runtime"
	"sync"
	"time"
)

const defaultWaitPollInterval = 100 * time.Millisecond

var (
	// ErrInvalidReplica reports incomplete typed replica dependencies or policy.
	ErrInvalidReplica = errors.New("projection: invalid replica")
	// ErrStale reports a checkpoint older than the requested freshness bound.
	ErrStale = errors.New("projection: stale replica")
)

// StaleError describes a freshness-policy rejection without exposing row keys.
type StaleError struct {
	Projection ProjectionID
	Cursor     Cursor
	Maximum    time.Duration
	Age        time.Duration
}

// Error implements error.
func (stale *StaleError) Error() string {
	if stale == nil {
		return ErrStale.Error()
	}
	return fmt.Sprintf(
		"%s: projection %q cursor %q age %s exceeds %s",
		ErrStale,
		stale.Projection,
		stale.Cursor,
		stale.Age,
		stale.Maximum,
	)
}

// Unwrap supports errors.Is with ErrStale.
func (*StaleError) Unwrap() error {
	return ErrStale
}

// KeyEncoder maps a typed lookup key to its stable wire encoding.
type KeyEncoder[K any] func(K) ([]byte, error)

// ValueDecoder constructs one typed value from an independent payload copy.
type ValueDecoder[V any] func([]byte) (V, error)

// ReplicaOption configures typed read and wait behavior.
type ReplicaOption interface {
	applyReplica(*replicaSettings) error
}

type replicaOptionFunc func(*replicaSettings) error

func (function replicaOptionFunc) applyReplica(settings *replicaSettings) error {
	return function(settings)
}

type replicaSettings struct {
	clock        func() time.Time
	pollInterval time.Duration
}

// WithReplicaClock replaces the freshness clock.
func WithReplicaClock(clock func() time.Time) ReplicaOption {
	return replicaOptionFunc(func(settings *replicaSettings) error {
		if clock == nil {
			return fmt.Errorf("%w: clock is nil", ErrInvalidReplica)
		}
		settings.clock = clock
		return nil
	})
}

// WithWaitPollInterval sets the durable-checkpoint fallback polling interval.
func WithWaitPollInterval(interval time.Duration) ReplicaOption {
	return replicaOptionFunc(func(settings *replicaSettings) error {
		if interval <= 0 {
			return fmt.Errorf("%w: wait poll interval", ErrInvalidReplica)
		}
		settings.pollInterval = interval
		return nil
	})
}

// ReadOption configures one typed Get.
type ReadOption interface {
	applyRead(*readSettings) error
}

type readOptionFunc func(*readSettings) error

func (function readOptionFunc) applyRead(settings *readSettings) error {
	return function(settings)
}

type readSettings struct {
	maxFreshness time.Duration
	requireFresh bool
}

// RequireFreshWithin rejects reads older than maximum.
func RequireFreshWithin(maximum time.Duration) ReadOption {
	return readOptionFunc(func(settings *readSettings) error {
		if maximum <= 0 {
			return fmt.Errorf("%w: freshness bound", ErrInvalidReplica)
		}
		settings.maxFreshness = maximum
		settings.requireFresh = true
		return nil
	})
}

// CheckpointNotifier wakes readers after an atomic checkpoint update.
type CheckpointNotifier interface {
	NotifyCheckpoint(Checkpoint)
}

// Replica provides strongly typed, generation-consistent local reads.
type Replica[K, V any] struct {
	schema    Schema
	store     Store
	encodeKey KeyEncoder[K]
	decoders  map[string]ValueDecoder[V]
	clock     func() time.Time
	poll      time.Duration

	notifyMu sync.Mutex
	notify   chan struct{}
}

// NewReplica constructs a typed reader over one Store projection.
func NewReplica[K, V any](
	schema Schema,
	store Store,
	encodeKey KeyEncoder[K],
	decode ValueDecoder[V],
	options ...ReplicaOption,
) (*Replica[K, V], error) {
	return NewReplicaWithCompatibleDecoders(
		schema,
		store,
		encodeKey,
		decode,
		nil,
		options...,
	)
}

// NewReplicaWithCompatibleDecoders constructs a typed reader with one decoder
// for the current value fingerprint and one for every explicitly compatible
// historical fingerprint.
func NewReplicaWithCompatibleDecoders[K, V any](
	schema Schema,
	store Store,
	encodeKey KeyEncoder[K],
	current ValueDecoder[V],
	compatible map[string]ValueDecoder[V],
	options ...ReplicaOption,
) (*Replica[K, V], error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	if isNilInterface(store) || encodeKey == nil || current == nil {
		return nil, fmt.Errorf(
			"%w: store, key encoder, or value decoder",
			ErrInvalidReplica,
		)
	}
	if len(compatible) != schema.CompatibleFingerprints.Len() {
		return nil, fmt.Errorf(
			"%w: compatible decoder set",
			ErrInvalidReplica,
		)
	}
	decoders := make(
		map[string]ValueDecoder[V],
		schema.CompatibleFingerprints.Len()+1,
	)
	decoders[schema.Fingerprint] = current
	for fingerprint, decoder := range compatible {
		if !schema.CompatibleFingerprints.Contains(fingerprint) ||
			decoder == nil {
			return nil, fmt.Errorf(
				"%w: compatible decoder",
				ErrInvalidReplica,
			)
		}
		decoders[fingerprint] = decoder
	}
	settings := replicaSettings{
		clock: func() time.Time {
			return time.Now().UTC()
		},
		pollInterval: defaultWaitPollInterval,
	}
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidReplica,
				index,
			)
		}
		if err := option.applyReplica(&settings); err != nil {
			return nil, err
		}
	}
	return &Replica[K, V]{
		schema:    schema,
		store:     store,
		encodeKey: encodeKey,
		decoders:  decoders,
		clock:     settings.clock,
		poll:      settings.pollInterval,
		notify:    make(chan struct{}),
	}, nil
}

// Get reads and decodes a value from one stable visible generation.
func (replica *Replica[K, V]) Get(
	ctx context.Context,
	key K,
	options ...ReadOption,
) (V, bool, error) {
	var zero V
	if replica == nil {
		return zero, false, fmt.Errorf("%w: replica is nil", ErrInvalidReplica)
	}
	if err := validateReplicaContext(ctx); err != nil {
		return zero, false, err
	}
	settings := readSettings{}
	for index, option := range options {
		if option == nil {
			return zero, false, fmt.Errorf(
				"%w: read option %d is nil",
				ErrInvalidReplica,
				index,
			)
		}
		if err := option.applyRead(&settings); err != nil {
			return zero, false, err
		}
	}
	encoded, err := replica.encodeKey(key)
	if err != nil {
		return zero, false, fmt.Errorf(
			"projection: encode key: %w",
			err,
		)
	}

	for {
		if err := validateReplicaContext(ctx); err != nil {
			return zero, false, err
		}
		before, exists, err := replica.store.Checkpoint(
			ctx,
			replica.schema.ID,
		)
		if err != nil {
			return zero, false, err
		}
		if !exists {
			return zero, false, nil
		}
		if err := requireMatchingSchema(replica.schema, before.Schema); err != nil {
			return zero, false, err
		}
		payload, found, err := replica.store.Get(
			ctx,
			replica.schema.ID,
			encoded,
		)
		if err != nil {
			return zero, false, err
		}
		after, exists, err := replica.store.Checkpoint(
			ctx,
			replica.schema.ID,
		)
		if err != nil {
			return zero, false, err
		}
		if !exists || before.Generation != after.Generation {
			goruntime.Gosched()
			continue
		}
		if settings.requireFresh {
			age := after.Freshness(replica.clock().UTC())
			if age > settings.maxFreshness {
				return zero, false, &StaleError{
					Projection: replica.schema.ID,
					Cursor:     after.Cursor,
					Maximum:    settings.maxFreshness,
					Age:        age,
				}
			}
		}
		if !found {
			return zero, false, nil
		}
		decode := replica.decoders[before.Schema.Fingerprint]
		if decode == nil {
			return zero, false, fmt.Errorf(
				"%w: decoder for checkpoint schema",
				ErrInvalidReplica,
			)
		}
		value, err := decode(payload)
		if err != nil {
			return zero, false, fmt.Errorf(
				"projection: decode value: %w",
				err,
			)
		}
		return value, true, nil
	}
}

// WaitUntil waits until the durable checkpoint equals target.
func (replica *Replica[K, V]) WaitUntil(
	ctx context.Context,
	target Cursor,
) (Checkpoint, error) {
	if replica == nil {
		return Checkpoint{}, fmt.Errorf("%w: replica is nil", ErrInvalidReplica)
	}
	if err := validateReplicaContext(ctx); err != nil {
		return Checkpoint{}, err
	}
	if err := target.Validate(); err != nil {
		return Checkpoint{}, err
	}
	ticker := time.NewTicker(replica.poll)
	defer ticker.Stop()
	for {
		notification := replica.notification()
		checkpoint, exists, err := replica.store.Checkpoint(
			ctx,
			replica.schema.ID,
		)
		if err != nil {
			return Checkpoint{}, err
		}
		if exists {
			if err := requireMatchingSchema(
				replica.schema,
				checkpoint.Schema,
			); err != nil {
				return Checkpoint{}, err
			}
			if checkpoint.Cursor == target {
				return checkpoint, nil
			}
		}
		select {
		case <-notification:
		case <-ticker.C:
		case <-ctx.Done():
			return Checkpoint{}, context.Cause(ctx)
		}
	}
}

// NotifyCheckpoint broadcasts one committed checkpoint transition.
func (replica *Replica[K, V]) NotifyCheckpoint(checkpoint Checkpoint) {
	if replica == nil || checkpoint.Schema.ID != replica.schema.ID {
		return
	}
	replica.notifyMu.Lock()
	close(replica.notify)
	replica.notify = make(chan struct{})
	replica.notifyMu.Unlock()
}

func (replica *Replica[K, V]) notification() <-chan struct{} {
	replica.notifyMu.Lock()
	defer replica.notifyMu.Unlock()
	return replica.notify
}

func requireMatchingSchema(expected, actual Schema) error {
	return expected.Accepts(actual)
}

func validateReplicaContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidReplica)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return nil
}

func isNilInterface(value any) bool {
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
