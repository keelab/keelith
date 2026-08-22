// Package config provides immutable, validated, atomically published
// configuration snapshots.
package config

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidSnapshot means a revision or value tree is invalid.
	ErrInvalidSnapshot = errors.New("config: invalid snapshot")
	// ErrInvalidOption means Manager construction received an invalid option.
	ErrInvalidOption = errors.New("config: invalid option")
	// ErrUnknownField means a snapshot contains a path outside its schema.
	ErrUnknownField = errors.New("config: unknown field")
	// ErrValidation means a Validator rejected a merged snapshot.
	ErrValidation = errors.New("config: validation failed")
	// ErrAlreadyWatching means Watch is already running for a Manager.
	ErrAlreadyWatching = errors.New("config: manager is already watching")
	// ErrWatcherClosed means a Watcher was closed.
	ErrWatcherClosed = errors.New("config: watcher closed")
	// ErrDuplicateSubscriber means a subscriber name is already registered.
	ErrDuplicateSubscriber = errors.New("config: duplicate subscriber")
	// ErrTypedDecode means a component subtree cannot be decoded into its
	// declared Go configuration type.
	ErrTypedDecode = errors.New("config: typed decode failed")
	// ErrRestartRequired means a valid update changed a field outside the
	// component's declared hot-reload boundary.
	ErrRestartRequired = errors.New("config: component restart required")
)

// Source loads and watches complete source-local Snapshots.
type Source interface {
	Load(context.Context) (Snapshot, error)
	Watch(context.Context) (Watcher, error)
}

// Watcher returns complete source-local Snapshots.
type Watcher interface {
	Next(context.Context) (Snapshot, error)
	Close() error
}

// Validator validates one fully merged Snapshot.
type Validator func(Snapshot) error

// Subscriber applies one already-published Snapshot.
//
// A Subscriber may call Current but must not recursively call Load or Watch.
type Subscriber func(context.Context, Snapshot) error

// Binding validates and applies one typed component configuration.
//
// Manager validates all Bindings before publishing a Snapshot, then invokes
// Apply as a named Subscriber.
type Binding interface {
	Name() string
	Validate(Snapshot) error
	Apply(context.Context, Snapshot) error
}

// UnknownFieldPolicy controls schema enforcement.
type UnknownFieldPolicy uint8

const (
	// UnknownAllow accepts paths not present in KnownFields.
	UnknownAllow UnknownFieldPolicy = iota
	// UnknownReject rejects paths not present in KnownFields.
	UnknownReject
)

// SubscriberStatus is the latest apply outcome for one Subscriber.
type SubscriberStatus struct {
	Name            string
	Revision        string
	AppliedAt       time.Time
	RestartRequired bool
	LastError       string
}
