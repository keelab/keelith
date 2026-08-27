// Package etcd adapts etcd v3 KV, Lease, and Watch to Keelith registry
// contracts.
package etcd

import (
	"context"
	"time"
)

// LeaseID identifies one backend lease without exposing clientv3 types.
type LeaseID int64

// Entry is one immutable key/value result from a linearizable prefix list.
type Entry struct {
	Key   string
	Value []byte
}

// EventType identifies a backend watch mutation.
type EventType uint8

const (
	// EventPut creates or replaces an entry.
	EventPut EventType = iota + 1
	// EventDelete removes an entry.
	EventDelete
)

// Event is one key mutation in an ordered watch batch.
type Event struct {
	Type  EventType
	Key   string
	Value []byte
}

// Batch contains all mutations delivered at one backend revision.
type Batch struct {
	Revision int64
	Events   []Event
	Err      error
}

// Backend isolates registry semantics from the official etcd SDK.
type Backend interface {
	Grant(context.Context, time.Duration) (LeaseID, error)
	KeepAlive(context.Context, LeaseID) (<-chan error, error)
	Put(context.Context, string, []byte, LeaseID) (int64, error)
	Delete(context.Context, string) (int64, error)
	Revoke(context.Context, LeaseID) error
	List(context.Context, string) ([]Entry, int64, error)
	Watch(context.Context, string, int64) <-chan Batch
	Close() error
}
