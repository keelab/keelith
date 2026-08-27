// Package etcdversioned provides immutable revision and activation semantics
// on top of etcd v3.
package etcdversioned

import "context"

// KV is one exact backend value and its etcd modification revision.
type KV struct {
	Key         string
	ModRevision int64
	Value       []byte
}

// Event is one active-pointer watch event or terminal backend failure.
type Event struct {
	ModRevision int64
	Value       []byte
	Deleted     bool
	Err         error
}

// Backend isolates Store semantics from the official client for deterministic
// testing. CommitActivation must atomically verify the target revision still
// exists, compare active, and create history.
type Backend interface {
	Get(context.Context, string) (KV, bool, error)
	Create(context.Context, string, []byte) (KV, bool, error)
	CommitActivation(
		context.Context,
		string,
		string,
		int64,
		[]byte,
		string,
		[]byte,
	) (int64, bool, error)
	List(context.Context, string, int) ([]KV, error)
	Watch(context.Context, string, int64) <-chan Event
	Close() error
}
