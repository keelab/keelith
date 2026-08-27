// Package etcd provides a revisioned etcd v3 configuration Source.
package etcd

import "context"

// Value is one exact-key linearizable read.
type Value struct {
	Revision int64
	Found    bool
	Content  []byte
}

// Update is one exact-key watch event or terminal backend error.
type Update struct {
	Revision int64
	Content  []byte
	Deleted  bool
	Err      error
}

// Backend isolates Source semantics from the official clientv3 package.
type Backend interface {
	Get(context.Context, string) (Value, error)
	Watch(context.Context, string, int64) <-chan Update
	Close() error
}
