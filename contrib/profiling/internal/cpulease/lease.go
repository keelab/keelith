// Package cpulease coordinates process-wide CPU profiler ownership between
// Keelith profiling adapters.
package cpulease

import "sync"

var process sync.Mutex

// Lease represents exclusive ownership of Go's process-wide CPU profiler.
type Lease struct {
	once sync.Once
}

// TryAcquire acquires the process CPU profiler without waiting.
func TryAcquire() (*Lease, bool) {
	if !process.TryLock() {
		return nil, false
	}
	return &Lease{}, true
}

// Release returns process CPU profiler ownership. It is idempotent.
func (lease *Lease) Release() {
	if lease == nil {
		return
	}
	lease.once.Do(process.Unlock)
}
