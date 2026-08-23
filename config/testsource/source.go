// Package testsource provides an in-memory Config Source for tests and local
// composition.
package testsource

import (
	"context"
	"sync"

	"github.com/keelab/keelith/config"
)

// Source is an updateable in-memory config.Source.
type Source struct {
	mu       sync.Mutex
	current  config.Snapshot
	watchers map[*watcher]struct{}
}

// New creates a Source with initial.
func New(initial config.Snapshot) *Source {
	return &Source{
		current:  initial.Clone(),
		watchers: make(map[*watcher]struct{}),
	}
}

// Load returns the current complete Snapshot.
func (s *Source) Load(ctx context.Context) (config.Snapshot, error) {
	if cause := context.Cause(ctx); cause != nil {
		return config.Snapshot{}, cause
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current.Clone(), nil
}

// Watch creates a watcher for future complete Snapshots.
func (s *Source) Watch(ctx context.Context) (config.Watcher, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	watcher := &watcher{
		source:  s,
		updates: make(chan config.Snapshot, 1),
		done:    make(chan struct{}),
	}
	s.mu.Lock()
	s.watchers[watcher] = struct{}{}
	s.mu.Unlock()
	return watcher, nil
}

// Update atomically replaces the current Snapshot and notifies watchers.
func (s *Source) Update(snapshot config.Snapshot) {
	update := snapshot.Clone()
	s.mu.Lock()
	s.current = update
	for watcher := range s.watchers {
		select {
		case <-watcher.updates:
		default:
		}
		select {
		case watcher.updates <- update.Clone():
		default:
		}
	}
	s.mu.Unlock()
}

// WatcherCount returns the number of currently open watchers.
func (s *Source) WatcherCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.watchers)
}

type watcher struct {
	source  *Source
	updates chan config.Snapshot
	done    chan struct{}
	once    sync.Once
}

func (w *watcher) Next(ctx context.Context) (config.Snapshot, error) {
	select {
	case snapshot := <-w.updates:
		return snapshot.Clone(), nil
	case <-w.done:
		return config.Snapshot{}, config.ErrWatcherClosed
	case <-ctx.Done():
		return config.Snapshot{}, context.Cause(ctx)
	}
}

func (w *watcher) Close() error {
	w.once.Do(func() {
		w.source.mu.Lock()
		delete(w.source.watchers, w)
		close(w.done)
		w.source.mu.Unlock()
	})
	return nil
}
