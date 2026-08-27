package consul

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/keelab/keelith/registry"
)

type watcher struct {
	client   *Client
	service  string
	context  context.Context
	cancel   context.CancelFunc
	revision string
	updates  chan registry.Snapshot
	done     chan struct{}

	mu       sync.Mutex
	terminal error
	once     sync.Once
}

func (watcher *watcher) Next(ctx context.Context) (registry.Snapshot, error) {
	if ctx == nil {
		return registry.Snapshot{}, fmt.Errorf(
			"%w: context is nil",
			ErrInvalidOption,
		)
	}
	select {
	case <-watcher.done:
		return registry.Snapshot{}, watcher.terminalError()
	default:
	}
	select {
	case snapshot := <-watcher.updates:
		return snapshot.Clone(), nil
	case <-watcher.done:
		return registry.Snapshot{}, watcher.terminalError()
	case <-ctx.Done():
		return registry.Snapshot{}, context.Cause(ctx)
	}
}

func (watcher *watcher) Close() error {
	watcher.finish(registry.ErrWatcherClosed)
	return nil
}

func (watcher *watcher) run() {
	for {
		records, revision, err := watcher.client.backend.List(
			watcher.context,
			watcher.service,
			watcher.client.datacenter,
			watcher.revision,
			watcher.client.blockingWait,
		)
		if err != nil {
			if cause := context.Cause(watcher.context); cause != nil {
				watcher.finish(cause)
			} else {
				watcher.finish(fmt.Errorf(
					"consul registry: blocking health query: %w",
					err,
				))
			}
			return
		}
		if revision == watcher.revision {
			continue
		}
		snapshot, err := watcher.client.snapshot(
			watcher.service,
			revision,
			records,
		)
		if err != nil {
			watcher.finish(err)
			return
		}
		watcher.revision = revision
		watcher.publish(snapshot)
	}
}

func (watcher *watcher) publish(snapshot registry.Snapshot) {
	select {
	case <-watcher.done:
		return
	default:
	}
	select {
	case <-watcher.updates:
	default:
	}
	select {
	case watcher.updates <- snapshot.Clone():
	case <-watcher.done:
	default:
	}
}

func (watcher *watcher) finish(err error) {
	if err == nil {
		err = ErrWatchClosed
	}
	watcher.once.Do(func() {
		watcher.mu.Lock()
		watcher.terminal = err
		watcher.mu.Unlock()
		if !errors.Is(err, registry.ErrWatcherClosed) &&
			!errors.Is(err, context.Canceled) {
			watcher.client.recordError(err)
		}
		watcher.client.removeWatcher(watcher)
		close(watcher.done)
		watcher.cancel()
	})
}

func (watcher *watcher) terminalError() error {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if watcher.terminal == nil {
		return registry.ErrWatcherClosed
	}
	return watcher.terminal
}
