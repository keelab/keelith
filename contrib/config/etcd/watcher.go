package etcd

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/keelab/keelith/config"
)

type watcher struct {
	source   *Source
	context  context.Context
	cancel   context.CancelFunc
	updates  <-chan Update
	revision int64
	results  chan result
	done     chan struct{}

	mu       sync.Mutex
	terminal error
	once     sync.Once
}

func (watcher *watcher) Next(ctx context.Context) (config.Snapshot, error) {
	if ctx == nil {
		return config.Snapshot{}, fmt.Errorf(
			"%w: context is nil",
			ErrInvalidOption,
		)
	}
	select {
	case <-watcher.done:
		return config.Snapshot{}, watcher.terminalError()
	default:
	}
	for {
		select {
		case current := <-watcher.results:
			if watcher.source.staleRevision(current.revision) {
				continue
			}
			return current.snapshot.Clone(), nil
		case <-watcher.done:
			return config.Snapshot{}, watcher.terminalError()
		case <-ctx.Done():
			return config.Snapshot{}, context.Cause(ctx)
		}
	}
}

func (watcher *watcher) Close() error {
	watcher.finish(config.ErrWatcherClosed)
	return nil
}

func (watcher *watcher) run() {
	for {
		select {
		case update, ok := <-watcher.updates:
			if !ok {
				if cause := context.Cause(watcher.context); cause != nil {
					watcher.finish(cause)
				} else {
					watcher.finish(ErrWatchClosed)
				}
				return
			}
			if update.Err != nil {
				watcher.finish(fmt.Errorf(
					"etcd config: watch revision %d: %w",
					update.Revision,
					update.Err,
				))
				return
			}
			if update.Revision <= watcher.revision {
				watcher.finish(fmt.Errorf(
					"%w: non-increasing revision %d after %d",
					ErrInvalidDocument,
					update.Revision,
					watcher.revision,
				))
				return
			}
			watcher.revision = update.Revision
			value := Value{
				Revision: update.Revision,
				Found:    !update.Deleted,
				Content:  update.Content,
			}
			snapshot, err := watcher.source.snapshot(value)
			if err != nil {
				watcher.source.recordError(err)
				continue
			}
			if watcher.source.acceptRevision(update.Revision) {
				watcher.publish(result{
					revision: update.Revision,
					snapshot: snapshot,
				})
			}
		case <-watcher.context.Done():
			watcher.finish(context.Cause(watcher.context))
			return
		}
	}
}

type result struct {
	revision int64
	snapshot config.Snapshot
}

func (watcher *watcher) publish(current result) {
	select {
	case <-watcher.done:
		return
	default:
	}
	select {
	case <-watcher.results:
	default:
	}
	select {
	case watcher.results <- result{
		revision: current.revision,
		snapshot: current.snapshot.Clone(),
	}:
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
		if !errors.Is(err, config.ErrWatcherClosed) &&
			!errors.Is(err, context.Canceled) {
			watcher.source.recordError(err)
		}
		watcher.source.removeWatcher(watcher)
		close(watcher.done)
		watcher.cancel()
	})
}

func (watcher *watcher) terminalError() error {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if watcher.terminal == nil {
		return config.ErrWatcherClosed
	}
	return watcher.terminal
}

var _ config.Watcher = (*watcher)(nil)
