package etcdversioned

import (
	"context"
	"errors"
	"sync"

	"github.com/keelab/keelith/config"
	core "github.com/keelab/keelith/config/versioned"
)

type sourceResult struct {
	snapshot config.Snapshot
	err      error
}

type sourceWatcher struct {
	source  *Source
	context context.Context
	cancel  context.CancelFunc
	updates <-chan Event

	modRevision int64
	generation  uint64
	results     chan sourceResult
	done        chan struct{}

	closeOnce sync.Once
	mu        sync.Mutex
	terminal  error
}

func (watcher *sourceWatcher) run() {
	defer close(watcher.done)
	defer close(watcher.results)
	defer watcher.source.removeWatcher(watcher)
	for {
		select {
		case <-watcher.context.Done():
			watcher.setTerminal(context.Cause(watcher.context))
			return
		case event, open := <-watcher.updates:
			if !open {
				if cause := context.Cause(watcher.context); cause != nil {
					watcher.setTerminal(cause)
				} else {
					watcher.setTerminal(ErrWatchClosed)
				}
				return
			}
			if event.Err != nil {
				watcher.source.recordError(event.Err)
				watcher.setTerminal(event.Err)
				return
			}
			if event.Deleted {
				watcher.modRevision = event.ModRevision
				watcher.source.recordError(core.ErrNoActive)
				continue
			}
			if event.ModRevision <= watcher.modRevision {
				continue
			}
			activation, err := decodeActivation(event.Value)
			if err != nil {
				watcher.modRevision = event.ModRevision
				watcher.source.recordError(err)
				continue
			}
			if activation.Generation <= watcher.generation {
				continue
			}
			snapshot, err := watcher.source.snapshot(watcher.context, activation)
			if err != nil {
				watcher.modRevision = event.ModRevision
				watcher.generation = activation.Generation
				watcher.source.recordError(err)
				continue
			}
			watcher.modRevision = event.ModRevision
			watcher.generation = activation.Generation
			watcher.source.accept(event.ModRevision, activation.Generation)
			select {
			case watcher.results <- sourceResult{snapshot: snapshot}:
			case <-watcher.context.Done():
				watcher.setTerminal(context.Cause(watcher.context))
				return
			}
		}
	}
}

func (watcher *sourceWatcher) Next(ctx context.Context) (config.Snapshot, error) {
	if watcher == nil || ctx == nil {
		return config.Snapshot{}, ErrInvalidOption
	}
	select {
	case <-ctx.Done():
		return config.Snapshot{}, context.Cause(ctx)
	case result, open := <-watcher.results:
		if open {
			return result.snapshot, result.err
		}
		return config.Snapshot{}, watcher.terminalError()
	}
}

func (watcher *sourceWatcher) Close() error {
	if watcher == nil {
		return nil
	}
	watcher.closeOnce.Do(watcher.cancel)
	<-watcher.done
	err := watcher.terminalError()
	if errors.Is(err, context.Canceled) || errors.Is(err, config.ErrWatcherClosed) {
		return nil
	}
	return err
}

func (watcher *sourceWatcher) setTerminal(err error) {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if err == nil {
		err = config.ErrWatcherClosed
	}
	watcher.terminal = err
}

func (watcher *sourceWatcher) terminalError() error {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if watcher.terminal == nil {
		return config.ErrWatcherClosed
	}
	return watcher.terminal
}

var _ config.Watcher = (*sourceWatcher)(nil)
