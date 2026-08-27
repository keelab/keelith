package etcd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/keelab/keelith/registry"
)

type watcher struct {
	client   *Client
	service  string
	prefix   string
	context  context.Context
	cancel   context.CancelFunc
	batches  <-chan Batch
	state    map[string]registry.Instance
	revision int64
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
		select {
		case batch, ok := <-watcher.batches:
			if !ok {
				if cause := context.Cause(watcher.context); cause != nil {
					watcher.finish(cause)
				} else {
					watcher.finish(ErrWatchClosed)
				}
				return
			}
			if batch.Err != nil {
				watcher.finish(fmt.Errorf(
					"etcd registry: watch revision %d: %w",
					batch.Revision,
					batch.Err,
				))
				return
			}
			if len(batch.Events) == 0 {
				continue
			}
			snapshot, err := watcher.apply(batch)
			if err != nil {
				watcher.finish(err)
				return
			}
			watcher.publish(snapshot)
		case <-watcher.context.Done():
			watcher.finish(context.Cause(watcher.context))
			return
		}
	}
}

func (watcher *watcher) apply(batch Batch) (registry.Snapshot, error) {
	if batch.Revision <= watcher.revision {
		return registry.Snapshot{}, fmt.Errorf(
			"%w: non-increasing revision %d after %d",
			ErrInvalidRecord,
			batch.Revision,
			watcher.revision,
		)
	}
	next := make(map[string]registry.Instance, len(watcher.state))
	for key, instance := range watcher.state {
		next[key] = instance
	}
	for _, event := range batch.Events {
		if !strings.HasPrefix(event.Key, watcher.prefix) {
			return registry.Snapshot{}, fmt.Errorf(
				"%w: event key %q is outside service prefix",
				ErrInvalidRecord,
				event.Key,
			)
		}
		switch event.Type {
		case EventPut:
			instance, err := watcher.client.decode(
				event.Key,
				event.Value,
				watcher.service,
			)
			if err != nil {
				return registry.Snapshot{}, err
			}
			next[event.Key] = instance
		case EventDelete:
			delete(next, event.Key)
		default:
			return registry.Snapshot{}, fmt.Errorf(
				"%w: event type %d",
				ErrInvalidRecord,
				event.Type,
			)
		}
	}
	snapshot, err := snapshotFromState(watcher.service, batch.Revision, next)
	if err != nil {
		return registry.Snapshot{}, err
	}
	watcher.state = next
	watcher.revision = batch.Revision
	return snapshot, nil
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
		watcher.client.recordErrorUnlessClosed(err)
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

func snapshotFromState(
	service string,
	revision int64,
	state map[string]registry.Instance,
) (registry.Snapshot, error) {
	if revision < 0 {
		return registry.Snapshot{}, fmt.Errorf(
			"%w: negative revision %d",
			ErrInvalidRecord,
			revision,
		)
	}
	instances := make([]registry.Instance, 0, len(state))
	for _, instance := range state {
		instances = append(instances, instance)
	}
	return registry.NewSnapshot(
		service,
		fmt.Sprintf("etcd:%d", revision),
		instances,
	)
}

func (client *Client) recordErrorUnlessClosed(err error) {
	if err == nil ||
		errors.Is(err, registry.ErrWatcherClosed) ||
		errors.Is(err, context.Canceled) {
		return
	}
	client.recordError(err)
}
