package etcdversioned

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/keelab/keelith/config"
	core "github.com/keelab/keelith/config/versioned"
)

// SourceDescription is a bounded, content-free runtime snapshot.
type SourceDescription struct {
	Closed     bool
	Active     bool
	Generation uint64
	Watchers   int
	LastError  string
}

// Source watches only an active pointer and resolves immutable documents.
type Source struct {
	store     *Store
	ownsStore bool

	opMu sync.Mutex
	mu   sync.Mutex

	closed            bool
	latestModRevision int64
	generation        uint64
	watchers          map[*sourceWatcher]struct{}
	lastErr           error

	closeOnce sync.Once
	closeErr  error
}

// NewSource wraps a Store as a read-only config.Source.
func NewSource(store *Store, ownsStore bool) (*Source, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store is nil", ErrInvalidOption)
	}
	if err := store.require(context.Background()); err != nil {
		return nil, err
	}
	return &Source{
		store: store, ownsStore: ownsStore,
		watchers: make(map[*sourceWatcher]struct{}),
	}, nil
}

// Start verifies that an active immutable document is loadable.
func (source *Source) Start(ctx context.Context) error {
	_, err := source.Load(ctx)
	return err
}

// Load reads the active pointer and its immutable revision.
func (source *Source) Load(ctx context.Context) (config.Snapshot, error) {
	if err := source.require(ctx); err != nil {
		return config.Snapshot{}, err
	}
	activation, modRevision, err := source.store.active(ctx)
	if err != nil {
		source.recordError(err)
		return config.Snapshot{}, err
	}
	snapshot, err := source.snapshot(ctx, activation)
	if err != nil {
		source.recordError(err)
		return config.Snapshot{}, err
	}
	source.accept(modRevision, activation.Generation)
	return snapshot, nil
}

// Watch observes future active-pointer generations without replaying Load.
func (source *Source) Watch(ctx context.Context) (config.Watcher, error) {
	if err := source.require(ctx); err != nil {
		return nil, err
	}
	source.opMu.Lock()
	defer source.opMu.Unlock()
	activation, modRevision, err := source.store.active(ctx)
	if err != nil {
		source.recordError(err)
		return nil, err
	}
	if _, err := source.snapshot(ctx, activation); err != nil {
		source.recordError(err)
		return nil, err
	}
	watchContext, cancel := context.WithCancel(ctx)
	updates := source.store.backend.Watch(
		watchContext,
		source.store.activeKey(),
		modRevision+1,
	)
	if updates == nil {
		cancel()
		return nil, fmt.Errorf("%w: backend returned nil watch", ErrInvalidOption)
	}
	watcher := &sourceWatcher{
		source: source, context: watchContext, cancel: cancel,
		updates: updates, modRevision: modRevision,
		generation: activation.Generation,
		results:    make(chan sourceResult, 1), done: make(chan struct{}),
		terminal: config.ErrWatcherClosed,
	}
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		cancel()
		return nil, core.ErrClosed
	}
	source.watchers[watcher] = struct{}{}
	source.mu.Unlock()
	go watcher.run()
	return watcher, nil
}

// Shutdown closes logical watchers and an explicitly owned Store.
func (source *Source) Shutdown(ctx context.Context) error {
	if source == nil {
		return nil
	}
	if ctx == nil {
		return ErrInvalidOption
	}
	source.closeOnce.Do(func() {
		source.opMu.Lock()
		defer source.opMu.Unlock()
		source.mu.Lock()
		source.closed = true
		watchers := make([]*sourceWatcher, 0, len(source.watchers))
		for watcher := range source.watchers {
			watchers = append(watchers, watcher)
		}
		source.mu.Unlock()
		for _, watcher := range watchers {
			source.closeErr = errors.Join(source.closeErr, watcher.Close())
		}
		if cause := context.Cause(ctx); cause != nil {
			source.closeErr = errors.Join(source.closeErr, cause)
		}
		if source.ownsStore {
			source.closeErr = errors.Join(source.closeErr, source.store.Close())
		}
	})
	return source.closeErr
}

// Describe returns bounded Source diagnostics.
func (source *Source) Describe() SourceDescription {
	if source == nil {
		return SourceDescription{Closed: true, LastError: "closed"}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return SourceDescription{
		Closed: source.closed, Active: source.generation > 0,
		Generation: source.generation, Watchers: len(source.watchers),
		LastError: errorClass(source.lastErr),
	}
}

func (source *Source) snapshot(
	ctx context.Context,
	activation core.Activation,
) (config.Snapshot, error) {
	metadata, content, err := source.store.Revision(ctx, activation.Revision)
	if err != nil {
		return config.Snapshot{}, err
	}
	values, err := decodeDocument(content, metadata.Format, source.store.maxBytes)
	if err != nil {
		return config.Snapshot{}, err
	}
	return config.NewSnapshot(
		fmt.Sprintf("etcd-versioned:%d:%s", activation.Generation, activation.Revision),
		values,
	)
}

func (source *Source) require(ctx context.Context) error {
	if source == nil || ctx == nil {
		return ErrInvalidOption
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return core.ErrClosed
	}
	return nil
}

func (source *Source) accept(modRevision int64, generation uint64) bool {
	source.mu.Lock()
	defer source.mu.Unlock()
	if modRevision <= source.latestModRevision || generation <= source.generation {
		return false
	}
	source.latestModRevision = modRevision
	source.generation = generation
	source.lastErr = nil
	return true
}

func (source *Source) recordError(err error) {
	source.mu.Lock()
	source.lastErr = err
	source.mu.Unlock()
}

func (source *Source) removeWatcher(watcher *sourceWatcher) {
	source.mu.Lock()
	delete(source.watchers, watcher)
	source.mu.Unlock()
}

func errorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, core.ErrNoActive):
		return "no-active"
	case errors.Is(err, core.ErrTampered):
		return "integrity"
	case errors.Is(err, core.ErrClosed):
		return "closed"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "context"
	default:
		return "backend"
	}
}

var _ config.Source = (*Source)(nil)
