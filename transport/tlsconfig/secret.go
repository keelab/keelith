package tlsconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keelab/keelith/secret"
)

// Secret watcher lifecycle states.
const (
	secretReconnectInitial = 250 * time.Millisecond
	secretReconnectMaximum = 5 * time.Second
)

// SecretWatcherState is a bounded lifecycle state.
type SecretWatcherState string

const (
	// SecretWatcherNew is the state before watching starts.
	SecretWatcherNew SecretWatcherState = "new"
	// SecretWatcherRunning is the active watch state.
	SecretWatcherRunning SecretWatcherState = "running"
	// SecretWatcherStopped is the terminal stopped state.
	SecretWatcherStopped SecretWatcherState = "stopped"
	// SecretWatcherFailed is the state after a terminal watch failure.
	SecretWatcherFailed SecretWatcherState = "failed"
)

// SecretWatcherDescription is a material-free operational snapshot.
type SecretWatcherDescription struct {
	State      SecretWatcherState
	Running    bool
	LastFailed bool
	Reloads    uint64
	Reconnects uint64
	Failures   uint64
}

type secretMaterial struct {
	CertificatePEM string `json:"certificate_pem"`
	PrivateKeyPEM  string `json:"private_key_pem"`
	CAPEM          string `json:"ca_pem"`
}

// SecretManager resolves and watches complete secret replacements.
type SecretManager interface {
	Resolve(context.Context, secret.Reference) (secret.Value, error)
	Watch(context.Context, secret.Reference) (secret.Watcher, error)
}

// SecretWatcher atomically updates a Reloader from one secret JSON bundle.
//
// Keeping the certificate, private key, and CA in one secret prevents
// transient mismatched key-pair updates.
type SecretWatcher struct {
	manager   SecretManager
	reference secret.Reference
	reloader  *Reloader

	mu        sync.Mutex
	cancel    context.CancelFunc
	watcher   secret.Watcher
	done      chan struct{}
	lastError error
	version   string
	running   bool
	state     SecretWatcherState

	reloads    atomic.Uint64
	reconnects atomic.Uint64
	failures   atomic.Uint64
}

// NewSecretWatcher constructs an App lifecycle component.
func NewSecretWatcher(
	manager SecretManager,
	reference secret.Reference,
	reloader *Reloader,
) (*SecretWatcher, error) {
	if isNil(manager) || reloader == nil {
		return nil, fmt.Errorf(
			"%w: secret manager and reloader are required",
			ErrInvalidOption,
		)
	}
	if _, err := secret.NewReference(
		reference.Provider(),
		reference.Key(),
	); err != nil {
		return nil, err
	}
	return &SecretWatcher{
		manager:   manager,
		reference: reference,
		reloader:  reloader,
		done:      make(chan struct{}),
		state:     SecretWatcherNew,
	}, nil
}

// Start resolves initial material before opening the update watcher.
func (watcher *SecretWatcher) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	watcher.mu.Lock()
	if watcher.running || watcher.state == SecretWatcherStopped {
		watcher.mu.Unlock()
		return fmt.Errorf("%w: watcher already started", ErrInvalidOption)
	}
	watcher.running = true
	watcher.state = SecretWatcherRunning
	watcher.mu.Unlock()

	value, err := watcher.manager.Resolve(ctx, watcher.reference)
	if err != nil {
		watcher.failStart(err)
		return err
	}
	if err := watcher.apply(value); err != nil {
		watcher.failStart(err)
		return err
	}
	source, err := watcher.manager.Watch(ctx, watcher.reference)
	if err != nil {
		watcher.failStart(err)
		return err
	}
	watchContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	watcher.mu.Lock()
	watcher.cancel = cancel
	watcher.watcher = source
	watcher.mu.Unlock()
	go watcher.run(watchContext, source)
	return nil
}

// Shutdown stops watching. It is safe to call repeatedly.
func (watcher *SecretWatcher) Shutdown(ctx context.Context) error {
	if watcher == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	watcher.mu.Lock()
	cancel := watcher.cancel
	source := watcher.watcher
	done := watcher.done
	running := watcher.running
	watcher.mu.Unlock()
	if !running {
		return nil
	}
	if cancel != nil {
		cancel()
	}
	if source != nil {
		_ = source.Close()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// LastError returns the latest invalid update or watcher failure.
func (watcher *SecretWatcher) LastError() error {
	if watcher == nil {
		return fmt.Errorf("%w: watcher is nil", ErrInvalidOption)
	}
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	return watcher.lastError
}

// Description returns lifecycle and aggregate reload state without secret
// references, material, versions, paths, or error text.
func (watcher *SecretWatcher) Description() SecretWatcherDescription {
	if watcher == nil {
		return SecretWatcherDescription{State: SecretWatcherStopped}
	}
	watcher.mu.Lock()
	description := SecretWatcherDescription{
		State:      watcher.state,
		Running:    watcher.running,
		LastFailed: watcher.lastError != nil,
	}
	watcher.mu.Unlock()
	description.Reloads = watcher.reloads.Load()
	description.Reconnects = watcher.reconnects.Load()
	description.Failures = watcher.failures.Load()
	return description
}

func (watcher *SecretWatcher) run(
	ctx context.Context,
	source secret.Watcher,
) {
	defer func() {
		watcher.mu.Lock()
		watcher.running = false
		if watcher.state != SecretWatcherFailed {
			watcher.state = SecretWatcherStopped
		}
		watcher.mu.Unlock()
		close(watcher.done)
	}()
	for {
		value, err := source.Next(ctx)
		if err != nil {
			if context.Cause(ctx) != nil {
				return
			}
			watcher.record(err)
			_ = source.Close()
			source, err = watcher.reconnect(ctx)
			if err != nil {
				return
			}
			continue
		}
		if err := watcher.apply(value); err != nil {
			watcher.record(err)
			continue
		}
		watcher.record(nil)
	}
}

func (watcher *SecretWatcher) reconnect(
	ctx context.Context,
) (secret.Watcher, error) {
	delay := secretReconnectInitial
	for {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		value, err := watcher.manager.Resolve(ctx, watcher.reference)
		if err == nil {
			err = watcher.apply(value)
		}
		if err == nil {
			var source secret.Watcher
			source, err = watcher.manager.Watch(ctx, watcher.reference)
			if err == nil {
				watcher.mu.Lock()
				watcher.watcher = source
				watcher.mu.Unlock()
				watcher.reconnects.Add(1)
				watcher.record(nil)
				return source, nil
			}
		}
		watcher.record(err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, context.Cause(ctx)
		case <-timer.C:
		}
		delay *= 2
		if delay > secretReconnectMaximum {
			delay = secretReconnectMaximum
		}
	}
}

func (watcher *SecretWatcher) apply(value secret.Value) error {
	if value.Expired(time.Now()) {
		return fmt.Errorf("%w: secret value is expired", secret.ErrInvalidValue)
	}
	var encoded secretMaterial
	if err := json.Unmarshal(value.Bytes(), &encoded); err != nil {
		return fmt.Errorf("%w: secret JSON: %w", ErrInvalidMaterial, err)
	}
	if err := watcher.reloader.Update(Material{
		CertificatePEM: []byte(encoded.CertificatePEM),
		PrivateKeyPEM:  []byte(encoded.PrivateKeyPEM),
		CAPEM:          []byte(encoded.CAPEM),
	}); err != nil {
		return err
	}
	watcher.mu.Lock()
	changed := watcher.version != value.Version()
	watcher.version = value.Version()
	watcher.mu.Unlock()
	if changed {
		watcher.reloads.Add(1)
	}
	return nil
}

func (watcher *SecretWatcher) failStart(err error) {
	watcher.mu.Lock()
	watcher.running = false
	watcher.state = SecretWatcherFailed
	watcher.lastError = err
	watcher.failures.Add(1)
	watcher.mu.Unlock()
}

func (watcher *SecretWatcher) record(err error) {
	watcher.mu.Lock()
	watcher.lastError = err
	watcher.mu.Unlock()
	if err != nil {
		watcher.failures.Add(1)
	}
}
