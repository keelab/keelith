package pyroscope

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"sync"
	"time"

	grafanapyroscope "github.com/grafana/pyroscope-go"
	"github.com/keelab/contrib/profiling/internal/cpulease"
	"github.com/keelab/keelith/secret"
)

const maximumResponseBytes = 1024 * 1024

const (
	credentialReconnectInitial = 250 * time.Millisecond
	credentialReconnectMaximum = 5 * time.Second
)

// State is the bounded lifecycle state exposed through Description.
type State string

const (
	// StateNew means the runtime has not started.
	StateNew State = "new"
	// StateStarting means the runtime is opening profiling resources.
	StateStarting State = "starting"
	// StateRunning means profiling is active.
	StateRunning State = "running"
	// StateStopping means shutdown has started.
	StateStopping State = "stopping"
	// StateStopped means shutdown completed normally.
	StateStopped State = "stopped"
	// StateFailed means startup or shutdown failed.
	StateFailed State = "failed"
)

// SecretResolver resolves configured Basic Auth password references.
type SecretResolver interface {
	Resolve(context.Context, secret.Reference) (secret.Value, error)
}

// SecretWatchResolver resolves credentials and watches complete replacements.
type SecretWatchResolver interface {
	SecretResolver
	Watch(context.Context, secret.Reference) (secret.Watcher, error)
}

type profiler interface {
	Stop() error
}

type starter func(grafanapyroscope.Config) (profiler, error)

// Description is a value-free, low-cardinality runtime snapshot.
type Description struct {
	State                State
	Ready                bool
	Degraded             bool
	UsesCPU              bool
	TLS                  bool
	WatchesCredential    bool
	ProfileTypeCount     int
	Starts               uint64
	Stops                uint64
	Failures             uint64
	ExportFailures       uint64
	CredentialRotations  uint64
	CredentialReconnects uint64
	CredentialFailures   uint64
}

// Runtime owns one Pyroscope SDK session.
type Runtime struct {
	config        Config
	resolver      SecretResolver
	watchResolver SecretWatchResolver
	logger        *slog.Logger
	startSDK      starter

	mu                   sync.Mutex
	state                State
	profiler             profiler
	cpuLease             *cpulease.Lease
	authClient           *authHTTPClient
	credentialCancel     context.CancelFunc
	credentialWatcher    secret.Watcher
	credentialDone       chan struct{}
	credentialVersion    string
	stopDone             chan struct{}
	stopErr              error
	starts               uint64
	stops                uint64
	failures             uint64
	exportFailures       uint64
	credentialRotations  uint64
	credentialReconnects uint64
	credentialFailures   uint64
}

type preparedCredentialWatch struct {
	ctx               context.Context
	cancel            context.CancelFunc
	stopStartupCancel func() bool
	source            secret.Watcher
	done              chan struct{}
	reference         secret.Reference
	version           string
}

// New validates and constructs a dormant continuous profiling Runtime.
func New(
	config Config,
	resolver SecretResolver,
	logger *slog.Logger,
) (*Runtime, error) {
	normalized, err := NormalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if normalized.PasswordReference != "" && isNilResolver(resolver) {
		return nil, fmt.Errorf("%w: secret resolver is nil", ErrInvalidConfig)
	}
	var watchResolver SecretWatchResolver
	if normalized.WatchPassword {
		var ok bool
		watchResolver, ok = resolver.(SecretWatchResolver)
		if !ok || isNilResolver(watchResolver) {
			return nil, fmt.Errorf(
				"%w: password watch requires a secret watch resolver",
				ErrInvalidConfig,
			)
		}
	}
	if logger == nil {
		logger = slog.Default()
	}
	runtime := &Runtime{
		config:        normalized,
		resolver:      resolver,
		watchResolver: watchResolver,
		logger:        logger,
		state:         StateNew,
	}
	runtime.startSDK = func(config grafanapyroscope.Config) (profiler, error) {
		return grafanapyroscope.Start(config)
	}
	return runtime, nil
}

// Start resolves credentials, acquires CPU ownership when needed, and starts
// the SDK inside the App startup rollback boundary.
func (runtime *Runtime) Start(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return fmt.Errorf("%w: runtime or context is nil", ErrInvalidConfig)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	runtime.mu.Lock()
	switch runtime.state {
	case StateRunning, StateStarting, StateStopping:
		runtime.mu.Unlock()
		return ErrAlreadyStarted
	case StateStopped, StateFailed:
		runtime.mu.Unlock()
		return ErrStopped
	case StateNew:
		runtime.state = StateStarting
	}
	runtime.mu.Unlock()

	lease, authClient, credentialWatch, startErr := runtime.prepare(ctx)
	if startErr != nil {
		runtime.failStart(startErr)
		return startErr
	}
	profileTypes := make([]grafanapyroscope.ProfileType, 0, len(runtime.config.ProfileTypes))
	for _, profileType := range runtime.config.ProfileTypes {
		profileTypes = append(profileTypes, grafanapyroscope.ProfileType(profileType))
	}
	started, err := runtime.startSDK(grafanapyroscope.Config{
		ApplicationName: runtime.config.ApplicationName,
		ServerAddress:   runtime.config.ServerAddress,
		TenantID:        runtime.config.TenantID,
		UploadRate:      runtime.config.UploadRate,
		Logger:          sdkLogger{runtime: runtime},
		ProfileTypes:    profileTypes,
		DisableGCRuns:   runtime.config.DisableGCRuns,
		Tags:            cloneTags(runtime.config.Tags),
		HTTPClient:      authClient,
	})
	if err != nil {
		cleanupPrepared(lease, authClient, credentialWatch)
		startErr = fmt.Errorf("pyroscope profiling: start SDK: %w", err)
		runtime.failStart(startErr)
		return startErr
	}
	if credentialWatch != nil {
		credentialWatch.stopStartupCancel()
	}
	if cause := context.Cause(ctx); cause != nil {
		_ = started.Stop()
		cleanupPrepared(lease, authClient, credentialWatch)
		runtime.failStart(cause)
		return cause
	}
	runtime.mu.Lock()
	runtime.profiler = started
	runtime.cpuLease = lease
	runtime.authClient = authClient
	if credentialWatch != nil {
		runtime.credentialCancel = credentialWatch.cancel
		runtime.credentialWatcher = credentialWatch.source
		runtime.credentialDone = credentialWatch.done
		runtime.credentialVersion = credentialWatch.version
	}
	runtime.state = StateRunning
	runtime.starts++
	runtime.mu.Unlock()
	if credentialWatch != nil {
		go runtime.runCredentialWatch(credentialWatch)
	}
	return nil
}

// Shutdown starts a one-shot stop and waits until it finishes or ctx expires.
// Cleanup continues in the background after caller timeout.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return fmt.Errorf("%w: runtime or context is nil", ErrInvalidConfig)
	}
	done, err := runtime.beginStop()
	if err != nil {
		return err
	}
	select {
	case <-done:
		runtime.mu.Lock()
		stopErr := runtime.stopErr
		runtime.mu.Unlock()
		return stopErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Describe returns a value-free runtime snapshot.
func (runtime *Runtime) Describe() Description {
	if runtime == nil {
		return Description{State: StateFailed, Degraded: true}
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return Description{
		State: runtime.state,
		Ready: runtime.state == StateRunning,
		Degraded: runtime.state == StateFailed || runtime.failures > 0 ||
			runtime.exportFailures > 0 || runtime.credentialFailures > 0,
		UsesCPU:              UsesCPU(runtime.config),
		TLS:                  len(runtime.config.ServerAddress) >= len("https://") && runtime.config.ServerAddress[:len("https://")] == "https://",
		WatchesCredential:    runtime.config.WatchPassword,
		ProfileTypeCount:     len(runtime.config.ProfileTypes),
		Starts:               runtime.starts,
		Stops:                runtime.stops,
		Failures:             runtime.failures,
		ExportFailures:       runtime.exportFailures,
		CredentialRotations:  runtime.credentialRotations,
		CredentialReconnects: runtime.credentialReconnects,
		CredentialFailures:   runtime.credentialFailures,
	}
}

func (runtime *Runtime) prepare(
	ctx context.Context,
) (*cpulease.Lease, *authHTTPClient, *preparedCredentialWatch, error) {
	credentialWatch, value, err := runtime.prepareCredential(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	password := value.Bytes()
	trimmedPassword := secret.TrimLineBreaks(password)
	authClient := newAuthHTTPClient(
		runtime.config.RequestTimeout,
		runtime.config.BasicAuthUser,
		trimmedPassword,
	)
	clear(password)
	var lease *cpulease.Lease
	if UsesCPU(runtime.config) {
		var acquired bool
		lease, acquired = cpulease.TryAcquire()
		if !acquired {
			cleanupPrepared(nil, authClient, credentialWatch)
			return nil, nil, nil, ErrCPUConflict
		}
	}
	return lease, authClient, credentialWatch, nil
}

func (runtime *Runtime) prepareCredential(
	ctx context.Context,
) (*preparedCredentialWatch, secret.Value, error) {
	if runtime.config.PasswordReference == "" {
		return nil, secret.Value{}, nil
	}
	reference, _ := secret.Parse(runtime.config.PasswordReference)
	var prepared *preparedCredentialWatch
	if runtime.config.WatchPassword {
		watchContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
		stopStartupCancel := context.AfterFunc(ctx, cancel)
		source, err := runtime.watchResolver.Watch(watchContext, reference)
		if err != nil {
			stopStartupCancel()
			cancel()
			return nil, secret.Value{}, fmt.Errorf(
				"pyroscope profiling: establish password watch: %w",
				err,
			)
		}
		prepared = &preparedCredentialWatch{
			ctx:               watchContext,
			cancel:            cancel,
			stopStartupCancel: stopStartupCancel,
			source:            source,
			done:              make(chan struct{}),
			reference:         reference,
		}
	}
	value, err := runtime.resolver.Resolve(ctx, reference)
	if err != nil {
		cleanupPrepared(nil, nil, prepared)
		return nil, secret.Value{}, fmt.Errorf(
			"pyroscope profiling: resolve password: %w",
			err,
		)
	}
	if err := validateCredential(value); err != nil {
		cleanupPrepared(nil, nil, prepared)
		return nil, secret.Value{}, err
	}
	if prepared != nil {
		prepared.version = value.Version()
	}
	return prepared, value, nil
}

func validateCredential(value secret.Value) error {
	if err := value.Validate(); err != nil || value.Expired(time.Now()) {
		return fmt.Errorf("%w: resolved password", ErrInvalidConfig)
	}
	return nil
}

func (runtime *Runtime) failStart(err error) {
	runtime.mu.Lock()
	runtime.state = StateFailed
	runtime.failures++
	runtime.stopErr = err
	runtime.mu.Unlock()
}

func (runtime *Runtime) beginStop() (<-chan struct{}, error) {
	runtime.mu.Lock()
	switch runtime.state {
	case StateNew:
		runtime.state = StateStopped
		runtime.stopDone = make(chan struct{})
		close(runtime.stopDone)
		done := runtime.stopDone
		runtime.mu.Unlock()
		return done, nil
	case StateStarting:
		runtime.mu.Unlock()
		return nil, ErrAlreadyStarted
	case StateFailed:
		if runtime.stopDone == nil {
			runtime.stopDone = make(chan struct{})
			close(runtime.stopDone)
		}
		done := runtime.stopDone
		runtime.mu.Unlock()
		return done, nil
	case StateStopped, StateStopping:
		done := runtime.stopDone
		runtime.mu.Unlock()
		return done, nil
	case StateRunning:
		runtime.state = StateStopping
		runtime.stopDone = make(chan struct{})
		done := runtime.stopDone
		started := runtime.profiler
		lease := runtime.cpuLease
		authClient := runtime.authClient
		credentialCancel := runtime.credentialCancel
		credentialWatcher := runtime.credentialWatcher
		credentialDone := runtime.credentialDone
		runtime.mu.Unlock()
		go runtime.stop(
			started,
			lease,
			authClient,
			credentialCancel,
			credentialWatcher,
			credentialDone,
			done,
		)
		return done, nil
	default:
		runtime.mu.Unlock()
		return nil, ErrStopped
	}
}

func (runtime *Runtime) stop(
	started profiler,
	lease *cpulease.Lease,
	authClient *authHTTPClient,
	credentialCancel context.CancelFunc,
	credentialWatcher secret.Watcher,
	credentialDone <-chan struct{},
	done chan struct{},
) {
	if credentialCancel != nil {
		credentialCancel()
	}
	if credentialWatcher != nil {
		_ = credentialWatcher.Close()
	}
	if credentialDone != nil {
		<-credentialDone
	}
	err := started.Stop()
	lease.Release()
	authClient.clear()
	runtime.mu.Lock()
	runtime.stopErr = err
	runtime.stops++
	if err != nil {
		runtime.state = StateFailed
		runtime.failures++
	} else {
		runtime.state = StateStopped
	}
	runtime.profiler = nil
	runtime.cpuLease = nil
	runtime.authClient = nil
	runtime.credentialCancel = nil
	runtime.credentialWatcher = nil
	runtime.credentialDone = nil
	runtime.credentialVersion = ""
	close(done)
	runtime.mu.Unlock()
}

func (runtime *Runtime) runCredentialWatch(
	prepared *preparedCredentialWatch,
) {
	source := prepared.source
	defer close(prepared.done)
	for {
		value, err := source.Next(prepared.ctx)
		if err != nil {
			if context.Cause(prepared.ctx) != nil {
				return
			}
			runtime.recordCredentialFailure()
			_ = source.Close()
			source, err = runtime.reconnectCredential(
				prepared.ctx,
				prepared.reference,
			)
			if err != nil {
				return
			}
			continue
		}
		if err := runtime.applyCredential(value); err != nil {
			runtime.recordCredentialFailure()
		}
	}
}

func (runtime *Runtime) reconnectCredential(
	ctx context.Context,
	reference secret.Reference,
) (secret.Watcher, error) {
	delay := credentialReconnectInitial
	for {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		source, watchErr := runtime.watchResolver.Watch(ctx, reference)
		if watchErr == nil {
			value, resolveErr := runtime.resolver.Resolve(ctx, reference)
			if resolveErr == nil {
				resolveErr = runtime.applyCredential(value)
			}
			if resolveErr == nil {
				runtime.mu.Lock()
				runtime.credentialWatcher = source
				runtime.credentialReconnects++
				runtime.mu.Unlock()
				return source, nil
			}
			_ = source.Close()
		}
		runtime.recordCredentialFailure()
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
		if delay > credentialReconnectMaximum {
			delay = credentialReconnectMaximum
		}
	}
}

func (runtime *Runtime) applyCredential(value secret.Value) error {
	if err := validateCredential(value); err != nil {
		return err
	}
	password := value.Bytes()
	defer clear(password)
	trimmedPassword := secret.TrimLineBreaks(password)
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if value.Version() == runtime.credentialVersion {
		return nil
	}
	if runtime.authClient == nil {
		return ErrStopped
	}
	runtime.authClient.update(trimmedPassword)
	runtime.credentialVersion = value.Version()
	runtime.credentialRotations++
	return nil
}

func (runtime *Runtime) recordCredentialFailure() uint64 {
	runtime.mu.Lock()
	runtime.credentialFailures++
	count := runtime.credentialFailures
	runtime.mu.Unlock()
	if count&(count-1) == 0 {
		runtime.logger.Warn(
			"continuous profiler credential watch failure",
			"failures",
			count,
		)
	}
	return count
}

func cleanupPrepared(
	lease *cpulease.Lease,
	authClient *authHTTPClient,
	credentialWatch *preparedCredentialWatch,
) {
	if credentialWatch != nil {
		credentialWatch.stopStartupCancel()
		credentialWatch.cancel()
		_ = credentialWatch.source.Close()
	}
	lease.Release()
	authClient.clear()
}

func (runtime *Runtime) recordExportFailure() uint64 {
	runtime.mu.Lock()
	runtime.exportFailures++
	count := runtime.exportFailures
	runtime.mu.Unlock()
	return count
}

type sdkLogger struct {
	runtime *Runtime
}

func (logger sdkLogger) Infof(string, ...interface{})  {}
func (logger sdkLogger) Debugf(string, ...interface{}) {}

func (logger sdkLogger) Errorf(string, ...interface{}) {
	if logger.runtime == nil {
		return
	}
	count := logger.runtime.recordExportFailure()
	if count&(count-1) == 0 {
		logger.runtime.logger.Warn(
			"continuous profiler reported an exporter failure",
			"failures",
			count,
		)
	}
}

type authHTTPClient struct {
	client   *http.Client
	username string
	mu       sync.RWMutex
	password []byte
}

func newAuthHTTPClient(
	timeout time.Duration,
	username string,
	password []byte,
) *authHTTPClient {
	transport := http.DefaultTransport
	if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = defaultTransport.Clone()
	}
	return &authHTTPClient{
		client: &http.Client{
			Transport: transport,
			Timeout:   timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		username: username,
		password: append([]byte(nil), password...),
	}
}

func (client *authHTTPClient) Do(request *http.Request) (*http.Response, error) {
	if client == nil || request == nil {
		return nil, fmt.Errorf("%w: http request", ErrInvalidConfig)
	}
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	client.mu.RLock()
	if client.username != "" && len(client.password) != 0 {
		clone.SetBasicAuth(client.username, string(client.password))
	}
	client.mu.RUnlock()
	response, err := client.client.Do(clone)
	if err != nil {
		return nil, err
	}
	response.Body = &limitedReadCloser{
		Reader: io.LimitReader(response.Body, maximumResponseBytes+1),
		Closer: response.Body,
	}
	return response, nil
}

type limitedReadCloser struct {
	io.Reader
	io.Closer
}

func (client *authHTTPClient) clear() {
	if client == nil {
		return
	}
	client.mu.Lock()
	clear(client.password)
	client.password = nil
	client.mu.Unlock()
}

func (client *authHTTPClient) update(password []byte) {
	if client == nil {
		return
	}
	next := append([]byte(nil), password...)
	client.mu.Lock()
	clear(client.password)
	client.password = next
	client.mu.Unlock()
}

func isNilResolver(resolver SecretResolver) bool {
	if resolver == nil {
		return true
	}
	value := reflect.ValueOf(resolver)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
