package jwt

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	defaultRefreshInterval    = 5 * time.Minute
	defaultMinimumRefreshWait = 5 * time.Second
	defaultRequestTimeout     = 10 * time.Second
	defaultMaxResponseBytes   = 1 * 1024 * 1024
	maxJWKSKeys               = 128
)

// State is the bounded RemoteKeySet lifecycle state.
type State string

const (
	// StateCreated means no remote fetch has started.
	StateCreated State = "created"
	// StateStarting means the first fetch is in progress.
	StateStarting State = "starting"
	// StateRunning means remote key refresh is active.
	StateRunning State = "running"
	// StateStopping means shutdown has started.
	StateStopping State = "stopping"
	// StateStopped means shutdown completed normally.
	StateStopped State = "stopped"
	// StateFailed means startup or refresh failed.
	StateFailed State = "failed"
)

// RemoteConfig configures one lifecycle-owned remote JWKS provider.
type RemoteConfig struct {
	// ComponentName is the unique App dependency-graph identity. Empty selects
	// "security.jwt.jwks"; set it when one App uses multiple issuers.
	ComponentName string
	// url is an https JWKS endpoint. Plain http is rejected unless
	// AllowInsecureHTTP is explicitly enabled for a loopback endpoint.
	URL string
	// Algorithms restricts accepted JWK algorithms and defaults to RS256,
	// ES256, and EdDSA.
	Algorithms []string
	// HTTPClient is copied before use. Nil selects an isolated default client.
	HTTPClient *http.Client
	// RefreshInterval controls periodic last-good refresh.
	RefreshInterval time.Duration
	// MinimumRefreshInterval limits unknown-kid refresh amplification.
	MinimumRefreshInterval time.Duration
	// RequestTimeout bounds each http operation.
	RequestTimeout time.Duration
	// MaxResponseBytes bounds the decoded JWKS document.
	MaxResponseBytes int64
	// AllowInsecureHTTP permits only explicit loopback http endpoints.
	AllowInsecureHTTP bool
}

type normalizedRemoteConfig struct {
	componentName          string
	endpoint               *url.URL
	algorithmSet           map[string]struct{}
	client                 *http.Client
	refreshInterval        time.Duration
	minimumRefreshInterval time.Duration
	requestTimeout         time.Duration
	maxResponseBytes       int64
}

type keySnapshot struct {
	keys map[keyReference]any
}

// RemoteDescription contains only lifecycle and aggregate state. It never
// exposes endpoint, key ids, key material, response content, or error text.
type RemoteDescription struct {
	State      State
	Ready      bool
	LastFailed bool
	KeyCount   int
	Refreshes  uint64
	Failures   uint64
	KeyMisses  uint64
}

// RemoteKeySet fetches and rotates a bounded asymmetric JWKS snapshot.
type RemoteKeySet struct {
	config normalizedRemoteConfig

	mu          sync.Mutex
	state       State
	cancel      context.CancelFunc
	done        chan struct{}
	doneOnce    sync.Once
	startErr    error
	shutdownErr error

	snapshot    atomic.Pointer[keySnapshot]
	lastAttempt atomic.Int64
	refreshes   atomic.Uint64
	failures    atomic.Uint64
	keyMisses   atomic.Uint64
	lastFailed  atomic.Bool
	refresh     singleflight.Group
}

// NewRemoteKeySet validates configuration and creates a disconnected provider.
func NewRemoteKeySet(config RemoteConfig) (*RemoteKeySet, error) {
	normalized, err := normalizeRemoteConfig(config)
	if err != nil {
		return nil, err
	}
	return &RemoteKeySet{
		config: normalized,
		state:  StateCreated,
		done:   make(chan struct{}),
	}, nil
}

// Name returns the stable App component name.
func (set *RemoteKeySet) Name() string {
	if set == nil {
		return "security.jwt.jwks"
	}
	return set.config.componentName
}

// Start retrieves the initial JWKS before making the provider ready.
func (set *RemoteKeySet) Start(ctx context.Context) error {
	if set == nil || ctx == nil {
		return fmt.Errorf("%w: remote key set or context is nil", ErrInvalidState)
	}
	set.mu.Lock()
	switch set.state {
	case StateRunning:
		set.mu.Unlock()
		return nil
	case StateFailed:
		err := set.startErr
		set.mu.Unlock()
		return err
	case StateCreated:
		set.state = StateStarting
	default:
		state := set.state
		set.mu.Unlock()
		return fmt.Errorf("%w: cannot start from %s", ErrInvalidState, state)
	}
	set.mu.Unlock()

	if err := set.refreshKeys(ctx); err != nil {
		set.failures.Add(1)
		set.lastFailed.Store(true)
		set.mu.Lock()
		set.state = StateFailed
		set.startErr = ErrKeyUnavailable
		set.mu.Unlock()
		return ErrKeyUnavailable
	}
	if cause := context.Cause(ctx); cause != nil {
		set.mu.Lock()
		set.state = StateFailed
		set.startErr = cause
		set.mu.Unlock()
		return cause
	}

	runContext, cancel := context.WithCancel(context.Background())
	set.mu.Lock()
	set.cancel = cancel
	set.state = StateRunning
	set.mu.Unlock()
	go set.run(runContext)
	return nil
}

// Stop adapts RemoteKeySet to app.Component.
func (set *RemoteKeySet) Stop(ctx context.Context) error {
	return set.Shutdown(ctx)
}

// Shutdown stops periodic refresh. It is safe to call repeatedly.
func (set *RemoteKeySet) Shutdown(ctx context.Context) error {
	if set == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is nil", ErrInvalidState)
	}
	set.mu.Lock()
	switch set.state {
	case StateStopped:
		err := set.shutdownErr
		set.mu.Unlock()
		return err
	case StateCreated, StateFailed:
		set.state = StateStopped
		set.doneOnce.Do(func() { close(set.done) })
		set.mu.Unlock()
		return nil
	case StateRunning:
		set.state = StateStopping
		cancel := set.cancel
		done := set.done
		set.mu.Unlock()
		cancel()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	case StateStopping:
		done := set.done
		set.mu.Unlock()
		select {
		case <-done:
			set.mu.Lock()
			err := set.shutdownErr
			set.mu.Unlock()
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	default:
		state := set.state
		set.mu.Unlock()
		return fmt.Errorf("%w: cannot shut down from %s", ErrInvalidState, state)
	}
}

// Key resolves an exact key pair. A miss can initiate one rate-limited,
// single-flight refresh to close normal identity-provider rotation windows.
func (set *RemoteKeySet) Key(
	ctx context.Context,
	keyid string,
	algorithm string,
) (any, error) {
	if set == nil || ctx == nil {
		return nil, ErrKeyUnavailable
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	set.mu.Lock()
	running := set.state == StateRunning
	set.mu.Unlock()
	if !running {
		return nil, ErrKeyUnavailable
	}
	if key, found := set.lookup(keyid, algorithm); found {
		return key, nil
	}
	set.keyMisses.Add(1)
	if set.refreshAllowed(time.Now()) {
		_ = set.refreshOnDemand(ctx)
		if key, found := set.lookup(keyid, algorithm); found {
			return key, nil
		}
	}
	return nil, ErrKeyNotFound
}

// Description returns bounded value-free lifecycle state.
func (set *RemoteKeySet) Description() RemoteDescription {
	if set == nil {
		return RemoteDescription{State: StateStopped}
	}
	set.mu.Lock()
	state := set.state
	set.mu.Unlock()
	count := 0
	if snapshot := set.snapshot.Load(); snapshot != nil {
		count = len(snapshot.keys)
	}
	return RemoteDescription{
		State:      state,
		Ready:      state == StateRunning && count > 0,
		LastFailed: set.lastFailed.Load(),
		KeyCount:   count,
		Refreshes:  set.refreshes.Load(),
		Failures:   set.failures.Load(),
		KeyMisses:  set.keyMisses.Load(),
	}
}

func (set *RemoteKeySet) run(ctx context.Context) {
	defer func() {
		set.mu.Lock()
		set.state = StateStopped
		set.mu.Unlock()
		set.doneOnce.Do(func() { close(set.done) })
	}()
	ticker := time.NewTicker(set.config.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshContext, cancel := context.WithTimeout(
				ctx,
				set.config.requestTimeout,
			)
			err := set.refreshKeys(refreshContext)
			cancel()
			if err != nil {
				set.failures.Add(1)
				set.lastFailed.Store(true)
			}
		}
	}
}

func (set *RemoteKeySet) lookup(
	keyid string,
	algorithm string,
) (any, bool) {
	if !validKeyid(keyid) || !supportedAlgorithm(algorithm) {
		return nil, false
	}
	snapshot := set.snapshot.Load()
	if snapshot == nil {
		return nil, false
	}
	key, found := snapshot.keys[keyReference{
		keyid:     keyid,
		algorithm: algorithm,
	}]
	if !found {
		return nil, false
	}
	return clonePublicKey(key), true
}

func (set *RemoteKeySet) refreshAllowed(now time.Time) bool {
	last := time.Unix(0, set.lastAttempt.Load())
	return now.Sub(last) >= set.config.minimumRefreshInterval
}

func (set *RemoteKeySet) refreshOnDemand(ctx context.Context) error {
	now := time.Now()
	previous := set.lastAttempt.Load()
	if now.Sub(time.Unix(0, previous)) < set.config.minimumRefreshInterval ||
		!set.lastAttempt.CompareAndSwap(previous, now.UnixNano()) {
		return ErrKeyNotFound
	}
	result := set.refresh.DoChan("jwks", func() (any, error) {
		refreshContext, cancel := context.WithTimeout(
			context.Background(),
			set.config.requestTimeout,
		)
		defer cancel()
		return nil, set.fetchAndStore(refreshContext)
	})
	select {
	case completed := <-result:
		if completed.Err != nil {
			set.failures.Add(1)
			set.lastFailed.Store(true)
		}
		return completed.Err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (set *RemoteKeySet) refreshKeys(ctx context.Context) error {
	set.lastAttempt.Store(time.Now().UnixNano())
	_, err, _ := set.refresh.Do("jwks", func() (any, error) {
		return nil, set.fetchAndStore(ctx)
	})
	return err
}

func (set *RemoteKeySet) fetchAndStore(ctx context.Context) error {
	requestContext, cancel := context.WithTimeout(
		ctx,
		set.config.requestTimeout,
	)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestContext,
		http.MethodGet,
		set.config.endpoint.String(),
		nil,
	)
	if err != nil {
		return ErrKeyUnavailable
	}
	request.Header.Set("Accept", "application/jwk-set+json, application/json")
	response, err := set.config.client.Do(request)
	if err != nil {
		return ErrKeyUnavailable
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < http.StatusOK ||
		response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(
			io.Discard,
			io.LimitReader(response.Body, 4*1024),
		)
		return ErrKeyUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(
		response.Body,
		set.config.maxResponseBytes+1,
	))
	if err != nil || int64(len(body)) > set.config.maxResponseBytes {
		return ErrKeyUnavailable
	}
	keys, err := parseJWKS(body, set.config.algorithmSet)
	if err != nil {
		return ErrKeyUnavailable
	}
	set.snapshot.Store(&keySnapshot{keys: keys})
	set.refreshes.Add(1)
	set.lastFailed.Store(false)
	return nil
}

func normalizeRemoteConfig(
	config RemoteConfig,
) (normalizedRemoteConfig, error) {
	componentName := config.ComponentName
	if componentName == "" {
		componentName = "security.jwt.jwks"
	}
	if !validClaimName(componentName) {
		return normalizedRemoteConfig{}, fmt.Errorf(
			"%w: component name is malformed",
			ErrInvalidConfig,
		)
	}
	endpoint, err := url.Parse(config.URL)
	if err != nil ||
		endpoint == nil ||
		endpoint.Host == "" ||
		endpoint.User != nil ||
		endpoint.Fragment != "" ||
		endpoint.RawQuery != "" ||
		len(config.URL) > maxTokenValueBytes {
		return normalizedRemoteConfig{}, fmt.Errorf(
			"%w: JWKS url is malformed",
			ErrInvalidConfig,
		)
	}
	switch endpoint.Scheme {
	case "https":
	case "http":
		host := endpoint.Hostname()
		ip := net.ParseIP(host)
		loopback := strings.EqualFold(host, "localhost") ||
			ip != nil && ip.IsLoopback()
		if !config.AllowInsecureHTTP || !loopback {
			return normalizedRemoteConfig{}, fmt.Errorf(
				"%w: insecure JWKS is restricted to explicit loopback",
				ErrInvalidConfig,
			)
		}
	default:
		return normalizedRemoteConfig{}, fmt.Errorf(
			"%w: JWKS url must use https",
			ErrInvalidConfig,
		)
	}
	algorithms := config.Algorithms
	if len(algorithms) == 0 {
		algorithms = []string{"EdDSA", "ES256", "RS256"}
	}
	algorithms, err = normalizeAlgorithms(algorithms)
	if err != nil {
		return normalizedRemoteConfig{}, err
	}
	algorithmSet := make(map[string]struct{}, len(algorithms))
	for _, algorithm := range algorithms {
		algorithmSet[algorithm] = struct{}{}
	}
	refreshInterval, err := remoteDuration(
		"refresh interval",
		config.RefreshInterval,
		defaultRefreshInterval,
		time.Second,
		24*time.Hour,
	)
	if err != nil {
		return normalizedRemoteConfig{}, err
	}
	minimumRefreshInterval, err := remoteDuration(
		"minimum refresh interval",
		config.MinimumRefreshInterval,
		defaultMinimumRefreshWait,
		100*time.Millisecond,
		time.Hour,
	)
	if err != nil {
		return normalizedRemoteConfig{}, err
	}
	if minimumRefreshInterval > refreshInterval {
		return normalizedRemoteConfig{}, fmt.Errorf(
			"%w: minimum refresh interval exceeds periodic interval",
			ErrInvalidConfig,
		)
	}
	requestTimeout, err := remoteDuration(
		"request timeout",
		config.RequestTimeout,
		defaultRequestTimeout,
		100*time.Millisecond,
		time.Minute,
	)
	if err != nil {
		return normalizedRemoteConfig{}, err
	}
	maxBytes := config.MaxResponseBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxResponseBytes
	}
	if maxBytes < 1024 || maxBytes > 8*1024*1024 {
		return normalizedRemoteConfig{}, fmt.Errorf(
			"%w: JWKS response size is outside supported bounds",
			ErrInvalidConfig,
		)
	}
	client := &http.Client{}
	if config.HTTPClient != nil {
		clientCopy := *config.HTTPClient
		client = &clientCopy
	}
	previousRedirect := client.CheckRedirect
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= 3 ||
			len(via) > 0 &&
				(request.URL.Scheme != via[0].URL.Scheme ||
					!strings.EqualFold(request.URL.Host, via[0].URL.Host)) {
			return http.ErrUseLastResponse
		}
		if previousRedirect != nil {
			return previousRedirect(request, via)
		}
		return nil
	}
	client.Timeout = 0
	return normalizedRemoteConfig{
		componentName:          componentName,
		endpoint:               endpoint,
		algorithmSet:           algorithmSet,
		client:                 client,
		refreshInterval:        refreshInterval,
		minimumRefreshInterval: minimumRefreshInterval,
		requestTimeout:         requestTimeout,
		maxResponseBytes:       maxBytes,
	}, nil
}

func remoteDuration(
	name string,
	value time.Duration,
	defaultValue time.Duration,
	minimum time.Duration,
	maximum time.Duration,
) (time.Duration, error) {
	if value == 0 {
		value = defaultValue
	}
	if value < minimum || value > maximum {
		return 0, fmt.Errorf(
			"%w: %s is outside supported bounds",
			ErrInvalidConfig,
			name,
		)
	}
	return value, nil
}
