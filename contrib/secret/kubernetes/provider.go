// Package kubernetes resolves and watches namespace-scoped Kubernetes Secret
// keys without copying secret material into Keelith configuration snapshots.
package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/secret"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

const (
	defaultMaxBytes = 1024 * 1024
	maxAllowedBytes = 16 * 1024 * 1024
)

var (
	// ErrInvalidOption reports malformed namespace, key, client, or budget.
	ErrInvalidOption = errors.New("secret/kubernetes: invalid option")
	// ErrInvalidObject reports an unexpected or malformed Kubernetes object.
	ErrInvalidObject = errors.New("secret/kubernetes: invalid object")
	// ErrTooLarge reports secret material beyond the configured byte budget.
	ErrTooLarge = errors.New("secret/kubernetes: value is too large")
	// ErrWatchClosed reports an unexpected Kubernetes Watch termination.
	ErrWatchClosed = errors.New("secret/kubernetes: watch closed")
	// ErrClosed reports an operation after Provider shutdown.
	ErrClosed = errors.New("secret/kubernetes: provider closed")
	// ErrUnavailable reports a Kubernetes API failure without its cause.
	ErrUnavailable = errors.New("secret/kubernetes: unavailable")
)

// Options configure one namespace-scoped Provider.
//
// Provider-local references use <secret-name>/<data-key>. For example,
// secret://kubernetes/orders-runtime-secrets/server-tls.json.
type Options struct {
	Namespace string `config:"namespace"`
	MaxBytes  int    `config:"max_bytes"`
}

// Description is a material-free operational snapshot.
type Description struct {
	MaxBytes int
	Closed   bool
	Watchers int
	Degraded bool
}

// SecretClient is the read-only portion of the official generated client.
type SecretClient interface {
	Get(
		context.Context,
		string,
		metav1.GetOptions,
	) (*corev1.Secret, error)
	List(
		context.Context,
		metav1.ListOptions,
	) (*corev1.SecretList, error)
	Watch(
		context.Context,
		metav1.ListOptions,
	) (watch.Interface, error)
}

// Provider implements secret.Provider for one Kubernetes namespace.
type Provider struct {
	client  SecretClient
	options Options

	mu        sync.Mutex
	closed    bool
	watchers  map[*watcher]struct{}
	lastError error
	closeOnce sync.Once
}

type keySpec struct {
	name    string
	dataKey string
}

// New constructs a Provider around one namespace-scoped Secret client.
func New(client SecretClient, options Options) (*Provider, error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: Secret client is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return &Provider{
		client:   client,
		options:  normalized,
		watchers: make(map[*watcher]struct{}),
	}, nil
}

// Open constructs a Provider from an explicit Kubernetes rest config.
func Open(restConfig *rest.Config, options Options) (*Provider, error) {
	if restConfig == nil {
		return nil, fmt.Errorf("%w: rest config is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	configCopy := rest.CopyConfig(restConfig)
	rest.AddUserAgent(configCopy, "keelith-kubernetes-secret-provider")
	client, err := coreclient.NewForConfig(configCopy)
	if err != nil {
		return nil, fmt.Errorf(
			"secret/kubernetes: create core client: %w",
			err,
		)
	}
	return New(client.Secrets(normalized.Namespace), normalized)
}

// OpenInCluster constructs a Provider from the pod service account.
func OpenInCluster(options Options) (*Provider, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf(
			"secret/kubernetes: in-cluster config: %w",
			err,
		)
	}
	return Open(config, options)
}

// NormalizeOptions applies stable defaults and validates resource budgets.
func NormalizeOptions(input Options) (Options, error) {
	options := input
	options.Namespace = strings.TrimSpace(options.Namespace)
	if len(validation.IsDNS1123Label(options.Namespace)) != 0 {
		return Options{}, fmt.Errorf(
			"%w: namespace %q is invalid",
			ErrInvalidOption,
			options.Namespace,
		)
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = defaultMaxBytes
	}
	if options.MaxBytes < 1 || options.MaxBytes > maxAllowedBytes {
		return Options{}, fmt.Errorf(
			"%w: max bytes is outside 1..%d",
			ErrInvalidOption,
			maxAllowedBytes,
		)
	}
	return options, nil
}

// ValidateOptions validates a Provider without constructing a client.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

// Resolve reads one exact Secret data key.
func (provider *Provider) Resolve(
	ctx context.Context,
	key string,
) (secret.Value, error) {
	if provider == nil || ctx == nil {
		return secret.Value{}, fmt.Errorf(
			"%w: provider or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return secret.Value{}, cause
	}
	if err := provider.requireOpen(); err != nil {
		return secret.Value{}, err
	}
	target, err := parseKey(key)
	if err != nil {
		return secret.Value{}, err
	}
	object, err := provider.client.Get(
		ctx,
		target.name,
		metav1.GetOptions{},
	)
	if apierrors.IsNotFound(err) {
		provider.recordError(secret.ErrNotFound)
		return secret.Value{}, secret.ErrNotFound
	}
	if err != nil {
		provider.recordError(err)
		return secret.Value{}, fmt.Errorf(
			"secret/kubernetes: get Secret: %w",
			err,
		)
	}
	value, _, err := provider.value(object, target)
	provider.recordError(err)
	return value, err
}

// ResolveClassified resolves one exact Secret data key without returning or
// inspecting arbitrary Kubernetes client errors or context cancellation
// causes. Non-success outcomes always return a zero secret.Value.
func (provider *Provider) ResolveClassified(
	ctx context.Context,
	key string,
) (secret.Value, secret.ResolveStatus) {
	if provider == nil || isNil(ctx) {
		return secret.Value{}, secret.ResolveStatusInvalid
	}
	if ctx.Err() != nil {
		return secret.Value{}, secret.ResolveStatusCanceled
	}
	if err := provider.requireOpen(); err != nil {
		return secret.Value{}, secret.ResolveStatusUnavailable
	}
	target, err := parseKey(key)
	if err != nil {
		return secret.Value{}, secret.ResolveStatusInvalid
	}
	object, err := provider.client.Get(
		ctx,
		target.name,
		metav1.GetOptions{},
	)
	if ctx.Err() != nil {
		return secret.Value{}, secret.ResolveStatusCanceled
	}
	if err != nil {
		if isNotFoundStatus(err) {
			provider.recordError(secret.ErrNotFound)
			return secret.Value{}, secret.ResolveStatusNotFound
		}
		provider.recordError(ErrUnavailable)
		return secret.Value{}, secret.ResolveStatusUnavailable
	}

	value, status, diagnostic := provider.classifiedValue(object, target)
	provider.recordError(diagnostic)
	if status != secret.ResolveStatusSuccess {
		return secret.Value{}, status
	}
	return value, status
}

// Watch establishes a resourceVersion-safe List-to-Watch stream.
//
// The first Next returns the value observed by the establishing List. This
// closes the Resolve-to-Watch race for lifecycle consumers; a repeated version
// is harmless because secret values are immutable and versioned.
func (provider *Provider) Watch(
	ctx context.Context,
	key string,
) (secret.Watcher, error) {
	if provider == nil || ctx == nil {
		return nil, fmt.Errorf(
			"%w: provider or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := provider.requireOpen(); err != nil {
		return nil, err
	}
	target, err := parseKey(key)
	if err != nil {
		return nil, err
	}
	fieldSelector := fields.OneTermEqualSelector(
		"metadata.name",
		target.name,
	).String()
	list, err := provider.client.List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"secret/kubernetes: establish watch: %w",
			err,
		)
	}
	object, err := exactSecret(list, target.name)
	if err != nil {
		return nil, err
	}
	initial, digest, err := provider.value(object, target)
	if err != nil {
		return nil, err
	}
	if err := validateResourceVersion(list.ResourceVersion); err != nil {
		return nil, err
	}
	watchContext, cancel := context.WithCancel(ctx)
	stream, err := provider.client.Watch(
		watchContext,
		metav1.ListOptions{
			FieldSelector:       fieldSelector,
			ResourceVersion:     list.ResourceVersion,
			AllowWatchBookmarks: true,
		},
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf(
			"secret/kubernetes: watch Secret: %w",
			err,
		)
	}
	if isNil(stream) {
		cancel()
		return nil, fmt.Errorf("%w: watch is nil", ErrInvalidOption)
	}
	current := &watcher{
		provider:   provider,
		target:     target,
		context:    watchContext,
		cancel:     cancel,
		stream:     stream,
		latestRV:   list.ResourceVersion,
		lastDigest: digest,
		updates:    make(chan secret.Value, 1),
		terminal:   make(chan struct{}),
		done:       make(chan struct{}),
	}
	current.updates <- initial
	provider.mu.Lock()
	if provider.closed {
		provider.mu.Unlock()
		cancel()
		stream.Stop()
		return nil, ErrClosed
	}
	provider.watchers[current] = struct{}{}
	provider.lastError = nil
	provider.mu.Unlock()
	go current.run()
	return current, nil
}

// Shutdown closes active watches and rejects new operations.
func (provider *Provider) Shutdown(ctx context.Context) error {
	if provider == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	provider.closeOnce.Do(func() {
		provider.mu.Lock()
		provider.closed = true
		watchers := make([]*watcher, 0, len(provider.watchers))
		for current := range provider.watchers {
			watchers = append(watchers, current)
		}
		provider.mu.Unlock()
		for _, current := range watchers {
			_ = current.Close()
		}
	})
	return context.Cause(ctx)
}

// LastError returns the latest API, object, material, or watch failure.
func (provider *Provider) LastError() error {
	if provider == nil {
		return ErrClosed
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.lastError
}

// WatcherCount returns active logical watchers.
func (provider *Provider) WatcherCount() int {
	if provider == nil {
		return 0
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return len(provider.watchers)
}

// Describe returns material-free runtime state.
func (provider *Provider) Describe() Description {
	if provider == nil {
		return Description{Closed: true, Degraded: true}
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return Description{
		MaxBytes: provider.options.MaxBytes,
		Closed:   provider.closed,
		Watchers: len(provider.watchers),
		Degraded: provider.lastError != nil,
	}
}

func (provider *Provider) value(
	object *corev1.Secret,
	target keySpec,
) (secret.Value, [sha256.Size]byte, error) {
	if object == nil ||
		object.Name != target.name ||
		object.Namespace != "" &&
			object.Namespace != provider.options.Namespace {
		return secret.Value{}, [sha256.Size]byte{}, fmt.Errorf(
			"%w: Secret identity does not match reference",
			ErrInvalidObject,
		)
	}
	content, exists := object.Data[target.dataKey]
	if !exists {
		return secret.Value{}, [sha256.Size]byte{}, secret.ErrNotFound
	}
	if len(content) > provider.options.MaxBytes {
		return secret.Value{}, [sha256.Size]byte{}, fmt.Errorf(
			"%w: %d bytes exceeds %d",
			ErrTooLarge,
			len(content),
			provider.options.MaxBytes,
		)
	}
	digest := sha256.Sum256(content)
	value, err := secret.NewValue(
		content,
		"kubernetes:"+
			normalizedResourceVersion(object.ResourceVersion)+
			":sha256:"+
			hex.EncodeToString(digest[:]),
		time.Time{},
	)
	if err != nil {
		return secret.Value{}, [sha256.Size]byte{}, err
	}
	return value, digest, nil
}

func (provider *Provider) classifiedValue(
	object *corev1.Secret,
	target keySpec,
) (secret.Value, secret.ResolveStatus, error) {
	if object == nil ||
		object.Name != target.name ||
		object.Namespace != provider.options.Namespace {
		return secret.Value{}, secret.ResolveStatusInvalid, ErrInvalidObject
	}
	content, exists := object.Data[target.dataKey]
	if !exists {
		return secret.Value{}, secret.ResolveStatusNotFound, secret.ErrNotFound
	}
	if len(content) > provider.options.MaxBytes {
		return secret.Value{}, secret.ResolveStatusInvalid, ErrTooLarge
	}
	digest := sha256.Sum256(content)
	value, err := secret.NewValue(
		content,
		"kubernetes:"+
			normalizedResourceVersion(object.ResourceVersion)+
			":sha256:"+
			hex.EncodeToString(digest[:]),
		time.Time{},
	)
	if err != nil {
		return secret.Value{}, secret.ResolveStatusInvalid, ErrInvalidObject
	}
	return value, secret.ResolveStatusSuccess, nil
}

func (provider *Provider) requireOpen() error {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.closed {
		return ErrClosed
	}
	return nil
}

func (provider *Provider) recordError(err error) {
	provider.mu.Lock()
	provider.lastError = err
	provider.mu.Unlock()
}

func (provider *Provider) removeWatcher(current *watcher) {
	provider.mu.Lock()
	delete(provider.watchers, current)
	provider.mu.Unlock()
}

func exactSecret(
	list *corev1.SecretList,
	name string,
) (*corev1.Secret, error) {
	if list == nil {
		return nil, fmt.Errorf("%w: Secret list is nil", ErrInvalidObject)
	}
	if len(list.Items) == 0 {
		return nil, secret.ErrNotFound
	}
	if len(list.Items) != 1 || list.Items[0].Name != name {
		return nil, fmt.Errorf(
			"%w: exact query returned %d Secrets",
			ErrInvalidObject,
			len(list.Items),
		)
	}
	return &list.Items[0], nil
}

func parseKey(key string) (keySpec, error) {
	if strings.TrimSpace(key) != key || len(key) > 512 {
		return keySpec{}, fmt.Errorf(
			"%w: key must be <secret-name>/<data-key>",
			ErrInvalidOption,
		)
	}
	parts := strings.Split(key, "/")
	if len(parts) != 2 ||
		len(validation.IsDNS1123Subdomain(parts[0])) != 0 ||
		len(validation.IsConfigMapKey(parts[1])) != 0 {
		return keySpec{}, fmt.Errorf(
			"%w: key %q must be <secret-name>/<data-key>",
			ErrInvalidOption,
			key,
		)
	}
	return keySpec{name: parts[0], dataKey: parts[1]}, nil
}

func validateResourceVersion(value string) error {
	if _, err := resourceversion.CompareResourceVersion(value, value); err != nil {
		return fmt.Errorf(
			"%w: resourceVersion: %w",
			ErrInvalidObject,
			err,
		)
	}
	return nil
}

func normalizedResourceVersion(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "0"
	}
	return value
}

func isNotFoundStatus(err error) bool {
	// Wrapped and interface-shaped errors remain unavailable: inspecting their
	// chains would invoke arbitrary Is, As, or Unwrap implementations.
	statusError, ok := err.(*apierrors.StatusError) //nolint:errorlint
	return ok &&
		statusError != nil &&
		statusError.ErrStatus.Reason == metav1.StatusReasonNotFound
}

type watcher struct {
	provider *Provider
	target   keySpec
	context  context.Context
	cancel   context.CancelFunc
	stream   watch.Interface

	latestRV   string
	lastDigest [sha256.Size]byte

	updates  chan secret.Value
	terminal chan struct{}
	done     chan struct{}

	mu          sync.Mutex
	terminalErr error
	closeOnce   sync.Once
	failOnce    sync.Once
}

func (watcher *watcher) Next(ctx context.Context) (secret.Value, error) {
	if watcher == nil || ctx == nil {
		return secret.Value{}, fmt.Errorf(
			"%w: watcher or context is nil",
			ErrInvalidOption,
		)
	}
	select {
	case value := <-watcher.updates:
		return value, nil
	case <-watcher.terminal:
		watcher.mu.Lock()
		err := watcher.terminalErr
		watcher.mu.Unlock()
		return secret.Value{}, err
	case <-watcher.done:
		return secret.Value{}, secret.ErrWatcherClosed
	case <-ctx.Done():
		return secret.Value{}, context.Cause(ctx)
	}
}

func (watcher *watcher) Close() error {
	if watcher == nil {
		return nil
	}
	watcher.closeOnce.Do(func() {
		watcher.provider.removeWatcher(watcher)
		watcher.cancel()
		watcher.stream.Stop()
		close(watcher.done)
	})
	return nil
}

func (watcher *watcher) run() {
	for {
		select {
		case <-watcher.context.Done():
			return
		case event, open := <-watcher.stream.ResultChan():
			if !open {
				watcher.fail(ErrWatchClosed)
				return
			}
			if err := watcher.apply(event); err != nil {
				watcher.fail(err)
				return
			}
		}
	}
}

func (watcher *watcher) apply(event watch.Event) error {
	if event.Type == watch.Error {
		err := apierrors.FromObject(event.Object)
		return fmt.Errorf("secret/kubernetes: watch event: %w", err)
	}
	if event.Type == watch.Bookmark {
		return nil
	}
	object, ok := event.Object.(*corev1.Secret)
	if !ok || object == nil {
		return fmt.Errorf(
			"%w: watch object is %T",
			ErrInvalidObject,
			event.Object,
		)
	}
	if object.Name != watcher.target.name {
		return fmt.Errorf(
			"%w: watch returned Secret %q",
			ErrInvalidObject,
			object.Name,
		)
	}
	comparison, err := resourceversion.CompareResourceVersion(
		object.ResourceVersion,
		watcher.latestRV,
	)
	if err != nil {
		return fmt.Errorf(
			"%w: resourceVersion: %w",
			ErrInvalidObject,
			err,
		)
	}
	if comparison <= 0 {
		return nil
	}
	watcher.latestRV = object.ResourceVersion
	switch event.Type {
	case watch.Deleted:
		return secret.ErrNotFound
	case watch.Added, watch.Modified:
	default:
		return fmt.Errorf(
			"%w: unsupported watch event %q",
			ErrInvalidObject,
			event.Type,
		)
	}
	value, digest, err := watcher.provider.value(object, watcher.target)
	if err != nil {
		return err
	}
	watcher.provider.recordError(nil)
	if digest == watcher.lastDigest {
		return nil
	}
	watcher.lastDigest = digest
	watcher.publish(value)
	return nil
}

func (watcher *watcher) publish(value secret.Value) {
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
	case watcher.updates <- value:
	default:
	}
}

func (watcher *watcher) fail(err error) {
	watcher.failOnce.Do(func() {
		watcher.provider.recordError(err)
		watcher.provider.removeWatcher(watcher)
		watcher.cancel()
		watcher.stream.Stop()
		select {
		case <-watcher.updates:
		default:
		}
		watcher.mu.Lock()
		watcher.terminalErr = err
		watcher.mu.Unlock()
		close(watcher.terminal)
	})
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ secret.Provider = (*Provider)(nil)
var _ secret.ClassifiedProvider = (*Provider)(nil)
