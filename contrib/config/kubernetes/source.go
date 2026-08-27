// Package kubernetes adapts one named configmap key to a revisioned Keelith
// configuration Source.
package kubernetes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/keelab/keelith/config"
	"gopkg.in/yaml.v3"
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
	maxAllowedBytes = 4 * 1024 * 1024
)

var (
	// ErrInvalidOption reports malformed topology, identity, format, or budget.
	ErrInvalidOption = errors.New("kubernetes config: invalid option")
	// ErrNotFound reports a required configmap or key that does not exist.
	ErrNotFound = errors.New("kubernetes config: document not found")
	// ErrTooLarge reports content beyond the configured byte budget.
	ErrTooLarge = errors.New("kubernetes config: document is too large")
	// ErrInvalidDocument reports invalid json/yaml or ambiguous key storage.
	ErrInvalidDocument = errors.New("kubernetes config: invalid document")
	// ErrWatchClosed reports an unexpected Kubernetes Watch termination.
	ErrWatchClosed = errors.New("kubernetes config: watch closed")
	// ErrClosed reports an operation after Source shutdown.
	ErrClosed = errors.New("kubernetes config: source closed")
)

// Format identifies one complete configmap document.
type Format string

const (
	// FormatJSON decodes one json object.
	FormatJSON Format = "json"
	// FormatYAML decodes one yaml object.
	FormatYAML Format = "yaml"
)

// ConfigMapClient is the read-only portion of the official generated client.
type ConfigMapClient interface {
	List(
		context.Context,
		metav1.ListOptions,
	) (*corev1.ConfigMapList, error)
	Watch(
		context.Context,
		metav1.ListOptions,
	) (watch.Interface, error)
}

// Options identify one exact configmap key.
type Options struct {
	Namespace    string `config:"namespace"`
	Name         string `config:"name"`
	Key          string `config:"key"`
	Format       Format `config:"format"`
	AllowMissing bool   `config:"allow_missing"`
	MaxBytes     int    `config:"max_bytes"`
}

// Description is a value-free runtime snapshot.
type Description struct {
	Format       Format
	AllowMissing bool
	MaxBytes     int
	Closed       bool
	Watchers     int
	Degraded     bool
}

// Source implements config.Source around one namespace-scoped configmap.
type Source struct {
	client        ConfigMapClient
	options       Options
	fieldSelector string

	mu         sync.Mutex
	closed     bool
	watchers   map[*watcher]struct{}
	lastError  error
	generation uint64
	latestRV   string

	closeOnce sync.Once
}

// New constructs a Source around one namespace-scoped configmap client.
func New(client ConfigMapClient, options Options) (*Source, error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: configmap client is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return &Source{
		client:  client,
		options: normalized,
		fieldSelector: fields.OneTermEqualSelector(
			"metadata.name",
			normalized.Name,
		).String(),
		watchers: make(map[*watcher]struct{}),
	}, nil
}

// Open constructs a Source from an explicit Kubernetes rest config.
func Open(restConfig *rest.Config, options Options) (*Source, error) {
	if restConfig == nil {
		return nil, fmt.Errorf("%w: rest config is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	configCopy := rest.CopyConfig(restConfig)
	rest.AddUserAgent(configCopy, "keelith-configmap-source")
	client, err := coreclient.NewForConfig(configCopy)
	if err != nil {
		return nil, fmt.Errorf(
			"kubernetes config: create core client: %w",
			err,
		)
	}
	return New(client.ConfigMaps(normalized.Namespace), normalized)
}

// OpenInCluster constructs a Source from the pod service account.
func OpenInCluster(options Options) (*Source, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf(
			"kubernetes config: in-cluster config: %w",
			err,
		)
	}
	return Open(config, options)
}

// NormalizeOptions applies stable defaults and validates one exact document.
func NormalizeOptions(input Options) (Options, error) {
	options := input
	options.Namespace = strings.TrimSpace(options.Namespace)
	options.Name = strings.TrimSpace(options.Name)
	options.Key = strings.TrimSpace(options.Key)
	if len(validation.IsDNS1123Label(options.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(options.Name)) != 0 {
		return Options{}, fmt.Errorf(
			"%w: namespace or configmap name is invalid",
			ErrInvalidOption,
		)
	}
	if len(validation.IsConfigMapKey(options.Key)) != 0 {
		return Options{}, fmt.Errorf(
			"%w: configmap key %q is invalid",
			ErrInvalidOption,
			options.Key,
		)
	}
	if options.Format == "" {
		switch strings.ToLower(filepath.Ext(options.Key)) {
		case ".json":
			options.Format = FormatJSON
		case ".yaml", ".yml":
			options.Format = FormatYAML
		default:
			return Options{}, fmt.Errorf(
				"%w: format cannot be inferred from key %q",
				ErrInvalidOption,
				options.Key,
			)
		}
	}
	if options.Format != FormatJSON && options.Format != FormatYAML {
		return Options{}, fmt.Errorf(
			"%w: format %q is invalid",
			ErrInvalidOption,
			options.Format,
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

// ValidateOptions validates a source without constructing a client.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

// Start verifies the initial configmap document.
func (source *Source) Start(ctx context.Context) error {
	_, err := source.Load(ctx)
	return err
}

// Load performs one exact field-selected List and publishes its local
// generation as the newest observed state.
func (source *Source) Load(ctx context.Context) (config.Snapshot, error) {
	if source == nil || ctx == nil {
		return config.Snapshot{}, fmt.Errorf(
			"%w: source or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return config.Snapshot{}, cause
	}
	if err := source.requireOpen(); err != nil {
		return config.Snapshot{}, err
	}
	list, err := source.client.List(ctx, metav1.ListOptions{
		FieldSelector: source.fieldSelector,
	})
	if err != nil {
		source.recordError(err)
		return config.Snapshot{}, fmt.Errorf(
			"kubernetes config: list configmap: %w",
			err,
		)
	}
	snapshot, err := source.snapshotFromList(list)
	if err != nil {
		source.recordError(err)
		return config.Snapshot{}, err
	}
	if err := source.recordLoad(list.ResourceVersion); err != nil {
		return config.Snapshot{}, err
	}
	return snapshot, nil
}

// Watch establishes a resourceVersion-safe future Watch without replaying the
// initial configmap value.
func (source *Source) Watch(ctx context.Context) (config.Watcher, error) {
	if source == nil || ctx == nil {
		return nil, fmt.Errorf(
			"%w: source or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := source.requireOpen(); err != nil {
		return nil, err
	}
	list, err := source.client.List(ctx, metav1.ListOptions{
		FieldSelector: source.fieldSelector,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"kubernetes config: establish watch: %w",
			err,
		)
	}
	if _, err := source.snapshotFromList(list); err != nil {
		return nil, err
	}
	watchContext, cancel := context.WithCancel(ctx)
	stream, err := source.client.Watch(
		watchContext,
		metav1.ListOptions{
			FieldSelector:       source.fieldSelector,
			ResourceVersion:     list.ResourceVersion,
			AllowWatchBookmarks: true,
		},
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf(
			"kubernetes config: watch configmap: %w",
			err,
		)
	}
	if isNil(stream) {
		cancel()
		return nil, fmt.Errorf("%w: watch is nil", ErrInvalidOption)
	}
	current := &watcher{
		source:   source,
		context:  watchContext,
		cancel:   cancel,
		stream:   stream,
		updates:  make(chan update, 1),
		terminal: make(chan error, 1),
		done:     make(chan struct{}),
	}
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		cancel()
		stream.Stop()
		return nil, ErrClosed
	}
	source.watchers[current] = struct{}{}
	source.mu.Unlock()
	go current.run()
	return current, nil
}

// Shutdown closes active watches.
func (source *Source) Shutdown(context.Context) error {
	if source == nil {
		return nil
	}
	source.closeOnce.Do(func() {
		source.mu.Lock()
		source.closed = true
		watchers := make([]*watcher, 0, len(source.watchers))
		for current := range source.watchers {
			watchers = append(watchers, current)
		}
		source.mu.Unlock()
		for _, current := range watchers {
			_ = current.Close()
		}
	})
	return nil
}

// LastError returns the latest API, decode, delete, or watch failure.
func (source *Source) LastError() error {
	if source == nil {
		return ErrClosed
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.lastError
}

// WatcherCount returns active logical watchers.
func (source *Source) WatcherCount() int {
	if source == nil {
		return 0
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return len(source.watchers)
}

// Describe returns value-free runtime state.
func (source *Source) Describe() Description {
	if source == nil {
		return Description{Closed: true, Degraded: true}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return Description{
		Format:       source.options.Format,
		AllowMissing: source.options.AllowMissing,
		MaxBytes:     source.options.MaxBytes,
		Closed:       source.closed,
		Watchers:     len(source.watchers),
		Degraded:     source.lastError != nil,
	}
}

func (source *Source) snapshotFromList(
	list *corev1.ConfigMapList,
) (config.Snapshot, error) {
	if list == nil {
		return config.Snapshot{}, fmt.Errorf(
			"%w: configmap list is nil",
			ErrInvalidDocument,
		)
	}
	if len(list.Items) > 1 {
		return config.Snapshot{}, fmt.Errorf(
			"%w: exact query returned %d ConfigMaps",
			ErrInvalidDocument,
			len(list.Items),
		)
	}
	if len(list.Items) == 0 {
		return source.missingSnapshot(list.ResourceVersion)
	}
	return source.snapshot(&list.Items[0])
}

func (source *Source) snapshot(
	configMap *corev1.ConfigMap,
) (config.Snapshot, error) {
	if configMap == nil {
		return config.Snapshot{}, fmt.Errorf(
			"%w: configmap is nil",
			ErrInvalidDocument,
		)
	}
	text, hasText := configMap.Data[source.options.Key]
	binary, hasBinary := configMap.BinaryData[source.options.Key]
	if hasText && hasBinary {
		return config.Snapshot{}, fmt.Errorf(
			"%w: key exists in Data and BinaryData",
			ErrInvalidDocument,
		)
	}
	if !hasText && !hasBinary {
		return source.missingSnapshot(configMap.ResourceVersion)
	}
	content := []byte(text)
	if hasBinary {
		content = append([]byte(nil), binary...)
	}
	values, err := decode(
		content,
		source.options.Format,
		source.options.MaxBytes,
	)
	if err != nil {
		return config.Snapshot{}, err
	}
	hash := sha256.Sum256(content)
	revision := "kubernetes:" +
		normalizedRevision(configMap.ResourceVersion) +
		":" + hex.EncodeToString(hash[:])
	return config.NewSnapshot(revision, values)
}

func (source *Source) missingSnapshot(
	resourceVersion string,
) (config.Snapshot, error) {
	if !source.options.AllowMissing {
		return config.Snapshot{}, ErrNotFound
	}
	return config.NewSnapshot(
		"kubernetes:"+normalizedRevision(resourceVersion)+":missing",
		map[string]any{},
	)
}

func (source *Source) requireOpen() error {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return ErrClosed
	}
	return nil
}

func (source *Source) recordError(err error) {
	source.mu.Lock()
	source.lastError = err
	source.mu.Unlock()
}

func (source *Source) recordLoad(resourceVersion string) error {
	source.mu.Lock()
	defer source.mu.Unlock()
	if _, err := resourceversion.CompareResourceVersion(
		resourceVersion,
		resourceVersion,
	); err != nil {
		source.lastError = err
		return fmt.Errorf(
			"%w: resourceVersion: %w",
			ErrInvalidDocument,
			err,
		)
	}
	if source.latestRV != "" {
		comparison, err := resourceversion.CompareResourceVersion(
			resourceVersion,
			source.latestRV,
		)
		if err != nil {
			source.lastError = err
			return fmt.Errorf(
				"%w: resourceVersion: %w",
				ErrInvalidDocument,
				err,
			)
		}
		if comparison < 0 {
			err := fmt.Errorf(
				"%w: resourceVersion %s after %s",
				ErrInvalidDocument,
				resourceVersion,
				source.latestRV,
			)
			source.lastError = err
			return err
		}
	}
	source.generation++
	source.latestRV = resourceVersion
	source.lastError = nil
	return nil
}

func (source *Source) acceptEvent(
	resourceVersion string,
) (uint64, bool, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if _, err := resourceversion.CompareResourceVersion(
		resourceVersion,
		resourceVersion,
	); err != nil {
		return 0, false, fmt.Errorf(
			"%w: resourceVersion: %w",
			ErrInvalidDocument,
			err,
		)
	}
	if source.latestRV != "" {
		comparison, err := resourceversion.CompareResourceVersion(
			resourceVersion,
			source.latestRV,
		)
		if err != nil {
			return 0, false, fmt.Errorf(
				"%w: resourceVersion: %w",
				ErrInvalidDocument,
				err,
			)
		}
		if comparison <= 0 {
			return source.generation, false, nil
		}
	}
	source.generation++
	source.latestRV = resourceVersion
	source.lastError = nil
	return source.generation, true, nil
}

func (source *Source) rejectEvent(
	resourceVersion string,
	eventErr error,
) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if _, err := resourceversion.CompareResourceVersion(
		resourceVersion,
		resourceVersion,
	); err != nil {
		source.lastError = errors.Join(eventErr, err)
		return
	}
	if source.latestRV != "" {
		comparison, err := resourceversion.CompareResourceVersion(
			resourceVersion,
			source.latestRV,
		)
		if err != nil || comparison <= 0 {
			return
		}
	}
	source.generation++
	source.latestRV = resourceVersion
	source.lastError = eventErr
}

func (source *Source) stale(generation uint64) bool {
	source.mu.Lock()
	defer source.mu.Unlock()
	return generation < source.generation
}

func (source *Source) removeWatcher(current *watcher) {
	source.mu.Lock()
	delete(source.watchers, current)
	source.mu.Unlock()
}

type update struct {
	generation uint64
	snapshot   config.Snapshot
}

type watcher struct {
	source  *Source
	context context.Context
	cancel  context.CancelFunc
	stream  watch.Interface

	updates  chan update
	terminal chan error
	done     chan struct{}
	once     sync.Once
}

func (watcher *watcher) Next(
	ctx context.Context,
) (config.Snapshot, error) {
	for {
		select {
		case current := <-watcher.updates:
			if watcher.source.stale(current.generation) {
				continue
			}
			return current.snapshot.Clone(), nil
		case err := <-watcher.terminal:
			return config.Snapshot{}, err
		case <-watcher.done:
			return config.Snapshot{}, config.ErrWatcherClosed
		case <-ctx.Done():
			return config.Snapshot{}, context.Cause(ctx)
		}
	}
}

func (watcher *watcher) Close() error {
	watcher.once.Do(func() {
		watcher.source.removeWatcher(watcher)
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
				if configMap, ok := rejectedCandidate(event, err); ok {
					watcher.source.rejectEvent(
						configMap.ResourceVersion,
						err,
					)
					continue
				}
				watcher.fail(err)
				return
			}
		}
	}
}

func rejectedCandidate(
	event watch.Event,
	err error,
) (*corev1.ConfigMap, bool) {
	if event.Type != watch.Added &&
		event.Type != watch.Modified &&
		event.Type != watch.Deleted {
		return nil, false
	}
	configMap, ok := event.Object.(*corev1.ConfigMap)
	if !ok || configMap == nil {
		return nil, false
	}
	if errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrTooLarge) ||
		errors.Is(err, ErrInvalidDocument) {
		return configMap, true
	}
	return nil, false
}

func (watcher *watcher) apply(event watch.Event) error {
	if event.Type == watch.Error {
		err := apierrors.FromObject(event.Object)
		return fmt.Errorf("kubernetes config: watch event: %w", err)
	}
	configMap, ok := event.Object.(*corev1.ConfigMap)
	if !ok || configMap == nil {
		if event.Type == watch.Bookmark {
			return nil
		}
		return fmt.Errorf(
			"%w: watch object is %T",
			ErrInvalidDocument,
			event.Object,
		)
	}
	var snapshot config.Snapshot
	var err error
	switch event.Type {
	case watch.Added, watch.Modified:
		snapshot, err = watcher.source.snapshot(configMap)
	case watch.Deleted:
		snapshot, err = watcher.source.missingSnapshot(
			configMap.ResourceVersion,
		)
	case watch.Bookmark:
		return nil
	default:
		return fmt.Errorf(
			"%w: unsupported watch event %q",
			ErrInvalidDocument,
			event.Type,
		)
	}
	if err != nil {
		return err
	}
	generation, accepted, err := watcher.source.acceptEvent(
		configMap.ResourceVersion,
	)
	if err != nil {
		return err
	}
	if !accepted {
		return nil
	}
	watcher.publish(update{
		generation: generation,
		snapshot:   snapshot,
	})
	return nil
}

func (watcher *watcher) publish(current update) {
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
	case watcher.updates <- update{
		generation: current.generation,
		snapshot:   current.snapshot.Clone(),
	}:
	default:
	}
}

func (watcher *watcher) fail(err error) {
	watcher.source.recordError(err)
	select {
	case <-watcher.done:
		return
	default:
	}
	select {
	case watcher.terminal <- err:
	default:
	}
}

func decode(
	content []byte,
	format Format,
	maxBytes int,
) (map[string]any, error) {
	if len(content) > maxBytes {
		return nil, fmt.Errorf(
			"%w: %d bytes exceeds %d",
			ErrTooLarge,
			len(content),
			maxBytes,
		)
	}
	var values map[string]any
	switch format {
	case FormatJSON:
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.UseNumber()
		if err := decoder.Decode(&values); err != nil {
			return nil, fmt.Errorf(
				"%w: json: %w",
				ErrInvalidDocument,
				err,
			)
		}
		if err := requireEOF(decoder.Decode(new(any))); err != nil {
			return nil, fmt.Errorf(
				"%w: json: %w",
				ErrInvalidDocument,
				err,
			)
		}
	case FormatYAML:
		decoder := yaml.NewDecoder(bytes.NewReader(content))
		if err := decoder.Decode(&values); err != nil {
			return nil, fmt.Errorf(
				"%w: yaml: %w",
				ErrInvalidDocument,
				err,
			)
		}
		var extra any
		if err := requireEOF(decoder.Decode(&extra)); err != nil {
			return nil, fmt.Errorf(
				"%w: yaml: %w",
				ErrInvalidDocument,
				err,
			)
		}
	default:
		return nil, fmt.Errorf(
			"%w: format %q",
			ErrInvalidOption,
			format,
		)
	}
	if values == nil {
		return nil, fmt.Errorf(
			"%w: root must be an object",
			ErrInvalidDocument,
		)
	}
	return values, nil
}

func requireEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple documents are not allowed")
	}
	return err
}

func normalizedRevision(revision string) string {
	revision = strings.TrimSpace(revision)
	if revision == "" {
		return "0"
	}
	return revision
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

var _ config.Source = (*Source)(nil)
