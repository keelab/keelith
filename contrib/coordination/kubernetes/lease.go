// Package kubernetes implements renewable Keelith ownership with precreated
// coordination.k8s.io/v1 lease objects.
package kubernetes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/coordination"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
)

const (
	managedLabel    = "coordination.keelith.dev/managed"
	fenceAnnotation = "coordination.keelith.dev/fence"

	minimumttl        = 3 * time.Second
	maximumttl        = 24 * time.Hour
	maximumMappings   = 128
	maximumCASRetries = 5
)

var (
	// ErrInvalidOption reports malformed identity, topology, key, or ttl.
	ErrInvalidOption = errors.New("kubernetes coordination: invalid option")
	// ErrClosed reports acquisition after Coordinator shutdown.
	ErrClosed = errors.New("kubernetes coordination: closed")
	// ErrLeaseNotConfigured reports an unmapped logical ownership key.
	ErrLeaseNotConfigured = errors.New(
		"kubernetes coordination: lease is not configured",
	)
	// ErrInvalidLease reports a Lease that does not carry Keelith fencing state.
	ErrInvalidLease = errors.New(
		"kubernetes coordination: invalid managed Lease",
	)
)

// LeaseClient is the read/write subset of the official generated client.
//
// Keelith never creates or deletes lease objects. Precreation makes it
// possible to grant get/update on explicit resourceNames without namespace-wide
// create/delete authority.
type LeaseClient interface {
	Get(
		context.Context,
		string,
		metav1.GetOptions,
	) (*coordinationv1.Lease, error)
	Update(
		context.Context,
		*coordinationv1.Lease,
		metav1.UpdateOptions,
	) (*coordinationv1.Lease, error)
}

// Options configure one namespace-scoped Coordinator.
type Options struct {
	Namespace string            `config:"namespace"`
	Identity  string            `config:"identity"`
	Leases    map[string]string `config:"leases"`
}

// Description is a key-, identity-, and error-free aggregate snapshot.
type Description struct {
	Active          int
	Mappings        int
	Acquired        uint64
	Contended       uint64
	Lost            uint64
	Released        uint64
	BackendFailures uint64
	Closed          bool
}

// Coordinator maintains token-checked Kubernetes Leases.
type Coordinator struct {
	client  LeaseClient
	options Options
	keys    []string

	mu              sync.Mutex
	active          map[*lease]struct{}
	acquired        uint64
	contended       uint64
	lost            uint64
	released        uint64
	backendFailures uint64
	closed          bool
}

// New constructs a Coordinator around one namespace-scoped lease client.
func New(client LeaseClient, options Options) (*Coordinator, error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: lease client is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(normalized.Leases))
	for key := range normalized.Leases {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return &Coordinator{
		client:  client,
		options: normalized,
		keys:    keys,
		active:  make(map[*lease]struct{}),
	}, nil
}

// Open constructs a Coordinator from an explicit Kubernetes rest config.
func Open(
	restConfig *rest.Config,
	options Options,
) (*Coordinator, error) {
	if restConfig == nil {
		return nil, fmt.Errorf("%w: rest config is nil", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	configCopy := rest.CopyConfig(restConfig)
	rest.AddUserAgent(configCopy, "keelith-kubernetes-coordination")
	client, err := coordinationclient.NewForConfig(configCopy)
	if err != nil {
		return nil, fmt.Errorf(
			"kubernetes coordination: create lease client: %w",
			err,
		)
	}
	return New(client.Leases(normalized.Namespace), normalized)
}

// OpenInCluster constructs a Coordinator from the pod service account.
func OpenInCluster(options Options) (*Coordinator, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf(
			"kubernetes coordination: in-cluster config: %w",
			err,
		)
	}
	return Open(config, options)
}

// NormalizeOptions snapshots and validates explicit key-to-Lease mappings.
func NormalizeOptions(input Options) (Options, error) {
	options := input
	options.Namespace = strings.TrimSpace(options.Namespace)
	options.Identity = strings.TrimSpace(options.Identity)
	if len(validation.IsDNS1123Label(options.Namespace)) != 0 ||
		!validIdentity(options.Identity) ||
		len(options.Leases) == 0 ||
		len(options.Leases) > maximumMappings {
		return Options{}, fmt.Errorf(
			"%w: namespace, identity, or mapping count",
			ErrInvalidOption,
		)
	}
	leases := make(map[string]string, len(options.Leases))
	names := make(map[string]string, len(options.Leases))
	for key, name := range options.Leases {
		name = strings.TrimSpace(name)
		if !validKey(key) ||
			len(validation.IsDNS1123Subdomain(name)) != 0 {
			return Options{}, fmt.Errorf(
				"%w: logical key or Lease name",
				ErrInvalidOption,
			)
		}
		if previous, duplicate := names[name]; duplicate {
			return Options{}, fmt.Errorf(
				"%w: Lease %q maps both %q and %q",
				ErrInvalidOption,
				name,
				previous,
				key,
			)
		}
		leases[key] = name
		names[name] = key
	}
	options.Leases = leases
	return options, nil
}

// ValidateOptions validates construction settings without creating a client.
func ValidateOptions(options Options) error {
	_, err := NormalizeOptions(options)
	return err
}

// Start verifies every precreated Lease before a workload can become ready.
func (coordinator *Coordinator) Start(ctx context.Context) error {
	if coordinator == nil || ctx == nil {
		return fmt.Errorf(
			"%w: coordinator or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	coordinator.mu.Lock()
	closed := coordinator.closed
	coordinator.mu.Unlock()
	if closed {
		return ErrClosed
	}
	for _, key := range coordinator.keys {
		name := coordinator.options.Leases[key]
		current, err := coordinator.client.Get(
			ctx,
			name,
			metav1.GetOptions{},
		)
		if err != nil {
			coordinator.recordBackendFailure()
			return fmt.Errorf(
				"kubernetes coordination: get precreated Lease: %w",
				err,
			)
		}
		if _, err := managedFence(current, name); err != nil {
			return err
		}
	}
	return nil
}

// TryAcquire uses resourceVersion CAS and starts automatic renewal.
func (coordinator *Coordinator) TryAcquire(
	ctx context.Context,
	key string,
	ttl time.Duration,
) (coordination.Lease, bool, error) {
	if coordinator == nil || ctx == nil || !validKey(key) {
		return nil, false, fmt.Errorf(
			"%w: coordinator, context, or key",
			coordination.ErrInvalidOption,
		)
	}
	effectivettl, seconds, err := normalizedttl(ttl)
	if err != nil {
		return nil, false, err
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, false, cause
	}
	name, exists := coordinator.options.Leases[key]
	if !exists {
		return nil, false, fmt.Errorf(
			"%w: logical key is unmapped",
			ErrLeaseNotConfigured,
		)
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return nil, false, ErrClosed
	}
	coordinator.mu.Unlock()
	token, err := ownerToken(coordinator.options.Identity)
	if err != nil {
		return nil, false, err
	}
	fence, acquired, err := coordinator.acquire(
		ctx,
		name,
		token,
		seconds,
	)
	if err != nil {
		return nil, false, err
	}
	if !acquired {
		coordinator.mu.Lock()
		coordinator.contended++
		coordinator.mu.Unlock()
		return nil, false, nil
	}
	result := &lease{
		coordinator: coordinator,
		name:        name,
		token:       token,
		fence:       fence,
		TTL:         effectivettl,
		seconds:     seconds,
		done:        make(chan struct{}),
		stop:        make(chan struct{}),
		loopDone:    make(chan struct{}),
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		releaseContext, cancel := context.WithTimeout(
			context.Background(),
			min(effectivettl, 5*time.Second),
		)
		releaseErr := coordinator.releaseBackend(
			releaseContext,
			name,
			token,
		)
		cancel()
		return nil, false, errors.Join(ErrClosed, releaseErr)
	}
	coordinator.active[result] = struct{}{}
	coordinator.acquired++
	coordinator.mu.Unlock()
	go result.renew()
	return result, true, nil
}

// Shutdown releases active Leases and rejects future acquisition.
func (coordinator *Coordinator) Shutdown(ctx context.Context) error {
	if coordinator == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	coordinator.mu.Lock()
	coordinator.closed = true
	active := make([]*lease, 0, len(coordinator.active))
	for current := range coordinator.active {
		active = append(active, current)
	}
	coordinator.mu.Unlock()
	var result error
	for index, current := range active {
		result = errors.Join(result, current.Release(ctx))
		if context.Cause(ctx) != nil {
			for _, remaining := range active[index+1:] {
				remaining.abandon(context.Cause(ctx))
			}
			break
		}
	}
	return result
}

// Description returns aggregate state without keys, Lease names, identities,
// tokens, resourceVersions, or errors.
func (coordinator *Coordinator) Description() Description {
	if coordinator == nil {
		return Description{}
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return Description{
		Active:          len(coordinator.active),
		Mappings:        len(coordinator.options.Leases),
		Acquired:        coordinator.acquired,
		Contended:       coordinator.contended,
		Lost:            coordinator.lost,
		Released:        coordinator.released,
		BackendFailures: coordinator.backendFailures,
		Closed:          coordinator.closed,
	}
}

func (coordinator *Coordinator) acquire(
	ctx context.Context,
	name string,
	token string,
	seconds int32,
) (uint64, bool, error) {
	for attempt := 0; attempt < maximumCASRetries; attempt++ {
		current, err := coordinator.client.Get(
			ctx,
			name,
			metav1.GetOptions{},
		)
		if err != nil {
			coordinator.recordBackendFailure()
			return 0, false, fmt.Errorf(
				"kubernetes coordination: get Lease: %w",
				err,
			)
		}
		fence, err := managedFence(current, name)
		if err != nil {
			return 0, false, err
		}
		now := time.Now()
		if !available(current.Spec, now) {
			return 0, false, nil
		}
		if fence == math.MaxUint64 {
			return 0, false, fmt.Errorf(
				"%w: fence exhausted",
				ErrInvalidLease,
			)
		}
		next := current.DeepCopy()
		microtime := metav1.NewMicroTime(now)
		next.Spec.HolderIdentity = stringPointer(token)
		next.Spec.LeaseDurationSeconds = int32Pointer(seconds)
		next.Spec.AcquireTime = &microtime
		next.Spec.RenewTime = &microtime
		transitions := int32(1)
		if next.Spec.LeaseTransitions != nil {
			if *next.Spec.LeaseTransitions == math.MaxInt32 {
				return 0, false, fmt.Errorf(
					"%w: transitions exhausted",
					ErrInvalidLease,
				)
			}
			transitions = *next.Spec.LeaseTransitions + 1
		}
		next.Spec.LeaseTransitions = int32Pointer(transitions)
		next.Annotations[fenceAnnotation] = strconv.FormatUint(
			fence+1,
			10,
		)
		if _, err := coordinator.client.Update(
			ctx,
			next,
			metav1.UpdateOptions{},
		); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			coordinator.recordBackendFailure()
			return 0, false, fmt.Errorf(
				"kubernetes coordination: acquire Lease: %w",
				err,
			)
		}
		return fence + 1, true, nil
	}
	coordinator.recordBackendFailure()
	return 0, false, fmt.Errorf(
		"kubernetes coordination: acquire Lease: too many CAS conflicts",
	)
}

func (coordinator *Coordinator) renewBackend(
	ctx context.Context,
	name string,
	token string,
	seconds int32,
) (bool, error) {
	for attempt := 0; attempt < maximumCASRetries; attempt++ {
		current, err := coordinator.client.Get(
			ctx,
			name,
			metav1.GetOptions{},
		)
		if err != nil {
			coordinator.recordBackendFailure()
			return false, err
		}
		if _, err := managedFence(current, name); err != nil {
			return false, err
		}
		if holder(current.Spec) != token {
			return false, nil
		}
		next := current.DeepCopy()
		now := metav1.NewMicroTime(time.Now())
		next.Spec.RenewTime = &now
		next.Spec.LeaseDurationSeconds = int32Pointer(seconds)
		if _, err := coordinator.client.Update(
			ctx,
			next,
			metav1.UpdateOptions{},
		); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			coordinator.recordBackendFailure()
			return false, err
		}
		return true, nil
	}
	coordinator.recordBackendFailure()
	return false, errors.New(
		"kubernetes coordination: renew Lease: too many CAS conflicts",
	)
}

func (coordinator *Coordinator) releaseBackend(
	ctx context.Context,
	name string,
	token string,
) error {
	for attempt := 0; attempt < maximumCASRetries; attempt++ {
		current, err := coordinator.client.Get(
			ctx,
			name,
			metav1.GetOptions{},
		)
		if err != nil {
			coordinator.recordBackendFailure()
			return fmt.Errorf(
				"kubernetes coordination: get Lease for release: %w",
				err,
			)
		}
		if _, err := managedFence(current, name); err != nil {
			return err
		}
		if holder(current.Spec) != token {
			return coordination.ErrLeaseLost
		}
		next := current.DeepCopy()
		next.Spec.HolderIdentity = stringPointer("")
		next.Spec.AcquireTime = nil
		next.Spec.RenewTime = nil
		if _, err := coordinator.client.Update(
			ctx,
			next,
			metav1.UpdateOptions{},
		); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			coordinator.recordBackendFailure()
			return fmt.Errorf(
				"kubernetes coordination: release Lease: %w",
				err,
			)
		}
		return nil
	}
	coordinator.recordBackendFailure()
	return errors.New(
		"kubernetes coordination: release Lease: too many CAS conflicts",
	)
}

func (coordinator *Coordinator) finish(
	current *lease,
	lost bool,
	released bool,
) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if _, exists := coordinator.active[current]; !exists {
		return
	}
	delete(coordinator.active, current)
	if lost {
		coordinator.lost++
	}
	if released {
		coordinator.released++
	}
}

func (coordinator *Coordinator) recordBackendFailure() {
	coordinator.mu.Lock()
	coordinator.backendFailures++
	coordinator.mu.Unlock()
}

type lease struct {
	coordinator *Coordinator
	name        string
	token       string
	fence       uint64
	TTL         time.Duration
	seconds     int32
	done        chan struct{}
	stop        chan struct{}
	loopDone    chan struct{}

	mu          sync.Mutex
	err         error
	releaseErr  error
	finishOnce  sync.Once
	stopOnce    sync.Once
	releaseOnce sync.Once
}

func (lease *lease) Fence() uint64 {
	if lease == nil {
		return 0
	}
	return lease.fence
}

func (lease *lease) Done() <-chan struct{} {
	if lease == nil {
		return closedSignal()
	}
	return lease.done
}

func (lease *lease) Err() error {
	if lease == nil {
		return coordination.ErrLeaseLost
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.err
}

func (lease *lease) Release(ctx context.Context) error {
	if lease == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	lease.releaseOnce.Do(func() {
		lease.stopOnce.Do(func() { close(lease.stop) })
		select {
		case <-lease.loopDone:
		case <-ctx.Done():
			lease.finish(context.Cause(ctx), false, false)
			return
		}
		releaseErr := lease.coordinator.releaseBackend(
			ctx,
			lease.name,
			lease.token,
		)
		if errors.Is(releaseErr, coordination.ErrLeaseLost) {
			lease.finish(coordination.ErrLeaseLost, true, false)
			return
		}
		lease.finish(releaseErr, false, releaseErr == nil)
	})
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.releaseErr
}

func (lease *lease) renew() {
	defer close(lease.loopDone)
	interval := lease.TTL / 3
	retryDelay := min(lease.TTL/10, time.Second)
	if retryDelay < 100*time.Millisecond {
		retryDelay = 100 * time.Millisecond
	}
	lastSuccess := time.Now()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-lease.stop:
			return
		case <-timer.C:
		}
		renewContext, cancel := context.WithTimeout(
			context.Background(),
			min(interval, 5*time.Second),
		)
		owned, err := lease.coordinator.renewBackend(
			renewContext,
			lease.name,
			lease.token,
			lease.seconds,
		)
		cancel()
		if err == nil && owned {
			lastSuccess = time.Now()
			timer.Reset(interval)
			continue
		}
		if err == nil && !owned {
			lease.finish(coordination.ErrLeaseLost, true, false)
			return
		}
		if time.Now().Add(retryDelay).After(
			lastSuccess.Add(lease.TTL - interval),
		) {
			lease.finish(
				errors.Join(coordination.ErrLeaseLost, err),
				true,
				false,
			)
			return
		}
		timer.Reset(retryDelay)
	}
}

func (lease *lease) abandon(cause error) {
	if lease == nil {
		return
	}
	lease.releaseOnce.Do(func() {
		lease.stopOnce.Do(func() { close(lease.stop) })
		lease.finish(cause, false, false)
	})
}

func (lease *lease) finish(
	err error,
	lost bool,
	released bool,
) {
	lease.finishOnce.Do(func() {
		lease.mu.Lock()
		lease.err = err
		if !released {
			lease.releaseErr = err
		}
		lease.mu.Unlock()
		close(lease.done)
		lease.coordinator.finish(lease, lost, released)
	})
}

func managedFence(
	current *coordinationv1.Lease,
	name string,
) (uint64, error) {
	if current == nil ||
		current.Name != name ||
		current.Labels[managedLabel] != "true" ||
		current.Annotations == nil {
		return 0, fmt.Errorf(
			"%w: identity or managed marker",
			ErrInvalidLease,
		)
	}
	text, exists := current.Annotations[fenceAnnotation]
	if !exists {
		return 0, fmt.Errorf(
			"%w: fence annotation is absent",
			ErrInvalidLease,
		)
	}
	fence, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: fence annotation is malformed",
			ErrInvalidLease,
		)
	}
	return fence, nil
}

func available(spec coordinationv1.LeaseSpec, now time.Time) bool {
	if holder(spec) == "" {
		return true
	}
	if spec.RenewTime == nil ||
		spec.LeaseDurationSeconds == nil ||
		*spec.LeaseDurationSeconds <= 0 {
		return false
	}
	expires := spec.RenewTime.Add(
		time.Duration(*spec.LeaseDurationSeconds) * time.Second,
	)
	return !now.Before(expires)
}

func holder(spec coordinationv1.LeaseSpec) string {
	if spec.HolderIdentity == nil {
		return ""
	}
	return *spec.HolderIdentity
}

func normalizedttl(ttl time.Duration) (time.Duration, int32, error) {
	if ttl < minimumttl || ttl > maximumttl {
		return 0, 0, fmt.Errorf(
			"%w: ttl must be within %s..%s",
			coordination.ErrInvalidOption,
			minimumttl,
			maximumttl,
		)
	}
	seconds64 := int64(math.Ceil(ttl.Seconds()))
	if seconds64 < 1 || seconds64 > math.MaxInt32 {
		return 0, 0, fmt.Errorf(
			"%w: ttl seconds overflow",
			coordination.ErrInvalidOption,
		)
	}
	seconds := int32(seconds64)
	return time.Duration(seconds) * time.Second, seconds, nil
}

func ownerToken(identity string) (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf(
			"kubernetes coordination: generate owner token: %w",
			err,
		)
	}
	return identity + "/" + hex.EncodeToString(random), nil
}

func validIdentity(value string) bool {
	if value == "" ||
		len(value) > 128 ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validKey(value string) bool {
	if value == "" ||
		len(value) > 512 ||
		strings.TrimSpace(value) != value ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func stringPointer(value string) *string {
	return &value
}

func int32Pointer(value int32) *int32 {
	return &value
}

func closedSignal() <-chan struct{} {
	result := make(chan struct{})
	close(result)
	return result
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

var _ coordination.Coordinator = (*Coordinator)(nil)
