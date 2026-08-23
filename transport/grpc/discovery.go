package grpc

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	kclient "github.com/keelab/keelith/client"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/registry"
	"github.com/keelab/keelith/selector"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultDiscoveryMaxConnections = 64
	defaultDiscoveryIdleTimeout    = 5 * time.Minute
)

var (
	// ErrDiscoveryNotRunning reports an invocation outside pool lifecycle.
	ErrDiscoveryNotRunning = errors.New("grpc transport: discovery connection not running")
	// ErrPoolExhausted reports a full pool with no inactive connection to evict.
	ErrPoolExhausted = errors.New("grpc transport: discovery connection pool exhausted")
	// ErrInvalidEndpoint reports a selected non-gRPC authority endpoint.
	ErrInvalidEndpoint = errors.New("grpc transport: invalid selected endpoint")
	// ErrNodeRetired reports a node removed after selection but before leasing.
	ErrNodeRetired = errors.New("grpc transport: selected node was retired")
)

// DiscoveryState is the observable dynamic connection lifecycle.
type DiscoveryState string

const (
	// DiscoveryStateNew means Start has not been called.
	DiscoveryStateNew DiscoveryState = "new"
	// DiscoveryStateRunning means new call and stream leases are accepted.
	DiscoveryStateRunning DiscoveryState = "running"
	// DiscoveryStateStopping means new leases are rejected while active work drains.
	DiscoveryStateStopping DiscoveryState = "stopping"
	// DiscoveryStateStopped means every ClientConn has been closed.
	DiscoveryStateStopped DiscoveryState = "stopped"
)

// DialFunc creates one grpc-go ClientConn for a selected immutable Node.
type DialFunc func(context.Context, selector.Node) (*ggrpc.ClientConn, error)

// DiscoveryConnectionConfig configures an instance-scoped dynamic pool.
type DiscoveryConnectionConfig struct {
	Name           string
	Dependency     string
	Picker         kclient.Picker
	NodeChanges    kclient.NodeChangeSource
	Dial           DialFunc
	MaxConnections int
	IdleTimeout    time.Duration
}

// DiscoveryDescription is a bounded dynamic connection diagnostic snapshot.
type DiscoveryDescription struct {
	Name             string
	State            DiscoveryState
	Connections      int
	Dialing          int
	Active           int
	MaxConnections   int
	DialAttempts     uint64
	DialFailures     uint64
	Evictions        uint64
	Retired          int
	Reconciliations  uint64
	TopologyRevision string
	LastError        string
}

type connectionEntry struct {
	key      string
	node     selector.Node
	conn     *ggrpc.ClientConn
	active   int
	lastUsed time.Time
	retired  bool
}

type pendingDial struct {
	done   chan struct{}
	cancel context.CancelCauseFunc
}

// DiscoveryConnection implements grpc.ClientConnInterface and app.Component.
//
// It lazily dials selected nodes, bounds cached connections, and holds stream
// leases until their terminal event.
type DiscoveryConnection struct {
	name           string
	dependency     string
	picker         kclient.Picker
	nodeChanges    kclient.NodeChangeSource
	dial           DialFunc
	maxConnections int
	idleTimeout    time.Duration

	mu               sync.Mutex
	state            DiscoveryState
	starting         bool
	entries          map[string]*connectionEntry
	dialing          map[string]*pendingDial
	desired          map[string]struct{}
	retired          map[string]struct{}
	topologyManaged  bool
	topologyVersion  uint64
	topologyRevision string
	active           int
	finalizing       bool
	stopErr          error
	lastError        string

	dialAttempts    uint64
	dialFailures    uint64
	evictions       uint64
	reconciliations uint64

	changeWatcher kclient.NodeChangeWatcher
	changeCancel  context.CancelFunc
	changeDone    chan struct{}
	done          chan struct{}
	doneOnce      sync.Once
}

// NewDiscoveryConnection validates a dynamic pool without starting it.
func NewDiscoveryConnection(
	config DiscoveryConnectionConfig,
) (*DiscoveryConnection, error) {
	name := strings.TrimSpace(config.Name)
	if !validDiscoveryName(name) {
		return nil, fmt.Errorf("%w: name is malformed", ErrInvalidOption)
	}
	dependency := strings.TrimSpace(config.Dependency)
	if dependency != "" && !validDiscoveryName(dependency) {
		return nil, fmt.Errorf("%w: dependency is malformed", ErrInvalidOption)
	}
	if isNilPicker(config.Picker) {
		return nil, fmt.Errorf("%w: picker is nil", ErrInvalidOption)
	}
	if config.Dial == nil {
		return nil, fmt.Errorf("%w: dial function is nil", ErrInvalidOption)
	}
	nodeChanges := config.NodeChanges
	if isNilNodeChangeSource(nodeChanges) {
		nodeChanges = nil
		if source, ok := config.Picker.(kclient.NodeChangeSource); ok &&
			!isNilNodeChangeSource(source) {
			nodeChanges = source
		}
	}
	maxConnections := config.MaxConnections
	if maxConnections == 0 {
		maxConnections = defaultDiscoveryMaxConnections
	}
	idleTimeout := config.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultDiscoveryIdleTimeout
	}
	if maxConnections <= 0 || idleTimeout <= 0 {
		return nil, fmt.Errorf(
			"%w: connection limit and idle timeout must be positive",
			ErrInvalidOption,
		)
	}
	return &DiscoveryConnection{
		name:           name,
		dependency:     dependency,
		picker:         config.Picker,
		nodeChanges:    nodeChanges,
		dial:           config.Dial,
		maxConnections: maxConnections,
		idleTimeout:    idleTimeout,
		state:          DiscoveryStateNew,
		entries:        make(map[string]*connectionEntry),
		dialing:        make(map[string]*pendingDial),
		desired:        make(map[string]struct{}),
		retired:        make(map[string]struct{}),
		done:           make(chan struct{}),
	}, nil
}

// Name returns the stable App component name.
func (dc *DiscoveryConnection) Name() string {
	if dc == nil {
		return ""
	}
	return dc.name
}

// Dependencies returns the optional Router component dependency.
func (dc *DiscoveryConnection) Dependencies() []string {
	if dc == nil || dc.dependency == "" {
		return nil
	}
	return []string{dc.dependency}
}

// Start enables dynamic call leases.
func (dc *DiscoveryConnection) Start(ctx context.Context) error {
	if dc == nil {
		return fmt.Errorf("%w: discovery connection is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	dc.mu.Lock()
	if dc.state != DiscoveryStateNew || dc.starting {
		dc.mu.Unlock()
		return ErrAlreadyStarted
	}
	if dc.nodeChanges == nil {
		dc.state = DiscoveryStateRunning
		dc.mu.Unlock()
		return nil
	}
	dc.starting = true
	runtimeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	dc.changeCancel = cancel
	dc.mu.Unlock()

	watcher, err := dc.nodeChanges.WatchNodeChanges(runtimeCtx)
	if err != nil {
		cancel()
		dc.failChangeStart(err)
		return fmt.Errorf("grpc transport: watch node changes: %w", err)
	}
	if isNilNodeChangeWatcher(watcher) {
		cancel()
		err = errors.New("node change source returned a nil watcher")
		dc.failChangeStart(err)
		return fmt.Errorf("grpc transport: %w", err)
	}
	initial, err := watcher.Next(ctx)
	if err != nil {
		cancel()
		_ = watcher.Close()
		dc.failChangeStart(err)
		return fmt.Errorf("grpc transport: initial node change: %w", err)
	}

	dc.mu.Lock()
	if dc.state != DiscoveryStateNew {
		dc.starting = false
		dc.changeCancel = nil
		dc.mu.Unlock()
		cancel()
		_ = watcher.Close()
		return ErrDiscoveryNotRunning
	}
	dc.starting = false
	dc.state = DiscoveryStateRunning
	dc.topologyManaged = true
	dc.changeWatcher = watcher
	dc.changeDone = make(chan struct{})
	dc.applyNodeChangeLocked(initial)
	changeDone := dc.changeDone
	dc.mu.Unlock()

	go dc.watchNodeChanges(runtimeCtx, watcher, changeDone)
	return nil
}

// Stop rejects new leases, drains active calls/streams, and closes connections.
func (dc *DiscoveryConnection) Stop(ctx context.Context) error {
	if dc == nil {
		return nil
	}
	if ctx == nil {
		return ErrNilContext
	}

	dc.mu.Lock()
	switch dc.state {
	case DiscoveryStateNew:
		dc.state = DiscoveryStateStopping
	case DiscoveryStateRunning:
		dc.state = DiscoveryStateStopping
	case DiscoveryStateStopping:
	case DiscoveryStateStopped:
		err := dc.stopErr
		dc.mu.Unlock()
		return err
	}
	changeCancel := dc.changeCancel
	changeWatcher := dc.changeWatcher
	changeDone := dc.changeDone
	dc.cancelPendingDialsLocked(ErrDiscoveryNotRunning)
	toClose, finalize := dc.beginFinalizeLocked()
	done := dc.done
	dc.mu.Unlock()
	if changeCancel != nil {
		changeCancel()
	}
	if !isNilNodeChangeWatcher(changeWatcher) {
		_ = changeWatcher.Close()
	}
	if finalize {
		dc.finalize(toClose)
	}
	if err := waitDiscoveryChannels(ctx, done, changeDone); err != nil {
		return err
	}
	dc.mu.Lock()
	err := dc.stopErr
	dc.mu.Unlock()
	return err
}

// Invoke selects one node and performs a unary call on its pooled ClientConn.
func (dc *DiscoveryConnection) Invoke(
	ctx context.Context,
	method string,
	request any,
	reply any,
	options ...ggrpc.CallOption,
) (resultErr error) {
	if ctx == nil {
		return ErrNilContext
	}
	if err := dc.accepting(); err != nil {
		return err
	}
	target, err := operationFromMethod(method, operation.KindUnary)
	if err != nil {
		return err
	}
	started := time.Now()
	topologyVersion := dc.currentTopologyVersion()
	node, done, err := dc.picker.Pick(ctx, target)
	if err != nil {
		return dependencyFailure(err)
	}
	defer completeSelection(ctx, done, started, &resultErr)

	routedContext, err := selectedGRPCContext(ctx, target, node)
	if err != nil {
		return failure.MarkTransport(err)
	}
	entry, release, err := dc.lease(
		routedContext,
		node,
		topologyVersion,
	)
	if err != nil {
		return err
	}
	defer release()
	return entry.conn.Invoke(
		routedContext,
		method,
		request,
		reply,
		options...,
	)
}

// NewStream selects one node and holds its lease until stream termination.
func (dc *DiscoveryConnection) NewStream(
	ctx context.Context,
	description *ggrpc.StreamDesc,
	method string,
	options ...ggrpc.CallOption,
) (result ggrpc.ClientStream, resultErr error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if description == nil {
		return nil, fmt.Errorf("%w: stream description is nil", ErrInvalidOption)
	}
	if err := dc.accepting(); err != nil {
		return nil, err
	}
	target, err := operationFromMethod(
		method,
		streamOperationKind(description.ClientStreams, description.ServerStreams),
	)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	topologyVersion := dc.currentTopologyVersion()
	node, done, err := dc.picker.Pick(ctx, target)
	if err != nil {
		return nil, dependencyFailure(err)
	}
	handedOff := false
	var release func()
	defer func() {
		recovered := recover()
		if !handedOff {
			if release != nil {
				release()
			}
			feedbackErr := resultErr
			if recovered != nil {
				feedbackErr = errors.New("grpc transport: stream creation panic")
			}
			done(selectionResult(ctx, started, feedbackErr))
		}
		if recovered != nil {
			panic(recovered)
		}
	}()

	routedContext, err := selectedGRPCContext(ctx, target, node)
	if err != nil {
		return nil, failure.MarkTransport(err)
	}
	entry, releaseLease, err := dc.lease(
		routedContext,
		node,
		topologyVersion,
	)
	if err != nil {
		return nil, err
	}
	release = releaseLease
	stream, err := entry.conn.NewStream(
		routedContext,
		description,
		method,
		options...,
	)
	if err != nil {
		return nil, err
	}
	result = newDiscoveryClientStream(
		routedContext,
		stream,
		done,
		release,
		started,
		description.ClientStreams && !description.ServerStreams,
	)
	handedOff = true
	return result, nil
}

// Describe returns bounded connection, dial, and lifecycle diagnostics.
func (dc *DiscoveryConnection) Describe() DiscoveryDescription {
	if dc == nil {
		return DiscoveryDescription{
			State:     DiscoveryStateStopped,
			LastError: "discovery connection is nil",
		}
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	retired := 0
	for _, entry := range dc.entries {
		if entry.retired {
			retired++
		}
	}
	return DiscoveryDescription{
		Name:             dc.name,
		State:            dc.state,
		Connections:      len(dc.entries),
		Dialing:          len(dc.dialing),
		Active:           dc.active,
		MaxConnections:   dc.maxConnections,
		DialAttempts:     dc.dialAttempts,
		DialFailures:     dc.dialFailures,
		Evictions:        dc.evictions,
		Retired:          retired,
		Reconciliations:  dc.reconciliations,
		TopologyRevision: dc.topologyRevision,
		LastError:        dc.lastError,
	}
}

func (dc *DiscoveryConnection) accepting() error {
	if dc == nil {
		return ErrDiscoveryNotRunning
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.state != DiscoveryStateRunning {
		return ErrDiscoveryNotRunning
	}
	return nil
}

func (dc *DiscoveryConnection) lease(
	ctx context.Context,
	node selector.Node,
	selectionVersion uint64,
) (*connectionEntry, func(), error) {
	if _, err := parseGRPCEndpoint(node); err != nil {
		return nil, nil, err
	}
	key := discoveryNodeKey(node.ID(), node.Endpoint(), node.Metadata())
	for {
		dc.mu.Lock()
		if dc.state != DiscoveryStateRunning {
			dc.mu.Unlock()
			return nil, nil, ErrDiscoveryNotRunning
		}
		if dc.nodeRetiredLocked(key, selectionVersion) {
			dc.mu.Unlock()
			return nil, nil, retiredNodeError(node)
		}
		if entry := dc.entries[key]; entry != nil {
			entry.active++
			dc.active++
			dc.mu.Unlock()
			return entry, dc.releaseFunc(entry), nil
		}
		if pending := dc.dialing[key]; pending != nil {
			done := pending.done
			dc.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, nil, context.Cause(ctx)
			}
		}

		evicted := dc.evictLocked(key, time.Now())
		if len(dc.entries)+len(dc.dialing) >=
			dc.maxConnections {
			dc.mu.Unlock()
			dc.closeEvicted(evicted)
			return nil, nil, ErrPoolExhausted
		}
		dialContext, cancelDial := context.WithCancelCause(ctx)
		pending := &pendingDial{
			done:   make(chan struct{}),
			cancel: cancelDial,
		}
		dc.dialing[key] = pending
		dc.dialAttempts++
		dc.mu.Unlock()
		dc.closeEvicted(evicted)

		clientConnection, dialErr := dc.dial(dialContext, node)
		dialCause := context.Cause(dialContext)
		cancelDial(context.Canceled)
		if dialErr == nil && isNilClientConnection(clientConnection) {
			dialErr = errors.New("dial function returned a nil ClientConn")
		}

		dc.mu.Lock()
		delete(dc.dialing, key)
		close(pending.done)
		if errors.Is(dialCause, ErrNodeRetired) {
			toClose, finalize := dc.beginFinalizeLocked()
			dc.mu.Unlock()
			if !isNilClientConnection(clientConnection) {
				_ = clientConnection.Close()
			}
			if finalize {
				dc.finalize(toClose)
			}
			return nil, nil, retiredNodeError(node)
		}
		if errors.Is(dialCause, ErrDiscoveryNotRunning) {
			toClose, finalize := dc.beginFinalizeLocked()
			dc.mu.Unlock()
			if !isNilClientConnection(clientConnection) {
				_ = clientConnection.Close()
			}
			if finalize {
				dc.finalize(toClose)
			}
			return nil, nil, ErrDiscoveryNotRunning
		}
		if dialCause != nil {
			toClose, finalize := dc.beginFinalizeLocked()
			dc.mu.Unlock()
			if !isNilClientConnection(clientConnection) {
				_ = clientConnection.Close()
			}
			if finalize {
				dc.finalize(toClose)
			}
			return nil, nil, dialCause
		}
		if dialErr != nil {
			dc.dialFailures++
			dc.lastError = dialErr.Error()
			toClose, finalize := dc.beginFinalizeLocked()
			dc.mu.Unlock()
			if !isNilClientConnection(clientConnection) {
				_ = clientConnection.Close()
			}
			if finalize {
				dc.finalize(toClose)
			}
			return nil, nil, failure.MarkTransport(fmt.Errorf(
				"grpc transport: dial node %q: %w",
				node.ID(),
				dialErr,
			))
		}
		if dc.state != DiscoveryStateRunning {
			toClose, finalize := dc.beginFinalizeLocked()
			dc.mu.Unlock()
			_ = clientConnection.Close()
			if finalize {
				dc.finalize(toClose)
			}
			return nil, nil, ErrDiscoveryNotRunning
		}
		if dc.nodeRetiredLocked(key, selectionVersion) {
			toClose, finalize := dc.beginFinalizeLocked()
			dc.mu.Unlock()
			_ = clientConnection.Close()
			if finalize {
				dc.finalize(toClose)
			}
			return nil, nil, retiredNodeError(node)
		}
		entry := &connectionEntry{
			key:      key,
			node:     node,
			conn:     clientConnection,
			active:   1,
			lastUsed: time.Now(),
		}
		dc.entries[key] = entry
		dc.active++
		dc.lastError = ""
		dc.mu.Unlock()
		return entry, dc.releaseFunc(entry), nil
	}
}

func (dc *DiscoveryConnection) releaseFunc(
	entry *connectionEntry,
) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			dc.mu.Lock()
			if entry.active > 0 {
				entry.active--
				dc.active--
			}
			entry.lastUsed = time.Now()
			var retired []*ggrpc.ClientConn
			if entry.active == 0 && entry.retired {
				if dc.entries[entry.key] == entry {
					delete(dc.entries, entry.key)
					retired = append(retired, entry.conn)
					dc.evictions++
				}
			}
			toClose, finalize := dc.beginFinalizeLocked()
			dc.mu.Unlock()
			dc.closeEvicted(retired)
			if finalize {
				dc.finalize(toClose)
			}
		})
	}
}

func (dc *DiscoveryConnection) failChangeStart(err error) {
	dc.mu.Lock()
	dc.starting = false
	dc.changeCancel = nil
	if err != nil {
		dc.lastError = err.Error()
	}
	dc.mu.Unlock()
}

func (dc *DiscoveryConnection) watchNodeChanges(
	ctx context.Context,
	watcher kclient.NodeChangeWatcher,
	done chan struct{},
) {
	defer close(done)
	for {
		change, err := watcher.Next(ctx)
		if err != nil {
			if context.Cause(ctx) == nil &&
				!errors.Is(err, kclient.ErrNodeChangeWatcherClosed) {
				dc.mu.Lock()
				dc.lastError = err.Error()
				dc.mu.Unlock()
			}
			return
		}
		dc.applyNodeChange(change)
	}
}

func (dc *DiscoveryConnection) applyNodeChange(
	change kclient.NodeChange,
) {
	dc.mu.Lock()
	if dc.state != DiscoveryStateRunning {
		dc.mu.Unlock()
		return
	}
	toClose := dc.applyNodeChangeLocked(change)
	dc.mu.Unlock()
	dc.closeEvicted(toClose)
}

func (dc *DiscoveryConnection) applyNodeChangeLocked(
	change kclient.NodeChange,
) []*ggrpc.ClientConn {
	desired := grpcTopologyKeys(change.Current())
	retired := make(map[string]struct{})
	for key := range dc.desired {
		if _, exists := desired[key]; !exists {
			retired[key] = struct{}{}
		}
	}
	for _, instance := range change.Removed() {
		for key := range grpcInstanceKeys(instance) {
			retired[key] = struct{}{}
		}
	}
	for _, update := range change.Updated() {
		for key := range grpcInstanceKeys(update.Previous()) {
			retired[key] = struct{}{}
		}
	}
	for key := range desired {
		delete(retired, key)
	}

	toClose := make([]*ggrpc.ClientConn, 0)
	for key, entry := range dc.entries {
		if _, exists := desired[key]; exists {
			entry.retired = false
			continue
		}
		entry.retired = true
		retired[key] = struct{}{}
		if entry.active == 0 {
			delete(dc.entries, key)
			toClose = append(toClose, entry.conn)
			dc.evictions++
		}
	}
	for key, pending := range dc.dialing {
		if _, exists := desired[key]; exists {
			continue
		}
		retired[key] = struct{}{}
		if pending.cancel != nil {
			pending.cancel(ErrNodeRetired)
		}
	}
	dc.desired = desired
	dc.retired = retired
	dc.topologyManaged = true
	dc.topologyVersion++
	dc.topologyRevision = change.Revision()
	dc.reconciliations++
	return toClose
}

func (dc *DiscoveryConnection) cancelPendingDialsLocked(cause error) {
	for _, pending := range dc.dialing {
		if pending.cancel != nil {
			pending.cancel(cause)
		}
	}
}

func grpcTopologyKeys(
	instances []registry.Instance,
) map[string]struct{} {
	result := make(map[string]struct{})
	for _, instance := range instances {
		for key := range grpcInstanceKeys(instance) {
			result[key] = struct{}{}
		}
	}
	return result
}

func grpcInstanceKeys(instance registry.Instance) map[string]struct{} {
	result := make(map[string]struct{})
	metadata := instance.Metadata()
	for _, endpoint := range instance.Endpoints() {
		if _, err := parseGRPCEndpointValue(endpoint); err != nil {
			continue
		}
		result[discoveryNodeKey(instance.ID(), endpoint, metadata)] = struct{}{}
	}
	return result
}

func discoveryNodeKey(
	id string,
	endpoint string,
	metadata map[string]string,
) string {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var fingerprint strings.Builder
	for _, key := range keys {
		value := metadata[key]
		fingerprint.WriteString(strconv.Itoa(len(key)))
		fingerprint.WriteByte(':')
		fingerprint.WriteString(key)
		fingerprint.WriteString(strconv.Itoa(len(value)))
		fingerprint.WriteByte(':')
		fingerprint.WriteString(value)
	}
	return id + "\x00" + endpoint + "\x00" + fingerprint.String()
}

func waitDiscoveryChannels(
	ctx context.Context,
	poolDone <-chan struct{},
	changeDone <-chan struct{},
) error {
	for poolDone != nil || changeDone != nil {
		select {
		case <-poolDone:
			poolDone = nil
		case <-changeDone:
			changeDone = nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

func (dc *DiscoveryConnection) currentTopologyVersion() uint64 {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.topologyVersion
}

func (dc *DiscoveryConnection) nodeRetiredLocked(
	key string,
	selectionVersion uint64,
) bool {
	if !dc.topologyManaged {
		return false
	}
	if _, retired := dc.retired[key]; retired {
		return true
	}
	if dc.topologyVersion > selectionVersion {
		_, current := dc.desired[key]
		return !current
	}
	return false
}

func retiredNodeError(node selector.Node) error {
	return failure.MarkTransport(fmt.Errorf(
		"%w: %s at %s",
		ErrNodeRetired,
		node.ID(),
		node.Endpoint(),
	))
}

func (dc *DiscoveryConnection) evictLocked(
	requestedKey string,
	now time.Time,
) []*ggrpc.ClientConn {
	evicted := make([]*ggrpc.ClientConn, 0)
	for key, entry := range dc.entries {
		if key == requestedKey || entry.active != 0 {
			continue
		}
		if now.Sub(entry.lastUsed) >= dc.idleTimeout {
			delete(dc.entries, key)
			evicted = append(evicted, entry.conn)
			dc.evictions++
		}
	}
	for len(dc.entries)+len(dc.dialing) >=
		dc.maxConnections {
		var oldest *connectionEntry
		for _, entry := range dc.entries {
			if entry.active != 0 ||
				oldest != nil && !entry.lastUsed.Before(oldest.lastUsed) {
				continue
			}
			oldest = entry
		}
		if oldest == nil {
			break
		}
		delete(dc.entries, oldest.key)
		evicted = append(evicted, oldest.conn)
		dc.evictions++
	}
	return evicted
}

func (dc *DiscoveryConnection) closeEvicted(
	connections []*ggrpc.ClientConn,
) {
	var closeErr error
	for _, clientConnection := range connections {
		if err := clientConnection.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	if closeErr != nil {
		dc.mu.Lock()
		dc.stopErr = errors.Join(dc.stopErr, closeErr)
		dc.lastError = closeErr.Error()
		dc.mu.Unlock()
	}
}

func (dc *DiscoveryConnection) beginFinalizeLocked() (
	[]*ggrpc.ClientConn,
	bool,
) {
	if dc.state != DiscoveryStateStopping ||
		dc.finalizing ||
		dc.active != 0 ||
		len(dc.dialing) != 0 {
		return nil, false
	}
	dc.finalizing = true
	result := make([]*ggrpc.ClientConn, 0, len(dc.entries))
	for _, entry := range dc.entries {
		result = append(result, entry.conn)
	}
	dc.entries = make(map[string]*connectionEntry)
	return result, true
}

func (dc *DiscoveryConnection) finalize(
	connections []*ggrpc.ClientConn,
) {
	var closeErr error
	for _, clientConnection := range connections {
		if err := clientConnection.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	dc.mu.Lock()
	dc.stopErr = errors.Join(dc.stopErr, closeErr)
	if closeErr != nil {
		dc.lastError = closeErr.Error()
	}
	dc.state = DiscoveryStateStopped
	dc.doneOnce.Do(func() { close(dc.done) })
	dc.mu.Unlock()
}

func selectedGRPCContext(
	ctx context.Context,
	target operation.Operation,
	node selector.Node,
) (context.Context, error) {
	endpoint, err := parseGRPCEndpoint(node)
	if err != nil {
		return nil, err
	}
	peer, err := operation.NewPeer(endpoint.Scheme, endpoint.Host)
	if err != nil {
		return nil, fmt.Errorf("%w: peer: %w", ErrInvalidEndpoint, err)
	}
	info, err := operation.NewRequestInfo(target, operation.WithPeer(peer))
	if err != nil {
		return nil, err
	}
	return operation.WithRequestInfo(ctx, info), nil
}

func parseGRPCEndpoint(node selector.Node) (*url.URL, error) {
	return parseGRPCEndpointValue(node.Endpoint())
}

func parseGRPCEndpointValue(value string) (*url.URL, error) {
	endpoint, err := url.Parse(value)
	if err != nil ||
		endpoint.Scheme != "grpc" && endpoint.Scheme != "grpcs" ||
		endpoint.Host == "" ||
		endpoint.User != nil ||
		endpoint.Opaque != "" ||
		endpoint.RawQuery != "" ||
		endpoint.Fragment != "" ||
		endpoint.RawPath != "" ||
		endpoint.Path != "" && endpoint.Path != "/" {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrInvalidEndpoint,
			value,
		)
	}
	return endpoint, nil
}

func completeSelection(
	ctx context.Context,
	done selector.Done,
	started time.Time,
	resultErr *error,
) {
	recovered := recover()
	feedbackErr := *resultErr
	if recovered != nil {
		feedbackErr = errors.New("grpc transport: unary invocation panic")
	}
	done(selectionResult(ctx, started, feedbackErr))
	if recovered != nil {
		panic(recovered)
	}
}

func selectionResult(
	ctx context.Context,
	started time.Time,
	err error,
) selector.Result {
	feedbackErr := feedbackFailure(err)
	return selector.Result{
		Latency:  time.Since(started),
		Error:    feedbackErr,
		Canceled: errors.Is(feedbackErr, context.Canceled),
		Retried:  operation.AttemptFromContext(ctx) > 1,
	}
}

func feedbackFailure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch status.Code(err) {
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.Unavailable:
		return failure.MarkTransport(err)
	default:
		return err
	}
}

func dependencyFailure(err error) error {
	if err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return failure.MarkTransport(err)
}

func validDiscoveryName(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func isNilPicker(picker kclient.Picker) bool {
	if picker == nil {
		return true
	}
	value := reflect.ValueOf(picker)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilNodeChangeSource(source kclient.NodeChangeSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilNodeChangeWatcher(watcher kclient.NodeChangeWatcher) bool {
	if watcher == nil {
		return true
	}
	value := reflect.ValueOf(watcher)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilClientConnection(dc *ggrpc.ClientConn) bool {
	return dc == nil
}

var _ ggrpc.ClientConnInterface = (*DiscoveryConnection)(nil)
