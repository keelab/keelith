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
	key        string
	node       selector.Node
	connection *ggrpc.ClientConn
	active     int
	lastUsed   time.Time
	retired    bool
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
func (connection *DiscoveryConnection) Name() string {
	if connection == nil {
		return ""
	}
	return connection.name
}

// Dependencies returns the optional Router component dependency.
func (connection *DiscoveryConnection) Dependencies() []string {
	if connection == nil || connection.dependency == "" {
		return nil
	}
	return []string{connection.dependency}
}

// Start enables dynamic call leases.
func (connection *DiscoveryConnection) Start(ctx context.Context) error {
	if connection == nil {
		return fmt.Errorf("%w: discovery connection is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	connection.mu.Lock()
	if connection.state != DiscoveryStateNew || connection.starting {
		connection.mu.Unlock()
		return ErrAlreadyStarted
	}
	if connection.nodeChanges == nil {
		connection.state = DiscoveryStateRunning
		connection.mu.Unlock()
		return nil
	}
	connection.starting = true
	runtimeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	connection.changeCancel = cancel
	connection.mu.Unlock()

	watcher, err := connection.nodeChanges.WatchNodeChanges(runtimeCtx)
	if err != nil {
		cancel()
		connection.failChangeStart(err)
		return fmt.Errorf("grpc transport: watch node changes: %w", err)
	}
	if isNilNodeChangeWatcher(watcher) {
		cancel()
		err = errors.New("node change source returned a nil watcher")
		connection.failChangeStart(err)
		return fmt.Errorf("grpc transport: %w", err)
	}
	initial, err := watcher.Next(ctx)
	if err != nil {
		cancel()
		_ = watcher.Close()
		connection.failChangeStart(err)
		return fmt.Errorf("grpc transport: initial node change: %w", err)
	}

	connection.mu.Lock()
	if connection.state != DiscoveryStateNew {
		connection.starting = false
		connection.changeCancel = nil
		connection.mu.Unlock()
		cancel()
		_ = watcher.Close()
		return ErrDiscoveryNotRunning
	}
	connection.starting = false
	connection.state = DiscoveryStateRunning
	connection.topologyManaged = true
	connection.changeWatcher = watcher
	connection.changeDone = make(chan struct{})
	connection.applyNodeChangeLocked(initial)
	changeDone := connection.changeDone
	connection.mu.Unlock()

	go connection.watchNodeChanges(runtimeCtx, watcher, changeDone)
	return nil
}

// Stop rejects new leases, drains active calls/streams, and closes connections.
func (connection *DiscoveryConnection) Stop(ctx context.Context) error {
	if connection == nil {
		return nil
	}
	if ctx == nil {
		return ErrNilContext
	}

	connection.mu.Lock()
	switch connection.state {
	case DiscoveryStateNew:
		connection.state = DiscoveryStateStopping
	case DiscoveryStateRunning:
		connection.state = DiscoveryStateStopping
	case DiscoveryStateStopping:
	case DiscoveryStateStopped:
		err := connection.stopErr
		connection.mu.Unlock()
		return err
	}
	changeCancel := connection.changeCancel
	changeWatcher := connection.changeWatcher
	changeDone := connection.changeDone
	connection.cancelPendingDialsLocked(ErrDiscoveryNotRunning)
	toClose, finalize := connection.beginFinalizeLocked()
	done := connection.done
	connection.mu.Unlock()
	if changeCancel != nil {
		changeCancel()
	}
	if !isNilNodeChangeWatcher(changeWatcher) {
		_ = changeWatcher.Close()
	}
	if finalize {
		connection.finalize(toClose)
	}
	if err := waitDiscoveryChannels(ctx, done, changeDone); err != nil {
		return err
	}
	connection.mu.Lock()
	err := connection.stopErr
	connection.mu.Unlock()
	return err
}

// Invoke selects one node and performs a unary call on its pooled ClientConn.
func (connection *DiscoveryConnection) Invoke(
	ctx context.Context,
	method string,
	request any,
	reply any,
	options ...ggrpc.CallOption,
) (resultErr error) {
	if ctx == nil {
		return ErrNilContext
	}
	if err := connection.accepting(); err != nil {
		return err
	}
	target, err := operationFromMethod(method, operation.KindUnary)
	if err != nil {
		return err
	}
	started := time.Now()
	topologyVersion := connection.currentTopologyVersion()
	node, done, err := connection.picker.Pick(ctx, target)
	if err != nil {
		return dependencyFailure(err)
	}
	defer completeSelection(ctx, done, started, &resultErr)

	routedContext, err := selectedGRPCContext(ctx, target, node)
	if err != nil {
		return failure.MarkTransport(err)
	}
	entry, release, err := connection.lease(
		routedContext,
		node,
		topologyVersion,
	)
	if err != nil {
		return err
	}
	defer release()
	return entry.connection.Invoke(
		routedContext,
		method,
		request,
		reply,
		options...,
	)
}

// NewStream selects one node and holds its lease until stream termination.
func (connection *DiscoveryConnection) NewStream(
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
	if err := connection.accepting(); err != nil {
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
	topologyVersion := connection.currentTopologyVersion()
	node, done, err := connection.picker.Pick(ctx, target)
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
	entry, releaseLease, err := connection.lease(
		routedContext,
		node,
		topologyVersion,
	)
	if err != nil {
		return nil, err
	}
	release = releaseLease
	stream, err := entry.connection.NewStream(
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
func (connection *DiscoveryConnection) Describe() DiscoveryDescription {
	if connection == nil {
		return DiscoveryDescription{
			State:     DiscoveryStateStopped,
			LastError: "discovery connection is nil",
		}
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	retired := 0
	for _, entry := range connection.entries {
		if entry.retired {
			retired++
		}
	}
	return DiscoveryDescription{
		Name:             connection.name,
		State:            connection.state,
		Connections:      len(connection.entries),
		Dialing:          len(connection.dialing),
		Active:           connection.active,
		MaxConnections:   connection.maxConnections,
		DialAttempts:     connection.dialAttempts,
		DialFailures:     connection.dialFailures,
		Evictions:        connection.evictions,
		Retired:          retired,
		Reconciliations:  connection.reconciliations,
		TopologyRevision: connection.topologyRevision,
		LastError:        connection.lastError,
	}
}

func (connection *DiscoveryConnection) accepting() error {
	if connection == nil {
		return ErrDiscoveryNotRunning
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.state != DiscoveryStateRunning {
		return ErrDiscoveryNotRunning
	}
	return nil
}

func (connection *DiscoveryConnection) lease(
	ctx context.Context,
	node selector.Node,
	selectionVersion uint64,
) (*connectionEntry, func(), error) {
	if _, err := parseGRPCEndpoint(node); err != nil {
		return nil, nil, err
	}
	key := discoveryNodeKey(node.ID(), node.Endpoint(), node.Metadata())
	for {
		connection.mu.Lock()
		if connection.state != DiscoveryStateRunning {
			connection.mu.Unlock()
			return nil, nil, ErrDiscoveryNotRunning
		}
		if connection.nodeRetiredLocked(key, selectionVersion) {
			connection.mu.Unlock()
			return nil, nil, retiredNodeError(node)
		}
		if entry := connection.entries[key]; entry != nil {
			entry.active++
			connection.active++
			connection.mu.Unlock()
			return entry, connection.releaseFunc(entry), nil
		}
		if pending := connection.dialing[key]; pending != nil {
			done := pending.done
			connection.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, nil, context.Cause(ctx)
			}
		}

		evicted := connection.evictLocked(key, time.Now())
		if len(connection.entries)+len(connection.dialing) >=
			connection.maxConnections {
			connection.mu.Unlock()
			connection.closeEvicted(evicted)
			return nil, nil, ErrPoolExhausted
		}
		dialContext, cancelDial := context.WithCancelCause(ctx)
		pending := &pendingDial{
			done:   make(chan struct{}),
			cancel: cancelDial,
		}
		connection.dialing[key] = pending
		connection.dialAttempts++
		connection.mu.Unlock()
		connection.closeEvicted(evicted)

		clientConnection, dialErr := connection.dial(dialContext, node)
		dialCause := context.Cause(dialContext)
		cancelDial(context.Canceled)
		if dialErr == nil && isNilClientConnection(clientConnection) {
			dialErr = errors.New("dial function returned a nil ClientConn")
		}

		connection.mu.Lock()
		delete(connection.dialing, key)
		close(pending.done)
		if errors.Is(dialCause, ErrNodeRetired) {
			toClose, finalize := connection.beginFinalizeLocked()
			connection.mu.Unlock()
			if !isNilClientConnection(clientConnection) {
				_ = clientConnection.Close()
			}
			if finalize {
				connection.finalize(toClose)
			}
			return nil, nil, retiredNodeError(node)
		}
		if errors.Is(dialCause, ErrDiscoveryNotRunning) {
			toClose, finalize := connection.beginFinalizeLocked()
			connection.mu.Unlock()
			if !isNilClientConnection(clientConnection) {
				_ = clientConnection.Close()
			}
			if finalize {
				connection.finalize(toClose)
			}
			return nil, nil, ErrDiscoveryNotRunning
		}
		if dialCause != nil {
			toClose, finalize := connection.beginFinalizeLocked()
			connection.mu.Unlock()
			if !isNilClientConnection(clientConnection) {
				_ = clientConnection.Close()
			}
			if finalize {
				connection.finalize(toClose)
			}
			return nil, nil, dialCause
		}
		if dialErr != nil {
			connection.dialFailures++
			connection.lastError = dialErr.Error()
			toClose, finalize := connection.beginFinalizeLocked()
			connection.mu.Unlock()
			if !isNilClientConnection(clientConnection) {
				_ = clientConnection.Close()
			}
			if finalize {
				connection.finalize(toClose)
			}
			return nil, nil, failure.MarkTransport(fmt.Errorf(
				"grpc transport: dial node %q: %w",
				node.ID(),
				dialErr,
			))
		}
		if connection.state != DiscoveryStateRunning {
			toClose, finalize := connection.beginFinalizeLocked()
			connection.mu.Unlock()
			_ = clientConnection.Close()
			if finalize {
				connection.finalize(toClose)
			}
			return nil, nil, ErrDiscoveryNotRunning
		}
		if connection.nodeRetiredLocked(key, selectionVersion) {
			toClose, finalize := connection.beginFinalizeLocked()
			connection.mu.Unlock()
			_ = clientConnection.Close()
			if finalize {
				connection.finalize(toClose)
			}
			return nil, nil, retiredNodeError(node)
		}
		entry := &connectionEntry{
			key:        key,
			node:       node,
			connection: clientConnection,
			active:     1,
			lastUsed:   time.Now(),
		}
		connection.entries[key] = entry
		connection.active++
		connection.lastError = ""
		connection.mu.Unlock()
		return entry, connection.releaseFunc(entry), nil
	}
}

func (connection *DiscoveryConnection) releaseFunc(
	entry *connectionEntry,
) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			connection.mu.Lock()
			if entry.active > 0 {
				entry.active--
				connection.active--
			}
			entry.lastUsed = time.Now()
			var retired []*ggrpc.ClientConn
			if entry.active == 0 && entry.retired {
				if connection.entries[entry.key] == entry {
					delete(connection.entries, entry.key)
					retired = append(retired, entry.connection)
					connection.evictions++
				}
			}
			toClose, finalize := connection.beginFinalizeLocked()
			connection.mu.Unlock()
			connection.closeEvicted(retired)
			if finalize {
				connection.finalize(toClose)
			}
		})
	}
}

func (connection *DiscoveryConnection) failChangeStart(err error) {
	connection.mu.Lock()
	connection.starting = false
	connection.changeCancel = nil
	if err != nil {
		connection.lastError = err.Error()
	}
	connection.mu.Unlock()
}

func (connection *DiscoveryConnection) watchNodeChanges(
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
				connection.mu.Lock()
				connection.lastError = err.Error()
				connection.mu.Unlock()
			}
			return
		}
		connection.applyNodeChange(change)
	}
}

func (connection *DiscoveryConnection) applyNodeChange(
	change kclient.NodeChange,
) {
	connection.mu.Lock()
	if connection.state != DiscoveryStateRunning {
		connection.mu.Unlock()
		return
	}
	toClose := connection.applyNodeChangeLocked(change)
	connection.mu.Unlock()
	connection.closeEvicted(toClose)
}

func (connection *DiscoveryConnection) applyNodeChangeLocked(
	change kclient.NodeChange,
) []*ggrpc.ClientConn {
	desired := grpcTopologyKeys(change.Current())
	retired := make(map[string]struct{})
	for key := range connection.desired {
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
	for key, entry := range connection.entries {
		if _, exists := desired[key]; exists {
			entry.retired = false
			continue
		}
		entry.retired = true
		retired[key] = struct{}{}
		if entry.active == 0 {
			delete(connection.entries, key)
			toClose = append(toClose, entry.connection)
			connection.evictions++
		}
	}
	for key, pending := range connection.dialing {
		if _, exists := desired[key]; exists {
			continue
		}
		retired[key] = struct{}{}
		if pending.cancel != nil {
			pending.cancel(ErrNodeRetired)
		}
	}
	connection.desired = desired
	connection.retired = retired
	connection.topologyManaged = true
	connection.topologyVersion++
	connection.topologyRevision = change.Revision()
	connection.reconciliations++
	return toClose
}

func (connection *DiscoveryConnection) cancelPendingDialsLocked(cause error) {
	for _, pending := range connection.dialing {
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

func (connection *DiscoveryConnection) currentTopologyVersion() uint64 {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.topologyVersion
}

func (connection *DiscoveryConnection) nodeRetiredLocked(
	key string,
	selectionVersion uint64,
) bool {
	if !connection.topologyManaged {
		return false
	}
	if _, retired := connection.retired[key]; retired {
		return true
	}
	if connection.topologyVersion > selectionVersion {
		_, current := connection.desired[key]
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

func (connection *DiscoveryConnection) evictLocked(
	requestedKey string,
	now time.Time,
) []*ggrpc.ClientConn {
	evicted := make([]*ggrpc.ClientConn, 0)
	for key, entry := range connection.entries {
		if key == requestedKey || entry.active != 0 {
			continue
		}
		if now.Sub(entry.lastUsed) >= connection.idleTimeout {
			delete(connection.entries, key)
			evicted = append(evicted, entry.connection)
			connection.evictions++
		}
	}
	for len(connection.entries)+len(connection.dialing) >=
		connection.maxConnections {
		var oldest *connectionEntry
		for _, entry := range connection.entries {
			if entry.active != 0 ||
				oldest != nil && !entry.lastUsed.Before(oldest.lastUsed) {
				continue
			}
			oldest = entry
		}
		if oldest == nil {
			break
		}
		delete(connection.entries, oldest.key)
		evicted = append(evicted, oldest.connection)
		connection.evictions++
	}
	return evicted
}

func (connection *DiscoveryConnection) closeEvicted(
	connections []*ggrpc.ClientConn,
) {
	var closeErr error
	for _, clientConnection := range connections {
		if err := clientConnection.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	if closeErr != nil {
		connection.mu.Lock()
		connection.stopErr = errors.Join(connection.stopErr, closeErr)
		connection.lastError = closeErr.Error()
		connection.mu.Unlock()
	}
}

func (connection *DiscoveryConnection) beginFinalizeLocked() (
	[]*ggrpc.ClientConn,
	bool,
) {
	if connection.state != DiscoveryStateStopping ||
		connection.finalizing ||
		connection.active != 0 ||
		len(connection.dialing) != 0 {
		return nil, false
	}
	connection.finalizing = true
	result := make([]*ggrpc.ClientConn, 0, len(connection.entries))
	for _, entry := range connection.entries {
		result = append(result, entry.connection)
	}
	connection.entries = make(map[string]*connectionEntry)
	return result, true
}

func (connection *DiscoveryConnection) finalize(
	connections []*ggrpc.ClientConn,
) {
	var closeErr error
	for _, clientConnection := range connections {
		if err := clientConnection.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	connection.mu.Lock()
	connection.stopErr = errors.Join(connection.stopErr, closeErr)
	if closeErr != nil {
		connection.lastError = closeErr.Error()
	}
	connection.state = DiscoveryStateStopped
	connection.doneOnce.Do(func() { close(connection.done) })
	connection.mu.Unlock()
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
	for _, character := range value {
		if unicode.IsControl(character) {
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

func isNilClientConnection(connection *ggrpc.ClientConn) bool {
	return connection == nil
}

var _ ggrpc.ClientConnInterface = (*DiscoveryConnection)(nil)
