package continuation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

const (
	defaultWorkflowPollInterval = time.Second
	defaultWorkflowBatchSize    = 100
	defaultWorkflowNodeBudget   = 10_000
	maxWorkflowReadyScan        = 10_000
)

// WorkflowChildRuntime is the narrow child-call capability used by a DAG.
type WorkflowChildRuntime interface {
	StartCall(context.Context, CallID, Operation, []byte) (Snapshot, error)
	Attach(context.Context, CallID, uint64, int) (Attachment, error)
}

// WorkflowExecution is one stable at-least-once Machine node attempt.
type WorkflowExecution struct {
	ParentCallID CallID
	NodeID       string
	Attempt      uint32
	Operation    Operation
	ExecutionID  string
}

// WorkflowNodeResult is a bounded terminal Machine node result.
type WorkflowNodeResult struct {
	Status       WorkflowNodeStatus
	FailureClass string
}

// WorkflowNodeHandler executes one application-defined Machine node.
type WorkflowNodeHandler interface {
	Execute(context.Context, WorkflowExecution) (WorkflowNodeResult, error)
}

// WorkflowNodeHandlerFunc adapts a function to WorkflowNodeHandler.
type WorkflowNodeHandlerFunc func(
	context.Context,
	WorkflowExecution,
) (WorkflowNodeResult, error)

// Execute invokes the adapted workflow node function.
func (fn WorkflowNodeHandlerFunc) Execute(
	ctx context.Context,
	execution WorkflowExecution,
) (WorkflowNodeResult, error) {
	return fn(ctx, execution)
}

// WorkflowRuntimeConfig configures durable DAG reconciliation.
type WorkflowRuntimeConfig struct {
	Store             Store
	Registry          *Registry
	ChildRuntime      WorkflowChildRuntime
	Handlers          map[Operation]WorkflowNodeHandler
	ExecutorID        string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
	BatchSize         int
	MaxNodeActions    int
	TerminalRetention time.Duration
	Clock             func() time.Time
}

// WorkflowRuntime reconciles version-frozen durable parent snapshots.
type WorkflowRuntime struct {
	store             Store
	leases            LeaseStore
	registry          *Registry
	children          WorkflowChildRuntime
	handlers          map[string]WorkflowNodeHandler
	executorID        string
	leaseDuration     time.Duration
	heartbeat         time.Duration
	pollInterval      time.Duration
	batchSize         int
	maxNodeActions    int
	terminalRetention time.Duration
	now               func() time.Time
}

// NewWorkflowRuntime snapshots bounded dependencies and handlers.
func NewWorkflowRuntime(config WorkflowRuntimeConfig) (*WorkflowRuntime, error) {
	if isNilStore(config.Store) || config.Registry == nil ||
		!config.Registry.Frozen() {
		return nil, ErrInvalidRuntime
	}
	leases, ok := config.Store.(LeaseStore)
	if !ok || isNilLeaseStore(leases) {
		return nil, ErrLeaseUnsupported
	}
	if config.ExecutorID == "" {
		var err error
		config.ExecutorID, err = randomExecutorID()
		if err != nil {
			return nil, ErrInvalidRuntime
		}
	}
	if config.LeaseDuration == 0 {
		config.LeaseDuration = defaultLeaseDuration
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultWorkflowPollInterval
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = config.LeaseDuration / 3
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaultWorkflowBatchSize
	}
	if config.MaxNodeActions == 0 {
		config.MaxNodeActions = defaultWorkflowNodeBudget
	}
	if config.TerminalRetention == 0 {
		config.TerminalRetention = defaultTerminalRetention
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	if !validLeaseOwner(config.ExecutorID) || config.LeaseDuration <= 0 ||
		config.LeaseDuration > maxLeaseDuration || config.PollInterval <= 0 ||
		config.HeartbeatInterval <= 0 ||
		config.HeartbeatInterval >= config.LeaseDuration ||
		config.PollInterval > time.Minute || config.BatchSize <= 0 ||
		config.BatchSize > 10_000 || config.MaxNodeActions <= 0 ||
		config.MaxNodeActions > maxWorkflowNodes ||
		config.TerminalRetention <= 0 ||
		config.TerminalRetention > maxTerminalRetention {
		return nil, ErrInvalidRuntime
	}
	handlers := make(map[string]WorkflowNodeHandler, len(config.Handlers))
	for operation, handler := range config.Handlers {
		if !validOperation(operation.value) || isNilWorkflowValue(handler) {
			return nil, ErrInvalidRuntime
		}
		handlers[operation.String()] = handler
	}
	return &WorkflowRuntime{
		store:             config.Store,
		leases:            leases,
		registry:          config.Registry,
		children:          config.ChildRuntime,
		handlers:          handlers,
		executorID:        config.ExecutorID,
		leaseDuration:     config.LeaseDuration,
		heartbeat:         config.HeartbeatInterval,
		pollInterval:      config.PollInterval,
		batchSize:         config.BatchSize,
		maxNodeActions:    config.MaxNodeActions,
		terminalRetention: config.TerminalRetention,
		now:               config.Clock,
	}, nil
}

// Start durably creates one exact workflow definition version.
func (runtime *WorkflowRuntime) Start(
	ctx context.Context,
	callID CallID,
	operation Operation,
	version string,
	input []byte,
) (Snapshot, error) {
	if runtime == nil || ctx == nil {
		return Snapshot{}, ErrInvalidRuntime
	}
	definition, exists := runtime.registry.ResolveWorkflow(operation, version)
	if !exists {
		return Snapshot{}, ErrWorkflowDefinitionMismatch
	}
	snapshot, err := newWorkflowSnapshot(
		callID,
		definition,
		input,
		runtime.now(),
	)
	if err != nil {
		return Snapshot{}, err
	}
	return runtime.store.Create(ctx, snapshot)
}

// RunOnce reconciles one bounded ready workflow batch.
func (runtime *WorkflowRuntime) RunOnce(ctx context.Context) (int, error) {
	if runtime == nil || ctx == nil {
		return 0, ErrInvalidRuntime
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	ready, err := runtime.store.ListReady(ctx, maxWorkflowReadyScan)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, candidate := range ready {
		if processed >= runtime.batchSize {
			break
		}
		if candidate.workflow == nil {
			continue
		}
		if cause := context.Cause(ctx); cause != nil {
			return processed, cause
		}
		claim, claimErr := runtime.leases.Claim(ctx, ClaimRequest{
			CallID:           candidate.CallID(),
			ExpectedRevision: candidate.Revision(),
			OwnerID:          runtime.executorID,
			LeaseDuration:    runtime.leaseDuration,
		})
		if errors.Is(claimErr, ErrConflict) || errors.Is(claimErr, ErrNotReady) ||
			errors.Is(claimErr, ErrTimerNotReady) || errors.Is(claimErr, ErrLeaseHeld) {
			continue
		}
		if claimErr != nil {
			return processed, claimErr
		}
		processed++
		next, reconcileErr := runtime.reconcileWithHeartbeat(
			ctx,
			claim.Snapshot,
			candidate.ReadyAt(),
		)
		if reconcileErr != nil {
			runtime.release(ctx, claim)
			return processed, reconcileErr
		}
		request := CommitRequest{
			ExpectedRevision: claim.Snapshot.Revision(),
			Fence:            claim.Snapshot.Fence(),
			LeaseOwner:       claim.OwnerID,
			Snapshot:         next,
		}
		if next.Status().Terminal() {
			request.ExpiresAt = runtime.now().UTC().Add(runtime.terminalRetention)
		}
		if _, err := runtime.store.Transition(ctx, request); err != nil {
			runtime.release(ctx, claim)
			return processed, err
		}
	}
	return processed, nil
}

// Run polls and reconciles workflow parents until the context ends.
func (runtime *WorkflowRuntime) Run(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return ErrInvalidRuntime
	}
	for {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if _, err := runtime.RunOnce(ctx); err != nil {
			return err
		}
		timer := time.NewTimer(runtime.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
}

func (runtime *WorkflowRuntime) reconcileWithHeartbeat(
	ctx context.Context,
	current Snapshot,
	authoritativeWake time.Time,
) (Snapshot, error) {
	reconcileCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	done := make(chan struct{})
	heartbeatResult := make(chan error, 1)
	go func() {
		timer := time.NewTicker(runtime.heartbeat)
		defer timer.Stop()
		for {
			select {
			case <-done:
				heartbeatResult <- nil
				return
			case <-reconcileCtx.Done():
				heartbeatResult <- context.Cause(reconcileCtx)
				return
			case <-timer.C:
				_, err := runtime.leases.Renew(reconcileCtx, LeaseRequest{
					CallID:        current.CallID(),
					Revision:      current.Revision(),
					Fence:         current.Fence(),
					OwnerID:       runtime.executorID,
					LeaseDuration: runtime.leaseDuration,
				})
				if err != nil {
					cancel(err)
					heartbeatResult <- err
					return
				}
			}
		}
	}()
	next, err := runtime.reconcile(reconcileCtx, current, authoritativeWake)
	close(done)
	heartbeatErr := <-heartbeatResult
	if heartbeatErr != nil && !errors.Is(heartbeatErr, context.Canceled) {
		return Snapshot{}, heartbeatErr
	}
	return next, err
}

func (runtime *WorkflowRuntime) reconcile(
	ctx context.Context,
	current Snapshot,
	authoritativeWake time.Time,
) (Snapshot, error) {
	state := cloneWorkflowState(current.workflow)
	definition, exists := runtime.registry.ResolveWorkflow(
		current.Operation(),
		state.Version,
	)
	if !exists || definition.fingerprint != state.Fingerprint {
		return Snapshot{}, ErrWorkflowDefinitionMismatch
	}
	if current.Status() == StatusCancelRequested {
		skipNonTerminalWorkflowNodes(state)
		frame, err := NewFrame(FrameCanceled, nil)
		if err != nil {
			return Snapshot{}, err
		}
		return Apply(
			current,
			moveWorkflow(
				StatusCanceled,
				current.Fence(),
				state,
				time.Time{},
				frame,
			),
		)
	}
	frames := make([]Frame, 0)
	if err := runtime.refreshWaitingChildren(ctx, state, &frames); err != nil {
		return Snapshot{}, err
	}
	for id, node := range state.Nodes {
		if node.Kind == WorkflowNodeTimer && node.Status == WorkflowNodeWaiting &&
			!authoritativeWake.IsZero() && !authoritativeWake.Before(node.ReadyAt) {
			node.Status = WorkflowNodeSucceeded
			state.Nodes[id] = node
		}
	}
	actions := 0
	for actions < runtime.maxNodeActions {
		commands, err := planWorkflowForParent(
			definition,
			state,
			current.CallID(),
		)
		if err != nil {
			return Snapshot{}, err
		}
		if len(commands) == 0 {
			break
		}
		for _, command := range commands {
			if actions >= runtime.maxNodeActions {
				break
			}
			if err := runtime.executeCommand(
				ctx,
				current.CallID(),
				state,
				command,
				&frames,
			); err != nil {
				return Snapshot{}, err
			}
			actions++
		}
	}
	terminal, failed := workflowDefinitionOutcome(definition, state)
	if terminal {
		skipNonTerminalWorkflowNodes(state)
		status := StatusCompleted
		kind := FrameCompleted
		if failed {
			status = StatusFailed
			kind = FrameFailed
		}
		frame, err := NewFrame(kind, nil)
		if err != nil {
			return Snapshot{}, err
		}
		frames = append(frames, frame)
		return Apply(
			current,
			moveWorkflow(status, current.Fence(), state, time.Time{}, frames...),
		)
	}
	wakeAt := runtime.nextWakeAt(state)
	return Apply(
		current,
		moveWorkflow(StatusSuspended, current.Fence(), state, wakeAt, frames...),
	)
}

func (runtime *WorkflowRuntime) executeCommand(
	ctx context.Context,
	parent CallID,
	state *workflowSnapshotState,
	command workflowCommand,
	frames *[]Frame,
) error {
	node := state.Nodes[command.nodeID]
	if command.completeStatus != "" {
		node.Status = command.completeStatus
		if node.Status == WorkflowNodeFailed {
			node.FailureClass = "DEPENDENCY_FAILED"
		}
		state.Nodes[node.ID] = node
		return nil
	}
	node.Attempt = command.attempt
	switch command.kind {
	case WorkflowNodeTimer:
		node.Status = WorkflowNodeWaiting
		node.ReadyAt = command.readyAt
	case WorkflowNodeChild:
		if isNilWorkflowValue(runtime.children) {
			return ErrWorkflowHandlerNotFound
		}
		node.Status = WorkflowNodeWaiting
		node.ChildCallID = command.childCallID
		state.Nodes[node.ID] = node
		_, err := runtime.children.StartCall(
			ctx,
			command.childCallID,
			command.operation,
			nil,
		)
		if err != nil && !errors.Is(err, ErrAlreadyExists) {
			return fmt.Errorf("continuation: start child: %w", err)
		}
		return runtime.refreshChild(ctx, state, node.ID, frames)
	case WorkflowNodeMachine:
		handler, exists := runtime.handlers[command.operation.String()]
		if !exists || isNilWorkflowValue(handler) {
			return ErrWorkflowHandlerNotFound
		}
		node.Status = WorkflowNodeRunning
		state.Nodes[node.ID] = node
		result, err := handler.Execute(ctx, WorkflowExecution{
			ParentCallID: parent,
			NodeID:       node.ID,
			Attempt:      command.attempt,
			Operation:    command.operation,
			ExecutionID:  workflowExecutionID(parent, node.ID, command.attempt),
		})
		switch {
		case err != nil:
			node.Status = WorkflowNodeFailed
			node.FailureClass = "HANDLER_ERROR"
		case validateWorkflowNodeResult(result) != nil:
			return ErrInvalidWorkflow
		default:
			node.Status = result.Status
			node.FailureClass = result.FailureClass
		}
	case WorkflowNodeJoin:
		return ErrInvalidWorkflow
	default:
		return ErrInvalidWorkflow
	}
	state.Nodes[node.ID] = node
	return nil
}

func (runtime *WorkflowRuntime) refreshWaitingChildren(
	ctx context.Context,
	state *workflowSnapshotState,
	frames *[]Frame,
) error {
	for id, node := range state.Nodes {
		if node.Kind == WorkflowNodeChild && node.Status == WorkflowNodeWaiting {
			if err := runtime.refreshChild(ctx, state, id, frames); err != nil {
				return err
			}
		}
	}
	return nil
}

func (runtime *WorkflowRuntime) refreshChild(
	ctx context.Context,
	state *workflowSnapshotState,
	nodeID string,
	frames *[]Frame,
) error {
	if isNilWorkflowValue(runtime.children) {
		return ErrWorkflowHandlerNotFound
	}
	node := state.Nodes[nodeID]
	attachment, err := runtime.children.Attach(ctx, node.ChildCallID, 0, 1)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("continuation: attach child: %w", err)
	}
	if !attachment.Snapshot.Status().Terminal() {
		return nil
	}
	if attachment.Snapshot.Status() == StatusCompleted {
		node.Status = WorkflowNodeSucceeded
	} else {
		node.Status = WorkflowNodeFailed
		node.FailureClass = "CHILD_TERMINAL_FAILURE"
	}
	state.Nodes[nodeID] = node
	payload, err := json.Marshal(struct {
		NodeID  string `json:"node_id"`
		ChildID string `json:"child_id"`
		Status  Status `json:"status"`
	}{NodeID: nodeID, ChildID: node.ChildCallID.String(), Status: attachment.Snapshot.Status()})
	if err != nil {
		return ErrInvalidWorkflow
	}
	frame, err := NewFrame(FrameWorkflowChild, payload)
	if err != nil {
		return err
	}
	*frames = append(*frames, frame)
	return nil
}

func (runtime *WorkflowRuntime) nextWakeAt(state *workflowSnapshotState) time.Time {
	wakeAt := normalizeReadyAt(runtime.now().UTC().Add(runtime.pollInterval))
	for _, node := range state.Nodes {
		if node.Kind == WorkflowNodeTimer && node.Status == WorkflowNodeWaiting &&
			(wakeAt.IsZero() || node.ReadyAt.Before(wakeAt)) {
			wakeAt = node.ReadyAt
		}
	}
	return wakeAt
}

func (runtime *WorkflowRuntime) release(ctx context.Context, lease Lease) {
	_ = runtime.leases.Release(ctx, LeaseRequest{
		CallID:   lease.Snapshot.CallID(),
		Revision: lease.Snapshot.Revision(),
		Fence:    lease.Snapshot.Fence(),
		OwnerID:  lease.OwnerID,
	})
}

func validateWorkflowNodeResult(result WorkflowNodeResult) error {
	if result.Status != WorkflowNodeSucceeded && result.Status != WorkflowNodeFailed ||
		len(result.FailureClass) > maxWorkflowFailureSize ||
		(result.FailureClass != "" && !validIdentity(result.FailureClass)) ||
		result.Status == WorkflowNodeSucceeded && result.FailureClass != "" {
		return ErrInvalidWorkflow
	}
	return nil
}

func workflowExecutionID(parent CallID, nodeID string, attempt uint32) string {
	hasher := sha256.New()
	writeDigestPart(hasher, []byte("keelith-workflow-execution-v1"))
	writeDigestPart(hasher, []byte(parent.String()))
	writeDigestPart(hasher, []byte(nodeID))
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], attempt)
	writeDigestPart(hasher, encoded[:])
	return hex.EncodeToString(hasher.Sum(nil))
}

func isNilWorkflowValue(value any) bool {
	if value == nil {
		return true
	}
	reflection := reflect.ValueOf(value)
	switch reflection.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflection.IsNil()
	default:
		return false
	}
}
