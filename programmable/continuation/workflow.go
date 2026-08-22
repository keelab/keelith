package continuation

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

const (
	maxWorkflowNodes       = 10_000
	maxWorkflowEdges       = 100_000
	maxWorkflowTimerDelay  = 365 * 24 * time.Hour
	maxWorkflowFailureSize = 128
)

var (
	// ErrInvalidWorkflow reports a malformed definition, state, or command.
	ErrInvalidWorkflow = errors.New("continuation: invalid workflow")
	// ErrWorkflowCycle reports a dependency cycle in a workflow definition.
	ErrWorkflowCycle = errors.New("continuation: workflow contains a cycle")
	// ErrWorkflowDefinitionMismatch reports missing or changed frozen schema.
	ErrWorkflowDefinitionMismatch = errors.New("continuation: workflow definition mismatch")
	// ErrWorkflowHandlerNotFound reports a ready Machine node without a handler.
	ErrWorkflowHandlerNotFound = errors.New("continuation: workflow handler not found")
)

// WorkflowNodeKind identifies one deterministic DAG node behavior.
type WorkflowNodeKind string

const (
	// WorkflowNodeMachine invokes an application-defined deterministic handler.
	WorkflowNodeMachine WorkflowNodeKind = "machine"
	// WorkflowNodeTimer waits for an absolute durable deadline.
	WorkflowNodeTimer WorkflowNodeKind = "timer"
	// WorkflowNodeChild starts and observes one stable child call.
	WorkflowNodeChild WorkflowNodeKind = "child"
	// WorkflowNodeJoin evaluates dependency terminals without invoking user code.
	WorkflowNodeJoin WorkflowNodeKind = "join"
)

func (kind WorkflowNodeKind) valid() bool {
	switch kind {
	case WorkflowNodeMachine, WorkflowNodeTimer, WorkflowNodeChild, WorkflowNodeJoin:
		return true
	default:
		return false
	}
}

// WorkflowJoinPolicy defines how dependency terminals release a Join node.
type WorkflowJoinPolicy string

const (
	// WorkflowJoinAll succeeds only when every dependency succeeds.
	WorkflowJoinAll WorkflowJoinPolicy = "all"
	// WorkflowJoinAny succeeds when any dependency succeeds.
	WorkflowJoinAny WorkflowJoinPolicy = "any"
)

func (policy WorkflowJoinPolicy) valid() bool {
	return policy == WorkflowJoinAll || policy == WorkflowJoinAny
}

// WorkflowFailurePolicy controls whether a dependency failure propagates.
type WorkflowFailurePolicy string

const (
	// WorkflowFailFast propagates a failed dependency to the node.
	WorkflowFailFast WorkflowFailurePolicy = "fail_fast"
	// WorkflowContinue runs after all dependencies terminate despite failures.
	WorkflowContinue WorkflowFailurePolicy = "continue"
)

func (policy WorkflowFailurePolicy) normalized() WorkflowFailurePolicy {
	if policy == "" {
		return WorkflowFailFast
	}
	return policy
}

func (policy WorkflowFailurePolicy) valid() bool {
	return policy == WorkflowFailFast || policy == WorkflowContinue
}

// WorkflowNodeStatus is the durable lifecycle of one DAG node.
type WorkflowNodeStatus string

const (
	// WorkflowNodePending has not yet been scheduled.
	WorkflowNodePending WorkflowNodeStatus = "pending"
	// WorkflowNodeRunning is executing application work.
	WorkflowNodeRunning WorkflowNodeStatus = "running"
	// WorkflowNodeWaiting is waiting for a timer or child terminal state.
	WorkflowNodeWaiting WorkflowNodeStatus = "waiting"
	// WorkflowNodeSucceeded completed successfully.
	WorkflowNodeSucceeded WorkflowNodeStatus = "succeeded"
	// WorkflowNodeFailed completed with a stable failure class.
	WorkflowNodeFailed WorkflowNodeStatus = "failed"
	// WorkflowNodeSkipped is no longer needed after cancellation or join outcome.
	WorkflowNodeSkipped WorkflowNodeStatus = "skipped"
)

func (status WorkflowNodeStatus) valid() bool {
	switch status {
	case WorkflowNodePending, WorkflowNodeRunning, WorkflowNodeWaiting,
		WorkflowNodeSucceeded, WorkflowNodeFailed, WorkflowNodeSkipped:
		return true
	default:
		return false
	}
}

// Terminal reports whether a node needs no further execution.
func (status WorkflowNodeStatus) Terminal() bool {
	return status == WorkflowNodeSucceeded ||
		status == WorkflowNodeFailed ||
		status == WorkflowNodeSkipped
}

// WorkflowNodeSpec is mutable input copied by NewWorkflowDefinition.
type WorkflowNodeSpec struct {
	ID            string
	Kind          WorkflowNodeKind
	Operation     Operation
	DependsOn     []string
	Join          WorkflowJoinPolicy
	FailurePolicy WorkflowFailurePolicy
	Delay         time.Duration
}

// WorkflowDefinitionSpec identifies one immutable versioned DAG.
type WorkflowDefinitionSpec struct {
	Operation Operation
	Version   string
	Nodes     []WorkflowNodeSpec
}

// WorkflowDefinition is an immutable, validated, version-frozen DAG.
type WorkflowDefinition struct {
	operation   Operation
	version     string
	fingerprint string
	nodes       []WorkflowNodeSpec
	byID        map[string]int
}

// NewWorkflowDefinition validates budgets and returns a canonical DAG.
func NewWorkflowDefinition(spec WorkflowDefinitionSpec) (*WorkflowDefinition, error) {
	if !validOperation(spec.Operation.value) || !validIdentity(spec.Version) ||
		len(spec.Nodes) == 0 || len(spec.Nodes) > maxWorkflowNodes {
		return nil, ErrInvalidWorkflow
	}
	nodes := cloneWorkflowSpecs(spec.Nodes)
	byID := make(map[string]int, len(nodes))
	edges := 0
	for index := range nodes {
		node := &nodes[index]
		node.FailurePolicy = node.FailurePolicy.normalized()
		if !validIdentity(node.ID) || !node.Kind.valid() ||
			!node.FailurePolicy.valid() {
			return nil, ErrInvalidWorkflow
		}
		if _, exists := byID[node.ID]; exists {
			return nil, ErrInvalidWorkflow
		}
		byID[node.ID] = index
		edges += len(node.DependsOn)
		if edges > maxWorkflowEdges || !validWorkflowNodeShape(*node) {
			return nil, ErrInvalidWorkflow
		}
		seen := make(map[string]struct{}, len(node.DependsOn))
		for _, dependency := range node.DependsOn {
			if !validIdentity(dependency) || dependency == node.ID {
				return nil, ErrInvalidWorkflow
			}
			if _, duplicate := seen[dependency]; duplicate {
				return nil, ErrInvalidWorkflow
			}
			seen[dependency] = struct{}{}
		}
	}
	for _, node := range nodes {
		for _, dependency := range node.DependsOn {
			if _, exists := byID[dependency]; !exists {
				return nil, ErrInvalidWorkflow
			}
		}
	}
	topological, err := topologicalWorkflowNodes(nodes, byID)
	if err != nil {
		return nil, err
	}
	fingerprint, err := workflowFingerprint(spec.Operation, spec.Version, nodes)
	if err != nil {
		return nil, err
	}
	canonicalByID := make(map[string]int, len(topological))
	for index, node := range topological {
		canonicalByID[node.ID] = index
	}
	return &WorkflowDefinition{
		operation:   spec.Operation,
		version:     spec.Version,
		fingerprint: fingerprint,
		nodes:       topological,
		byID:        canonicalByID,
	}, nil
}

// Operation returns the durable parent operation.
func (definition *WorkflowDefinition) Operation() Operation {
	if definition == nil {
		return Operation{}
	}
	return definition.operation
}

// Version returns the exact frozen definition version.
func (definition *WorkflowDefinition) Version() string {
	if definition == nil {
		return ""
	}
	return definition.version
}

// Fingerprint returns the canonical definition digest.
func (definition *WorkflowDefinition) Fingerprint() string {
	if definition == nil {
		return ""
	}
	return definition.fingerprint
}

// Nodes returns a deep copy in deterministic topological order.
func (definition *WorkflowDefinition) Nodes() []WorkflowNodeSpec {
	if definition == nil {
		return nil
	}
	return cloneWorkflowSpecs(definition.nodes)
}

func validWorkflowNodeShape(node WorkflowNodeSpec) bool {
	switch node.Kind {
	case WorkflowNodeMachine, WorkflowNodeChild:
		return validOperation(node.Operation.value) && node.Delay == 0 && node.Join == ""
	case WorkflowNodeTimer:
		return node.Operation.value == "" && node.Delay > 0 &&
			node.Delay <= maxWorkflowTimerDelay && node.Join == ""
	case WorkflowNodeJoin:
		return node.Operation.value == "" && node.Delay == 0 &&
			len(node.DependsOn) > 0 && node.Join.valid()
	default:
		return false
	}
}

func topologicalWorkflowNodes(
	nodes []WorkflowNodeSpec,
	byID map[string]int,
) ([]WorkflowNodeSpec, error) {
	indegree := make(map[string]int, len(nodes))
	dependents := make(map[string][]string, len(nodes))
	for _, node := range nodes {
		indegree[node.ID] = len(node.DependsOn)
		for _, dependency := range node.DependsOn {
			dependents[dependency] = append(dependents[dependency], node.ID)
		}
	}
	ready := make([]string, 0, len(nodes))
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	result := make([]WorkflowNodeSpec, 0, len(nodes))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		result = append(result, nodes[byID[id]])
		for _, dependent := range dependents[id] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}
	if len(result) != len(nodes) {
		return nil, ErrWorkflowCycle
	}
	return result, nil
}

func workflowFingerprint(
	operation Operation,
	version string,
	nodes []WorkflowNodeSpec,
) (string, error) {
	canonical := cloneWorkflowSpecs(nodes)
	sort.Slice(canonical, func(left, right int) bool {
		return canonical[left].ID < canonical[right].ID
	})
	for index := range canonical {
		sort.Strings(canonical[index].DependsOn)
	}
	type fingerprintNode struct {
		ID            string                `json:"id"`
		Kind          WorkflowNodeKind      `json:"kind"`
		Operation     string                `json:"operation"`
		DependsOn     []string              `json:"depends_on"`
		Join          WorkflowJoinPolicy    `json:"join"`
		FailurePolicy WorkflowFailurePolicy `json:"failure_policy"`
		DelayNanos    int64                 `json:"delay_nanos"`
	}
	wire := struct {
		Operation string            `json:"operation"`
		Version   string            `json:"version"`
		Nodes     []fingerprintNode `json:"nodes"`
	}{Operation: operation.String(), Version: version, Nodes: make([]fingerprintNode, len(canonical))}
	for index, node := range canonical {
		wire.Nodes[index] = fingerprintNode{
			ID:            node.ID,
			Kind:          node.Kind,
			Operation:     node.Operation.String(),
			DependsOn:     append([]string(nil), node.DependsOn...),
			Join:          node.Join,
			FailurePolicy: node.FailurePolicy,
			DelayNanos:    int64(node.Delay),
		}
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", ErrInvalidWorkflow
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func cloneWorkflowSpecs(source []WorkflowNodeSpec) []WorkflowNodeSpec {
	result := make([]WorkflowNodeSpec, len(source))
	for index, node := range source {
		result[index] = node
		result[index].DependsOn = append([]string(nil), node.DependsOn...)
	}
	return result
}

type workflowNodeState struct {
	ID           string
	Kind         WorkflowNodeKind
	Status       WorkflowNodeStatus
	Attempt      uint32
	ChildCallID  CallID
	ReadyAt      time.Time
	FailureClass string
}

type workflowSnapshotState struct {
	Version     string
	Fingerprint string
	StartedAt   time.Time
	Nodes       map[string]workflowNodeState
}

type workflowWire struct {
	Version     string             `json:"version"`
	Fingerprint string             `json:"fingerprint"`
	StartedAt   string             `json:"started_at"`
	Nodes       []workflowNodeWire `json:"nodes"`
}

type workflowNodeWire struct {
	ID           string             `json:"id"`
	Kind         WorkflowNodeKind   `json:"kind"`
	Status       WorkflowNodeStatus `json:"status"`
	Attempt      uint32             `json:"attempt"`
	ChildCallID  string             `json:"child_call_id"`
	ReadyAt      string             `json:"ready_at"`
	FailureClass string             `json:"failure_class"`
}

func workflowToWire(state *workflowSnapshotState) *workflowWire {
	if state == nil {
		return nil
	}
	ids := make([]string, 0, len(state.Nodes))
	for id := range state.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	wire := &workflowWire{
		Version:     state.Version,
		Fingerprint: state.Fingerprint,
		StartedAt:   formatReadyAt(state.StartedAt),
		Nodes:       make([]workflowNodeWire, len(ids)),
	}
	for index, id := range ids {
		node := state.Nodes[id]
		wire.Nodes[index] = workflowNodeWire{
			ID:           node.ID,
			Kind:         node.Kind,
			Status:       node.Status,
			Attempt:      node.Attempt,
			ChildCallID:  node.ChildCallID.String(),
			ReadyAt:      formatReadyAt(node.ReadyAt),
			FailureClass: node.FailureClass,
		}
	}
	return wire
}

func workflowFromWire(wire *workflowWire) (*workflowSnapshotState, error) {
	if wire == nil {
		return nil, nil
	}
	startedAt, err := time.Parse(time.RFC3339Nano, wire.StartedAt)
	if err != nil || wire.StartedAt != startedAt.UTC().Format(time.RFC3339Nano) {
		return nil, ErrInvalidWorkflow
	}
	state := &workflowSnapshotState{
		Version:     wire.Version,
		Fingerprint: wire.Fingerprint,
		StartedAt:   startedAt.UTC(),
		Nodes:       make(map[string]workflowNodeState, len(wire.Nodes)),
	}
	for _, encoded := range wire.Nodes {
		if _, duplicate := state.Nodes[encoded.ID]; duplicate {
			return nil, ErrInvalidWorkflow
		}
		var childCallID CallID
		if encoded.ChildCallID != "" {
			childCallID, err = NewCallID(encoded.ChildCallID)
			if err != nil {
				return nil, ErrInvalidWorkflow
			}
		}
		var readyAt time.Time
		if encoded.ReadyAt != "" {
			readyAt, err = time.Parse(time.RFC3339Nano, encoded.ReadyAt)
			if err != nil || encoded.ReadyAt != readyAt.UTC().Format(time.RFC3339Nano) {
				return nil, ErrInvalidWorkflow
			}
			readyAt = readyAt.UTC()
		}
		state.Nodes[encoded.ID] = workflowNodeState{
			ID:           encoded.ID,
			Kind:         encoded.Kind,
			Status:       encoded.Status,
			Attempt:      encoded.Attempt,
			ChildCallID:  childCallID,
			ReadyAt:      readyAt,
			FailureClass: encoded.FailureClass,
		}
	}
	if err := validateWorkflowState(state); err != nil {
		return nil, err
	}
	return state, nil
}

func validateWorkflowJSONShape(raw json.RawMessage) error {
	var workflow map[string]json.RawMessage
	if err := json.Unmarshal(raw, &workflow); err != nil ||
		!hasExactFields(workflow, "version", "fingerprint", "started_at", "nodes") ||
		anyJSONNull(
			workflow["version"],
			workflow["fingerprint"],
			workflow["started_at"],
			workflow["nodes"],
		) {
		return ErrInvalidWorkflow
	}
	var nodes []map[string]json.RawMessage
	if err := json.Unmarshal(workflow["nodes"], &nodes); err != nil {
		return ErrInvalidWorkflow
	}
	for _, node := range nodes {
		if !hasExactFields(
			node,
			"id",
			"kind",
			"status",
			"attempt",
			"child_call_id",
			"ready_at",
			"failure_class",
		) || anyJSONNull(
			node["id"],
			node["kind"],
			node["status"],
			node["attempt"],
			node["child_call_id"],
			node["ready_at"],
			node["failure_class"],
		) {
			return ErrInvalidWorkflow
		}
	}
	return nil
}

func newWorkflowSnapshotState(
	definition *WorkflowDefinition,
	startedAt time.Time,
) *workflowSnapshotState {
	state := &workflowSnapshotState{
		Version:     definition.version,
		Fingerprint: definition.fingerprint,
		StartedAt:   normalizeReadyAt(startedAt),
		Nodes:       make(map[string]workflowNodeState, len(definition.nodes)),
	}
	for _, node := range definition.nodes {
		state.Nodes[node.ID] = workflowNodeState{
			ID:     node.ID,
			Kind:   node.Kind,
			Status: WorkflowNodePending,
		}
	}
	return state
}

func cloneWorkflowState(source *workflowSnapshotState) *workflowSnapshotState {
	if source == nil {
		return nil
	}
	result := &workflowSnapshotState{
		Version:     source.Version,
		Fingerprint: source.Fingerprint,
		StartedAt:   source.StartedAt,
		Nodes:       make(map[string]workflowNodeState, len(source.Nodes)),
	}
	for id, node := range source.Nodes {
		result.Nodes[id] = node
	}
	return result
}

func validateWorkflowSuccessor(
	current *workflowSnapshotState,
	next *workflowSnapshotState,
) error {
	if current == nil || validateWorkflowState(current) != nil ||
		validateWorkflowState(next) != nil ||
		current.Version != next.Version ||
		current.Fingerprint != next.Fingerprint ||
		current.StartedAt != next.StartedAt ||
		len(current.Nodes) != len(next.Nodes) {
		return ErrInvalidWorkflow
	}
	for id, previous := range current.Nodes {
		candidate, exists := next.Nodes[id]
		if !exists || candidate.ID != previous.ID ||
			candidate.Kind != previous.Kind ||
			candidate.Attempt < previous.Attempt ||
			candidate.Attempt > previous.Attempt+1 ||
			(previous.ChildCallID.String() != "" &&
				candidate.ChildCallID != previous.ChildCallID) ||
			(!previous.ReadyAt.IsZero() &&
				candidate.ReadyAt != previous.ReadyAt) ||
			!allowedWorkflowNodeTransition(previous.Status, candidate.Status) {
			return ErrInvalidWorkflow
		}
	}
	return nil
}

func allowedWorkflowNodeTransition(
	from WorkflowNodeStatus,
	to WorkflowNodeStatus,
) bool {
	if from.Terminal() {
		return from == to
	}
	switch from {
	case WorkflowNodePending:
		return to == WorkflowNodePending || to == WorkflowNodeRunning ||
			to == WorkflowNodeWaiting || to == WorkflowNodeSucceeded ||
			to == WorkflowNodeFailed || to == WorkflowNodeSkipped
	case WorkflowNodeRunning:
		return to == WorkflowNodeRunning || to == WorkflowNodeWaiting ||
			to == WorkflowNodeSucceeded || to == WorkflowNodeFailed ||
			to == WorkflowNodeSkipped
	case WorkflowNodeWaiting:
		return to == WorkflowNodeWaiting || to == WorkflowNodeSucceeded ||
			to == WorkflowNodeFailed || to == WorkflowNodeSkipped
	default:
		return false
	}
}

func workflowDefinitionOutcome(
	definition *WorkflowDefinition,
	state *workflowSnapshotState,
) (bool, bool) {
	if definition == nil || state == nil {
		return false, false
	}
	dependent := make(map[string]bool, len(definition.nodes))
	for _, node := range definition.nodes {
		for _, dependency := range node.DependsOn {
			dependent[dependency] = true
		}
	}
	failed := false
	sinks := 0
	for _, spec := range definition.nodes {
		if dependent[spec.ID] {
			continue
		}
		sinks++
		node := state.Nodes[spec.ID]
		if !node.Status.Terminal() {
			return false, false
		}
		failed = failed || node.Status == WorkflowNodeFailed
	}
	return sinks > 0, failed
}

func skipNonTerminalWorkflowNodes(state *workflowSnapshotState) {
	for id, node := range state.Nodes {
		if !node.Status.Terminal() {
			node.Status = WorkflowNodeSkipped
			state.Nodes[id] = node
		}
	}
}

func newWorkflowSnapshot(
	callID CallID,
	definition *WorkflowDefinition,
	input []byte,
	startedAt time.Time,
) (Snapshot, error) {
	if definition == nil || startedAt.IsZero() {
		return Snapshot{}, ErrInvalidWorkflow
	}
	snapshot, err := NewSnapshotWithInput(callID, definition.operation, input)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.workflow = newWorkflowSnapshotState(definition, startedAt)
	if err := validateSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func moveWorkflow(
	status Status,
	fence uint64,
	state *workflowSnapshotState,
	readyAt time.Time,
	frames ...Frame,
) Transition {
	transition := Move(status, fence, frames...)
	transition.workflow = cloneWorkflowState(state)
	if status == StatusSuspended && !readyAt.IsZero() {
		transition.readyAt = normalizeReadyAt(readyAt)
		transition.timer = true
	}
	return transition
}

func validateWorkflowState(state *workflowSnapshotState) error {
	if state == nil {
		return nil
	}
	if !validIdentity(state.Version) || len(state.Fingerprint) != sha256.Size*2 ||
		state.StartedAt.IsZero() || state.StartedAt != normalizeReadyAt(state.StartedAt) ||
		len(state.Nodes) == 0 || len(state.Nodes) > maxWorkflowNodes {
		return ErrInvalidWorkflow
	}
	if _, err := hex.DecodeString(state.Fingerprint); err != nil {
		return ErrInvalidWorkflow
	}
	for id, node := range state.Nodes {
		if id != node.ID || !validIdentity(id) || !node.Kind.valid() ||
			!node.Status.valid() || node.Attempt > 1_000_000 ||
			len(node.FailureClass) > maxWorkflowFailureSize ||
			(node.FailureClass != "" && !validIdentity(node.FailureClass)) ||
			(!node.ReadyAt.IsZero() && node.ReadyAt != normalizeReadyAt(node.ReadyAt)) ||
			(node.ChildCallID.String() != "" && !validIdentity(node.ChildCallID.value)) ||
			(node.Kind != WorkflowNodeTimer && !node.ReadyAt.IsZero()) ||
			(node.Kind != WorkflowNodeChild && node.ChildCallID.String() != "") ||
			(node.Status == WorkflowNodeWaiting && node.Kind == WorkflowNodeTimer &&
				node.ReadyAt.IsZero()) ||
			(node.Status == WorkflowNodeWaiting && node.Kind == WorkflowNodeChild &&
				node.ChildCallID.String() == "") ||
			(node.Status != WorkflowNodeFailed && node.FailureClass != "") {
			return ErrInvalidWorkflow
		}
	}
	return nil
}

// WorkflowNodeView is a payload-free immutable node state.
type WorkflowNodeView struct{ state workflowNodeState }

// ID returns the immutable definition node ID.
func (view WorkflowNodeView) ID() string { return view.state.ID }

// Kind returns the immutable definition node kind.
func (view WorkflowNodeView) Kind() WorkflowNodeKind { return view.state.Kind }

// Status returns the durable node lifecycle state.
func (view WorkflowNodeView) Status() WorkflowNodeStatus { return view.state.Status }

// Attempt returns the durable at-least-once attempt number.
func (view WorkflowNodeView) Attempt() uint32 { return view.state.Attempt }

// ChildCallID returns the stable derived child identity when present.
func (view WorkflowNodeView) ChildCallID() CallID { return view.state.ChildCallID }

// ReadyAt returns the absolute timer deadline when present.
func (view WorkflowNodeView) ReadyAt() time.Time { return view.state.ReadyAt }

// FailureClass returns a bounded stable class without raw errors.
func (view WorkflowNodeView) FailureClass() string { return view.state.FailureClass }

// WorkflowView is a payload-free immutable workflow state.
type WorkflowView struct{ state *workflowSnapshotState }

// Version returns the exact immutable definition version.
func (view WorkflowView) Version() string {
	if view.state == nil {
		return ""
	}
	return view.state.Version
}

// Fingerprint returns the canonical definition digest captured at Start.
func (view WorkflowView) Fingerprint() string {
	if view.state == nil {
		return ""
	}
	return view.state.Fingerprint
}

// StartedAt returns the durable workflow origin used for timer deadlines.
func (view WorkflowView) StartedAt() time.Time {
	if view.state == nil {
		return time.Time{}
	}
	return view.state.StartedAt
}

// Node returns one immutable node view by definition ID.
func (view WorkflowView) Node(id string) (WorkflowNodeView, bool) {
	if view.state == nil {
		return WorkflowNodeView{}, false
	}
	node, exists := view.state.Nodes[id]
	return WorkflowNodeView{state: node}, exists
}

// Nodes returns immutable node views in ID order.
func (view WorkflowView) Nodes() []WorkflowNodeView {
	if view.state == nil {
		return nil
	}
	ids := make([]string, 0, len(view.state.Nodes))
	for id := range view.state.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]WorkflowNodeView, len(ids))
	for index, id := range ids {
		result[index] = WorkflowNodeView{state: view.state.Nodes[id]}
	}
	return result
}

// Workflow returns payload-free durable workflow state when this is a parent.
func (snapshot Snapshot) Workflow() (WorkflowView, bool) {
	if snapshot.workflow == nil {
		return WorkflowView{}, false
	}
	return WorkflowView{state: cloneWorkflowState(snapshot.workflow)}, true
}

// EvaluateWorkflowJoin deterministically evaluates ALL/ANY dependency state.
func EvaluateWorkflowJoin(
	policy WorkflowJoinPolicy,
	states []WorkflowNodeStatus,
) (WorkflowNodeStatus, bool) {
	if !policy.valid() || len(states) == 0 {
		return WorkflowNodeFailed, true
	}
	succeeded := 0
	failed := 0
	terminal := 0
	for _, status := range states {
		if status == WorkflowNodeSucceeded {
			succeeded++
		}
		if status == WorkflowNodeFailed || status == WorkflowNodeSkipped {
			failed++
		}
		if status.Terminal() {
			terminal++
		}
	}
	switch policy {
	case WorkflowJoinAll:
		if failed > 0 {
			return WorkflowNodeFailed, true
		}
		if succeeded == len(states) {
			return WorkflowNodeSucceeded, true
		}
	case WorkflowJoinAny:
		if succeeded > 0 {
			return WorkflowNodeSucceeded, true
		}
		if terminal == len(states) {
			return WorkflowNodeFailed, true
		}
	}
	return WorkflowNodePending, false
}

type workflowCommand struct {
	nodeID         string
	kind           WorkflowNodeKind
	operation      Operation
	attempt        uint32
	childCallID    CallID
	readyAt        time.Time
	completeStatus WorkflowNodeStatus
}

func planWorkflowForParent(
	definition *WorkflowDefinition,
	state *workflowSnapshotState,
	parent CallID,
) ([]workflowCommand, error) {
	if definition == nil || validateWorkflowState(state) != nil ||
		state.Version != definition.version ||
		state.Fingerprint != definition.fingerprint ||
		len(state.Nodes) != len(definition.nodes) {
		return nil, ErrWorkflowDefinitionMismatch
	}
	commands := make([]workflowCommand, 0)
	for _, spec := range definition.nodes {
		node, exists := state.Nodes[spec.ID]
		if !exists || node.Kind != spec.Kind {
			return nil, ErrWorkflowDefinitionMismatch
		}
		if node.Status != WorkflowNodePending {
			continue
		}
		dependencies := make([]WorkflowNodeStatus, len(spec.DependsOn))
		allSucceeded := true
		allTerminal := true
		failedDependency := false
		for index, id := range spec.DependsOn {
			dependency := state.Nodes[id]
			dependencies[index] = dependency.Status
			allSucceeded = allSucceeded && dependency.Status == WorkflowNodeSucceeded
			allTerminal = allTerminal && dependency.Status.Terminal()
			failedDependency = failedDependency || dependency.Status == WorkflowNodeFailed
		}
		if spec.Kind == WorkflowNodeJoin {
			status, terminal := EvaluateWorkflowJoin(spec.Join, dependencies)
			if terminal {
				commands = append(commands, workflowCommand{
					nodeID: spec.ID, kind: spec.Kind, completeStatus: status,
				})
			}
			continue
		}
		if failedDependency && spec.FailurePolicy == WorkflowFailFast {
			commands = append(commands, workflowCommand{
				nodeID: spec.ID, kind: spec.Kind, completeStatus: WorkflowNodeFailed,
			})
			continue
		}
		if len(spec.DependsOn) > 0 && !allSucceeded {
			if spec.FailurePolicy != WorkflowContinue || !allTerminal {
				continue
			}
		}
		attempt := node.Attempt + 1
		command := workflowCommand{
			nodeID: spec.ID, kind: spec.Kind, operation: spec.Operation, attempt: attempt,
		}
		switch spec.Kind {
		case WorkflowNodeTimer:
			command.readyAt = normalizeReadyAt(state.StartedAt.Add(spec.Delay))
		case WorkflowNodeChild:
			var err error
			command.childCallID, err = DeriveChildCallID(
				parent, spec.ID, attempt,
			)
			if err != nil {
				return nil, err
			}
		}
		commands = append(commands, command)
	}
	return commands, nil
}

// DeriveChildCallID returns a stable parent/node/attempt-scoped identity.
func DeriveChildCallID(parent CallID, nodeID string, attempt uint32) (CallID, error) {
	if !validIdentity(parent.value) || !validIdentity(nodeID) || attempt == 0 {
		return CallID{}, ErrInvalidWorkflow
	}
	hasher := sha256.New()
	writeDigestPart(hasher, []byte("keelith-workflow-child-v1"))
	writeDigestPart(hasher, []byte(parent.value))
	writeDigestPart(hasher, []byte(nodeID))
	var encodedAttempt [4]byte
	binary.BigEndian.PutUint32(encodedAttempt[:], attempt)
	writeDigestPart(hasher, encodedAttempt[:])
	return NewCallID("wf-child-" + hex.EncodeToString(hasher.Sum(nil)))
}
