package continuationgrpc

import (
	"encoding/hex"
	"time"

	continuationv1 "github.com/keelab/keelith/api/continuation/v1"
	"github.com/keelab/keelith/programmable/continuation"
)

const maxWireWorkflowNodes = 10_000

func workflowToWire(
	workflow continuation.WorkflowView,
) (*continuationv1.Workflow, error) {
	nodes := workflow.Nodes()
	if workflow.Version() == "" || workflow.Fingerprint() == "" ||
		workflow.StartedAt().IsZero() || len(nodes) == 0 ||
		len(nodes) > maxWireWorkflowNodes {
		return nil, ErrInvalidWireMessage
	}
	encoded := &continuationv1.Workflow{
		Version:     workflow.Version(),
		Fingerprint: workflow.Fingerprint(),
		StartedAt:   workflow.StartedAt().UTC().Format(time.RFC3339Nano),
		Nodes:       make([]*continuationv1.WorkflowNode, len(nodes)),
	}
	for index, node := range nodes {
		kind, err := workflowNodeKindToWire(node.Kind())
		if err != nil {
			return nil, err
		}
		status, err := workflowNodeStatusToWire(node.Status())
		if err != nil {
			return nil, err
		}
		encoded.Nodes[index] = &continuationv1.WorkflowNode{
			Id:           node.ID(),
			Kind:         kind,
			Status:       status,
			Attempt:      node.Attempt(),
			ChildCallId:  node.ChildCallID().String(),
			FailureClass: node.FailureClass(),
		}
		if !node.ReadyAt().IsZero() {
			encoded.Nodes[index].ReadyAt = node.ReadyAt().UTC().Format(time.RFC3339Nano)
		}
	}
	return encoded, nil
}

func validateWorkflowWire(workflow *continuationv1.Workflow) error {
	if workflow == nil || workflow.GetVersion() == "" ||
		len(workflow.GetFingerprint()) != 64 || workflow.GetStartedAt() == "" ||
		len(workflow.GetNodes()) == 0 || len(workflow.GetNodes()) > maxWireWorkflowNodes {
		return ErrInvalidWireMessage
	}
	if _, err := hex.DecodeString(workflow.GetFingerprint()); err != nil {
		return ErrInvalidWireMessage
	}
	startedAt, err := time.Parse(time.RFC3339Nano, workflow.GetStartedAt())
	if err != nil || workflow.GetStartedAt() != startedAt.UTC().Format(time.RFC3339Nano) {
		return ErrInvalidWireMessage
	}
	seen := make(map[string]struct{}, len(workflow.GetNodes()))
	for _, node := range workflow.GetNodes() {
		if node == nil || node.GetId() == "" {
			return ErrInvalidWireMessage
		}
		if _, duplicate := seen[node.GetId()]; duplicate {
			return ErrInvalidWireMessage
		}
		seen[node.GetId()] = struct{}{}
		if _, err := workflowNodeKindFromWire(node.GetKind()); err != nil {
			return err
		}
		if _, err := workflowNodeStatusFromWire(node.GetStatus()); err != nil {
			return err
		}
		if node.GetChildCallId() != "" {
			if _, err := callIDFromWire(node.GetChildCallId()); err != nil {
				return err
			}
		}
		if node.GetReadyAt() != "" {
			readyAt, parseErr := time.Parse(time.RFC3339Nano, node.GetReadyAt())
			if parseErr != nil || node.GetReadyAt() != readyAt.UTC().Format(time.RFC3339Nano) {
				return ErrInvalidWireMessage
			}
		}
	}
	return nil
}

func workflowNodeKindToWire(kind continuation.WorkflowNodeKind) (continuationv1.WorkflowNodeKind, error) {
	switch kind {
	case continuation.WorkflowNodeMachine:
		return continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_MACHINE, nil
	case continuation.WorkflowNodeTimer:
		return continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_TIMER, nil
	case continuation.WorkflowNodeChild:
		return continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_CHILD, nil
	case continuation.WorkflowNodeJoin:
		return continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_JOIN, nil
	default:
		return 0, ErrInvalidWireMessage
	}
}

func workflowNodeKindFromWire(kind continuationv1.WorkflowNodeKind) (continuation.WorkflowNodeKind, error) {
	switch kind {
	case continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_MACHINE:
		return continuation.WorkflowNodeMachine, nil
	case continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_TIMER:
		return continuation.WorkflowNodeTimer, nil
	case continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_CHILD:
		return continuation.WorkflowNodeChild, nil
	case continuationv1.WorkflowNodeKind_WORKFLOW_NODE_KIND_JOIN:
		return continuation.WorkflowNodeJoin, nil
	default:
		return "", ErrInvalidWireMessage
	}
}

func workflowNodeStatusToWire(status continuation.WorkflowNodeStatus) (continuationv1.WorkflowNodeStatus, error) {
	switch status {
	case continuation.WorkflowNodePending:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_PENDING, nil
	case continuation.WorkflowNodeRunning:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_RUNNING, nil
	case continuation.WorkflowNodeWaiting:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_WAITING, nil
	case continuation.WorkflowNodeSucceeded:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_SUCCEEDED, nil
	case continuation.WorkflowNodeFailed:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_FAILED, nil
	case continuation.WorkflowNodeSkipped:
		return continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_SKIPPED, nil
	default:
		return 0, ErrInvalidWireMessage
	}
}

func workflowNodeStatusFromWire(status continuationv1.WorkflowNodeStatus) (continuation.WorkflowNodeStatus, error) {
	switch status {
	case continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_PENDING:
		return continuation.WorkflowNodePending, nil
	case continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_RUNNING:
		return continuation.WorkflowNodeRunning, nil
	case continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_WAITING:
		return continuation.WorkflowNodeWaiting, nil
	case continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_SUCCEEDED:
		return continuation.WorkflowNodeSucceeded, nil
	case continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_FAILED:
		return continuation.WorkflowNodeFailed, nil
	case continuationv1.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_SKIPPED:
		return continuation.WorkflowNodeSkipped, nil
	default:
		return "", ErrInvalidWireMessage
	}
}
