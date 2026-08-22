package selector

import (
	"maps"
	"time"

	"github.com/keelab/keelith/registry"
)

// Node is an immutable selectable endpoint.
type Node struct {
	id       string
	service  string
	endpoint string
	metadata map[string]string
}

// ID returns the source instance ID.
func (n Node) ID() string {
	return n.id
}

// Service returns the logical service.
func (n Node) Service() string {
	return n.service
}

// Endpoint returns the selected standard endpoint URL.
func (n Node) Endpoint() string {
	return n.endpoint
}

// Metadata returns an independent metadata map.
func (n Node) Metadata() map[string]string {
	clone := make(map[string]string, len(n.metadata))
	maps.Copy(clone, n.metadata)
	return clone
}

func (n Node) key() string {
	return n.id + "\x00" + n.endpoint
}

func newNode(instance registry.Instance, endpoint string) Node {
	return Node{
		id:       instance.ID(),
		service:  instance.Service(),
		endpoint: endpoint,
		metadata: instance.Metadata(),
	}
}

// NodeStats is an immutable diagnostic P2C state snapshot.
type NodeStats struct {
	Node           Node
	EWMALatency    time.Duration
	Inflight       int64
	FailurePenalty float64
}
