// Package outlier provides passive instance health detection for selectors.
package outlier

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/selector"
)

var (
	// ErrInvalidConfig means an outlier detector policy is unusable.
	ErrInvalidConfig = errors.New("outlier: invalid config")
)

// Clock supplies deterministic detector time.
type Clock interface {
	Now() time.Time
}

// Classifier decides whether one completed call is an instance failure.
type Classifier func(selector.Result) bool

// Config controls passive consecutive-failure ejection.
type Config struct {
	ConsecutiveFailures int
	EjectionTime        time.Duration
	Clock               Clock
	Classifier          Classifier
}

// Status is an immutable, low-cardinality node health snapshot.
type Status struct {
	Service             string
	NodeID              string
	Endpoint            string
	ConsecutiveFailures int
	EjectedUntil        time.Time
	Ejected             bool
}

type nodeState struct {
	service      string
	nodeID       string
	endpoint     string
	consecutive  int
	ejectedUntil time.Time
}

// Detector implements selector.Observer using passive invocation results.
//
// Once the ejection deadline expires, the node becomes eligible for a
// probation request. A successful result fully restores it; another instance
// failure immediately ejects it again.
type Detector struct {
	threshold    int
	ejectionTime time.Duration
	clock        Clock
	classify     Classifier

	mu    sync.Mutex
	nodes map[string]*nodeState
}

// New constructs a passive outlier detector.
func New(config Config) (*Detector, error) {
	if config.ConsecutiveFailures <= 0 {
		return nil, fmt.Errorf(
			"%w: consecutive failures must be positive",
			ErrInvalidConfig,
		)
	}
	if config.EjectionTime <= 0 {
		return nil, fmt.Errorf(
			"%w: ejection time must be positive",
			ErrInvalidConfig,
		)
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	classify := config.Classifier
	if classify == nil {
		classify = defaultClassifier
	}
	return &Detector{
		threshold:    config.ConsecutiveFailures,
		ejectionTime: config.EjectionTime,
		clock:        clock,
		classify:     classify,
		nodes:        make(map[string]*nodeState),
	}, nil
}

// Allow reports whether a node is currently eligible.
func (d *Detector) Allow(ctx context.Context, _ operation.Operation, node selector.Node) bool {
	if d == nil || context.Cause(ctx) != nil {
		return false
	}
	now := d.clock.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	state := d.state(node)
	return state.ejectedUntil.IsZero() || !now.Before(state.ejectedUntil)
}

// Done records one invocation result.
func (d *Detector) Done(_ operation.Operation, node selector.Node, result selector.Result) {
	if d == nil {
		return
	}
	now := d.clock.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	state := d.state(node)

	if result.Canceled {
		return
	}
	if !d.classify(result) {
		state.consecutive = 0
		state.ejectedUntil = time.Time{}
		return
	}

	// A failure during post-ejection probation must immediately re-eject the
	// node instead of requiring another full threshold.
	if !state.ejectedUntil.IsZero() && !now.Before(state.ejectedUntil) {
		state.consecutive = d.threshold
		state.ejectedUntil = now.Add(d.ejectionTime)
		return
	}
	state.consecutive++
	if state.consecutive >= d.threshold {
		state.ejectedUntil = now.Add(d.ejectionTime)
	}
}

// Status returns deterministic diagnostic state.
func (d *Detector) Status() []Status {
	if d == nil {
		return nil
	}
	now := d.clock.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]Status, 0, len(d.nodes))
	for _, state := range d.nodes {
		result = append(result, Status{
			Service:             state.service,
			NodeID:              state.nodeID,
			Endpoint:            state.endpoint,
			ConsecutiveFailures: state.consecutive,
			EjectedUntil:        state.ejectedUntil,
			Ejected:             !state.ejectedUntil.IsZero() && now.Before(state.ejectedUntil),
		})
	}
	sort.Slice(result, func(first, second int) bool {
		if result[first].Service != result[second].Service {
			return result[first].Service < result[second].Service
		}
		if result[first].NodeID == result[second].NodeID {
			return result[first].Endpoint < result[second].Endpoint
		}
		return result[first].NodeID < result[second].NodeID
	})
	return result
}

// Reset forgets all accumulated node health state.
func (d *Detector) Reset() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.nodes = make(map[string]*nodeState)
	d.mu.Unlock()
}

func (d *Detector) state(node selector.Node) *nodeState {
	key := node.Service() + "\x00" + node.ID() + "\x00" + node.Endpoint()
	state := d.nodes[key]
	if state == nil {
		state = &nodeState{
			service:  node.Service(),
			nodeID:   node.ID(),
			endpoint: node.Endpoint(),
		}
		d.nodes[key] = state
	}
	return state
}

func defaultClassifier(result selector.Result) bool {
	switch failure.Classify(result.Error) {
	case failure.Transport, failure.Timeout:
		return true
	default:
		return false
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}
