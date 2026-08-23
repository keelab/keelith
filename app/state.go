package app

import "fmt"

// State is the App lifecycle phase.
type State uint8

const (
	// StateNew means Run has not been called.
	StateNew State = iota
	// StateStarting means hooks and servers are starting.
	StateStarting
	// StateReady means startup completed and servers are running.
	StateReady
	// StateDraining means shutdown hooks and servers are stopping.
	StateDraining
	// StateStopped means Run returned without failure.
	StateStopped
	// StateFailed means Run returned an error.
	StateFailed
)

func (state State) String() string {
	switch state {
	case StateNew:
		return "new"
	case StateStarting:
		return "starting"
	case StateReady:
		return "ready"
	case StateDraining:
		return "draining"
	case StateStopped:
		return "stopped"
	case StateFailed:
		return "failed"
	default:
		return fmt.Sprintf("State(%d)", state)
	}
}

// Description is a compact App lifecycle snapshot.
type Description struct {
	State    State
	Terminal bool
	Failed   bool
}
