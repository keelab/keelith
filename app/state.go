package app

import "fmt"

// State represents the state of the application.
type State uint8

const (
	// StateNew means the application has not started.
	StateNew State = iota
	// StateStarting means the application is starting.
	StateStarting
	// StateReady means the application is ready to serve requests.
	StateReady
	// StateDraining means the application is draining.
	StateDraining
	// StateStopped means the application has stopped.
	StateStopped
	// StateFailed means the application has failed.
	StateFailed
)

// String returns the string representation of the state.
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

// Description holds the state description of the application.
type Description struct {
	State    State // the current state of the application
	Terminal bool  // whether the state is terminal
	Failed   bool  // whether the state is failed
}
