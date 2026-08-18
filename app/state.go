package app

import "fmt"

// State represents the state of the application.
type State uint8

const (
	StateNew      State = iota // the application is new and not yet started
	StateStarting              // the application is starting
	StateReady                 // the application is ready to serve requests
	StateDraining              // the application is draining
	StateStopped               // the application is stopped
	StateFailed                // the application has failed
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
