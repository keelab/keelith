// Package server defines the lifecycle contract implemented by Keelith
// transports, workers, jobs, and operational endpoints.
package server

import "context"

// Server is a synchronously started, gracefully stopped runtime component.
//
// Start must return only after the component can accept work. Stop must be
// idempotent.
type Server interface {
	// Start starts the server and returns only after it can accept work.
	Start(context.Context) error
	// Stop stops the server and returns only after it has terminated.
	Stop(context.Context) error
}

// Waiter reports when a started Server terminates.
//
// Waiter is optional so simple lifecycle components only need to implement
// Server. Stop must cause an implementation's Wait method to return.
type Waiter interface {
	// Wait waits for the server to terminate and returns only after it has
	// terminated.
	Wait() error
}

// Named gives a Server a stable diagnostic name.
type Named interface {
	// Name returns the server's diagnostic name.
	Name() string
}
