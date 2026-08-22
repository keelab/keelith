package logging

import "log/slog"

// Dependencies is the stable wiring output for application constructors.
// It keeps slog as the public logging API while making policy ownership
// explicit in dependency manifests.
type Dependencies struct {
	Logger     *slog.Logger
	Controller *Controller
}

// ProvideLogger exposes the standard slog API from a wiring dependency set.
func ProvideLogger(dependencies Dependencies) *slog.Logger { return dependencies.Logger }

// ProvideController exposes the dynamic policy controller to wiring.
func ProvideController(dependencies Dependencies) *Controller { return dependencies.Controller }

// NewDependencies validates one wiring-ready dependency set.
func NewDependencies(logger *slog.Logger, controller *Controller) (Dependencies, error) {
	if logger == nil || controller == nil {
		return Dependencies{}, ErrInvalidOption
	}
	return Dependencies{Logger: logger, Controller: controller}, nil
}
