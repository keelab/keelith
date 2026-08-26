package logging

import "log/slog"

// Dependencies is the stable wiring output for application constructors.
// It keeps the caller-aware logger and policy ownership together in the
// dependency manifest.
type Dependencies struct {
	Logger     *Logger
	Controller *Controller
}

// ProvideLogger exposes the caller-aware logger from a wiring dependency set.
func ProvideLogger(dependencies Dependencies) *Logger { return dependencies.Logger }

// ProvideSlog exposes the underlying standard logger for integrations that
// have not adopted the Keelith facade.
func ProvideSlog(dependencies Dependencies) *slog.Logger {
	if dependencies.Logger == nil {
		return nil
	}
	return dependencies.Logger.Slog()
}

// ProvideController exposes the dynamic policy controller to wiring.
func ProvideController(dependencies Dependencies) *Controller { return dependencies.Controller }

// NewDependencies validates one wiring-ready dependency set.
func NewDependencies(logger *Logger, controller *Controller) (Dependencies, error) {
	if logger == nil || controller == nil {
		return Dependencies{}, ErrInvalidOption
	}
	return Dependencies{Logger: logger, Controller: controller}, nil
}
