package errors

// Frame is the transport-neutral, serializable projection of an Error.
//
// Cause is deliberately absent because implementation errors must not cross a
// transport boundary.
type Frame struct {
	Code     int32
	Reason   string
	Message  string
	Metadata map[string]string
}

// ToFrame creates an independent wire projection of target.
func ToFrame(target *Error) Frame {
	if target == nil {
		return Frame{}
	}
	return Frame{
		Code:     target.Code(),
		Reason:   target.Reason(),
		Message:  target.Message(),
		Metadata: target.Metadata(),
	}
}

// FromFrame creates an Error without a private Cause.
func FromFrame(frame Frame) *Error {
	return New(
		frame.Code,
		frame.Reason,
		frame.Message,
		WithMetadata(frame.Metadata),
	)
}
