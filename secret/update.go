package secret

// UpdateSubscription observes successful material replacement generations.
// It never exposes secret content, provider revisions, references, or errors.
type UpdateSubscription interface {
	Baseline() uint64
	Updates() <-chan uint64
	Close()
}

// UpdateSource is implemented by validated, last-good credential holders that
// can notify connection owners after a successful atomic replacement.
type UpdateSource interface {
	Ready() bool
	SubscribeUpdates() (UpdateSubscription, error)
}
