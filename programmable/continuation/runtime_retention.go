package continuation

// commitRequest constructs one executor-owned commit and atomically schedules
// terminal retention when the target state is terminal.
func (runtime *Runtime) commitRequest(
	current Snapshot,
	next Snapshot,
) CommitRequest {
	request := CommitRequest{
		ExpectedRevision: current.Revision(),
		Fence:            current.Fence(),
		LeaseOwner:       runtime.executorID,
		Snapshot:         next,
	}
	if next.Status().Terminal() {
		request.ExpiresAt = runtime.now().UTC().Add(
			runtime.terminalRetention,
		)
	}
	return request
}
