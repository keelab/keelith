package continuation

// commitRequest constructs one executor-owned commit and atomically schedules
// terminal retention when the target state is terminal.
func (r *Runtime) commitRequest(
	current Snapshot,
	next Snapshot,
) CommitRequest {
	request := CommitRequest{
		ExpectedRevision: current.Revision(),
		Fence:            current.Fence(),
		LeaseOwner:       r.executorID,
		Snapshot:         next,
	}
	if next.Status().Terminal() {
		request.ExpiresAt = r.now().UTC().Add(
			r.terminalRetention,
		)
	}
	return request
}
