// Package coordination defines small distributed ownership contracts:
// try to acquire an auto-maintained lease for a stable key, observe loss via
// Done/Err, and carry a fencing token on Handler contexts.
//
// The coordination/memory subpackage is a process-local stand-in for
// development and contract tests. It simulates mutual exclusion inside one
// process only; it is not a cross-process or cluster-wide lease service, does
// not renew or expire leases by TTL, and must not be used as production
// distributed ownership.
package coordination
