package continuation

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxLeaseOwnerBytes = 256
	maxLeaseDuration   = 24 * time.Hour
)

var (
	// ErrLeaseHeld reports a non-expired claim owned by another executor.
	ErrLeaseHeld = errors.New("continuation: execution lease held")
	// ErrLeaseLost reports an expired, replaced, or mismatched execution lease.
	ErrLeaseLost = errors.New("continuation: execution lease lost")
	// ErrLeaseUnsupported reports a Store that cannot safely run executors.
	ErrLeaseUnsupported = errors.New(
		"continuation: store does not support execution leases",
	)
)

// ClaimRequest atomically acquires one ready revision for an executor.
type ClaimRequest struct {
	CallID           CallID
	ExpectedRevision uint64
	OwnerID          string
	LeaseDuration    time.Duration
}

// Validate checks stable identity and bounded lease duration.
func (request ClaimRequest) Validate() error {
	if !validIdentity(request.CallID.value) ||
		request.ExpectedRevision == 0 ||
		!validLeaseOwner(request.OwnerID) ||
		request.LeaseDuration <= 0 ||
		request.LeaseDuration > maxLeaseDuration {
		return ErrInvalidStore
	}
	return nil
}

// Lease is one fence-owned execution claim.
type Lease struct {
	Snapshot Snapshot
	OwnerID  string
	Deadline time.Time
}

// LeaseRequest identifies an existing claim for renew or release.
type LeaseRequest struct {
	CallID        CallID
	Revision      uint64
	Fence         uint64
	OwnerID       string
	LeaseDuration time.Duration
}

// Validate checks claim identity. Renew requests additionally require duration.
func (request LeaseRequest) Validate(renew bool) error {
	if !validIdentity(request.CallID.value) ||
		request.Revision == 0 ||
		request.Fence == 0 ||
		!validLeaseOwner(request.OwnerID) {
		return ErrInvalidStore
	}
	if renew &&
		(request.LeaseDuration <= 0 ||
			request.LeaseDuration > maxLeaseDuration) {
		return ErrInvalidStore
	}
	return nil
}

// LeaseStore protects Machine.Advance ownership across executors.
//
// Claim must hide the owned RUNNING revision from ListReady until Deadline.
// Renew does not change the business revision. Release makes an uncommitted
// revision immediately reclaimable. Transition must validate CommitRequest's
// LeaseOwner whenever it is non-empty.
type LeaseStore interface {
	Store
	Claim(context.Context, ClaimRequest) (Lease, error)
	Renew(context.Context, LeaseRequest) (Lease, error)
	Release(context.Context, LeaseRequest) error
}

func validLeaseOwner(owner string) bool {
	if owner == "" ||
		len(owner) > maxLeaseOwnerBytes ||
		!utf8.ValidString(owner) ||
		strings.TrimSpace(owner) != owner {
		return false
	}
	for _, character := range owner {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
