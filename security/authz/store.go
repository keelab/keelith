package authz

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/security"
)

// Definition is one complete revisioned RBAC rule set.
type Definition struct {
	Revision string
	Rules    []Rule
}

type policySnapshot struct {
	revision string
	rbac     *RBAC
	rules    int
}

// StoreDescription is a value-free dynamic authorization snapshot.
type StoreDescription struct {
	Revision    string
	Rules       int
	Updates     uint64
	Evaluations uint64
	Allowed     uint64
	Denied      uint64
}

// Store atomically publishes complete immutable RBAC revisions and implements
// Authorizer without locking the request path.
type Store struct {
	updateMu sync.Mutex
	current  atomic.Pointer[policySnapshot]

	updates     atomic.Uint64
	evaluations atomic.Uint64
	allowed     atomic.Uint64
	denied      atomic.Uint64
}

// NewStore validates and publishes the initial definition.
func NewStore(definition Definition) (*Store, error) {
	snapshot, err := compileDefinition(definition)
	if err != nil {
		return nil, err
	}
	s := &Store{}
	s.current.Store(&snapshot)
	return s, nil
}

// Update atomically publishes a new definition. The same revision is ignored.
func (s *Store) Update(definition Definition) (bool, error) {
	if s == nil {
		return false, ErrInvalidAuthorizer
	}
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	current := s.current.Load()
	if current != nil && current.revision == definition.Revision {
		return false, nil
	}
	next, err := compileDefinition(definition)
	if err != nil {
		return false, err
	}
	s.current.Store(&next)
	s.updates.Add(1)
	return true, nil
}

// Authorize evaluates the current immutable policy.
func (s *Store) Authorize(
	ctx context.Context,
	principal security.Principal,
	target operation.Operation,
	request any,
) (Decision, error) {
	if ctx == nil || s == nil {
		return Decision{}, ErrInvalidAuthorizer
	}
	if cause := context.Cause(ctx); cause != nil {
		return Decision{}, cause
	}
	current := s.current.Load()
	if current == nil || current.rbac == nil {
		return Decision{}, ErrInvalidAuthorizer
	}
	s.evaluations.Add(1)
	decision, err := current.rbac.Authorize(
		ctx,
		principal,
		target,
		request,
	)
	if err != nil {
		return Decision{}, err
	}
	if decision.Allowed {
		s.allowed.Add(1)
	} else {
		s.denied.Add(1)
	}
	return decision, nil
}

// Describe returns aggregate rule and decision counters without operation,
// principal, role, scope, or claim values.
func (s *Store) Describe() StoreDescription {
	if s == nil {
		return StoreDescription{}
	}
	current := s.current.Load()
	description := StoreDescription{
		Updates:     s.updates.Load(),
		Evaluations: s.evaluations.Load(),
		Allowed:     s.allowed.Load(),
		Denied:      s.denied.Load(),
	}
	if current != nil {
		description.Revision = current.revision
		description.Rules = current.rules
	}
	return description
}

func compileDefinition(definition Definition) (policySnapshot, error) {
	if !validRevision(definition.Revision) {
		return policySnapshot{}, fmt.Errorf(
			"%w: revision is malformed",
			ErrInvalidAuthorizer,
		)
	}
	rbac, err := NewRBAC(definition.Rules...)
	if err != nil {
		return policySnapshot{}, err
	}
	return policySnapshot{
		revision: definition.Revision,
		rbac:     rbac,
		rules:    len(definition.Rules),
	}, nil
}

func validRevision(value string) bool {
	if value == "" ||
		len(value) > 256 ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
