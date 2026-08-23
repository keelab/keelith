package continuation

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/security"
	"github.com/keelab/keelith/security/authz"
)

const continuationAuthorizationService = "keelith.continuation.v1.ContinuationService"

// ServiceRuntime is the transport-neutral durable continuation data plane.
type ServiceRuntime interface {
	StartCall(context.Context, CallID, Operation, []byte) (Snapshot, error)
	Attach(context.Context, CallID, uint64, int) (Attachment, error)
	SubmitSignal(context.Context, CallID, string, []byte) (Snapshot, error)
	RequestCancel(context.Context, CallID, string) (Snapshot, error)
}

// WorkflowServiceRuntime is the transport-neutral workflow creation surface.
// Live attachment continues through ServiceRuntime so ordinary calls and
// workflow parents share one authorization and cursor protocol.
type WorkflowServiceRuntime interface {
	Start(context.Context, CallID, Operation, string, []byte) (Snapshot, error)
}

type inlineServiceRuntime interface {
	StartCallInline(
		context.Context,
		CallID,
		Operation,
		[]byte,
		InlineBudget,
	) (Snapshot, error)
}

// Ownership is an immutable CallID-to-principal access binding kept outside
// payload-bearing continuation snapshots.
type Ownership struct {
	callID    CallID
	operation Operation
	issuer    string
	subject   string
}

// NewOwnership snapshots one authenticated call owner.
func NewOwnership(
	callID CallID,
	durableOperation Operation,
	principal security.Principal,
) (Ownership, error) {
	if callID.String() == "" || durableOperation.String() == "" ||
		principal.Validate(time.Time{}) != nil {
		return Ownership{}, ErrInvalidService
	}
	return Ownership{
		callID:    callID,
		operation: durableOperation,
		issuer:    principal.Issuer(),
		subject:   principal.Subject(),
	}, nil
}

// CallID returns the bound durable identity.
func (ownership Ownership) CallID() CallID { return ownership.callID }

// Operation returns the bound durable operation.
func (ownership Ownership) Operation() Operation { return ownership.operation }

// Matches reports whether principal is the issuer-qualified owner.
func (ownership Ownership) Matches(principal security.Principal) bool {
	return ownership.issuer == principal.Issuer() &&
		ownership.subject == principal.Subject()
}

// Equal reports exact resource and owner equality without exposing identity.
func (ownership Ownership) Equal(other Ownership) bool {
	return ownership.callID == other.callID &&
		ownership.operation == other.operation &&
		ownership.issuer == other.issuer &&
		ownership.subject == other.subject
}

// Valid reports whether ownership was constructed with bounded identities.
func (ownership Ownership) Valid() bool {
	return ownership.callID.String() != "" &&
		ownership.operation.String() != "" &&
		ownership.subject != ""
}

// OwnershipStore durably binds a CallID to its authenticated creator.
type OwnershipStore interface {
	Bind(context.Context, Ownership) error
	Load(context.Context, CallID) (Ownership, error)
}

// ServiceConfig configures resource authorization around one Runtime.
type ServiceConfig struct {
	Runtime      ServiceRuntime
	Workflow     WorkflowServiceRuntime
	PublicAccess bool
	Authorizer   authz.Authorizer
	Ownerships   OwnershipStore
	Clock        func() time.Time
}

// Service centralizes authorization before any continuation Runtime access.
type Service struct {
	runtime      ServiceRuntime
	workflow     WorkflowServiceRuntime
	publicAccess bool
	authorizer   authz.Authorizer
	ownerships   OwnershipStore
	now          func() time.Time
}

// NewService validates and snapshots one access policy. Omitting both public
// access and a complete Authorizer/OwnershipStore pair creates a deny-all
// service.
func NewService(config ServiceConfig) (*Service, error) {
	if isNilServiceValue(config.Runtime) ||
		config.PublicAccess &&
			(!isNilServiceValue(config.Authorizer) ||
				!isNilServiceValue(config.Ownerships)) ||
		!config.PublicAccess &&
			(isNilServiceValue(config.Authorizer) !=
				isNilServiceValue(config.Ownerships)) {
		return nil, ErrInvalidService
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	return &Service{
		runtime:      config.Runtime,
		workflow:     config.Workflow,
		publicAccess: config.PublicAccess,
		authorizer:   config.Authorizer,
		ownerships:   config.Ownerships,
		now:          config.Clock,
	}, nil
}

// StartWorkflow authorizes, binds ownership and persists one exact workflow
// definition version. Omitting a workflow runtime keeps legacy services valid
// while failing explicit workflow requests closed.
func (service *Service) StartWorkflow(
	ctx context.Context,
	callID CallID,
	durableOperation Operation,
	version string,
	input []byte,
) (Snapshot, error) {
	if service == nil || ctx == nil || isNilServiceValue(service.workflow) {
		return Snapshot{}, ErrWorkflowDefinitionMismatch
	}
	if service.publicAccess {
		return service.workflow.Start(
			ctx,
			callID,
			durableOperation,
			version,
			input,
		)
	}
	principal, err := service.authorize(ctx, AuthorizationRequest{
		CallID: callID, Operation: durableOperation, Action: ActionStart,
	})
	if err != nil {
		return Snapshot{}, err
	}
	ownership, err := NewOwnership(callID, durableOperation, principal)
	if err != nil {
		return Snapshot{}, err
	}
	if err := service.ownerships.Bind(ctx, ownership); err != nil {
		if errors.Is(err, ErrOwnershipConflict) {
			return Snapshot{}, ErrAccessDenied
		}
		return Snapshot{}, err
	}
	return service.workflow.Start(
		ctx,
		callID,
		durableOperation,
		version,
		input,
	)
}

// StartCall authorizes, binds ownership and persists a new call.
func (service *Service) StartCall(
	ctx context.Context,
	callID CallID,
	durableOperation Operation,
	input []byte,
) (Snapshot, error) {
	return service.startCall(
		ctx,
		callID,
		durableOperation,
		input,
		nil,
	)
}

// StartCallInline performs the same authorization and ownership binding before
// using the optional synchronous fast path.
func (service *Service) StartCallInline(
	ctx context.Context,
	callID CallID,
	durableOperation Operation,
	input []byte,
	budget InlineBudget,
) (Snapshot, error) {
	return service.startCall(
		ctx,
		callID,
		durableOperation,
		input,
		&budget,
	)
}

func (service *Service) startCall(
	ctx context.Context,
	callID CallID,
	durableOperation Operation,
	input []byte,
	budget *InlineBudget,
) (Snapshot, error) {
	if service == nil || ctx == nil || service.runtime == nil {
		return Snapshot{}, ErrInvalidService
	}
	if service.publicAccess {
		return service.startRuntime(
			ctx,
			callID,
			durableOperation,
			input,
			budget,
		)
	}
	principal, err := service.authorize(
		ctx,
		AuthorizationRequest{
			CallID:    callID,
			Operation: durableOperation,
			Action:    ActionStart,
		},
	)
	if err != nil {
		return Snapshot{}, err
	}
	ownership, err := NewOwnership(callID, durableOperation, principal)
	if err != nil {
		return Snapshot{}, err
	}
	if err := service.ownerships.Bind(ctx, ownership); err != nil {
		if errors.Is(err, ErrOwnershipConflict) {
			return Snapshot{}, ErrAccessDenied
		}
		return Snapshot{}, err
	}
	return service.startRuntime(
		ctx,
		callID,
		durableOperation,
		input,
		budget,
	)
}

func (service *Service) startRuntime(
	ctx context.Context,
	callID CallID,
	durableOperation Operation,
	input []byte,
	budget *InlineBudget,
) (Snapshot, error) {
	if budget != nil {
		if inline, ok := service.runtime.(inlineServiceRuntime); ok {
			return inline.StartCallInline(
				ctx,
				callID,
				durableOperation,
				input,
				*budget,
			)
		}
	}
	return service.runtime.StartCall(ctx, callID, durableOperation, input)
}

// Attach authorizes one existing call before loading any payload-bearing state.
func (service *Service) Attach(
	ctx context.Context,
	callID CallID,
	after uint64,
	limit int,
) (Attachment, error) {
	if err := service.authorizeExisting(ctx, callID, ActionAttach); err != nil {
		return Attachment{}, err
	}
	return service.runtime.Attach(ctx, callID, after, limit)
}

// History authorizes a payload-free historical view independently from live
// Attach permission. Frame sequence and kind remain available for timelines.
func (service *Service) History(
	ctx context.Context,
	callID CallID,
	after uint64,
	limit int,
) (Attachment, error) {
	if err := service.authorizeExisting(ctx, callID, ActionHistory); err != nil {
		return Attachment{}, err
	}
	attachment, err := service.runtime.Attach(ctx, callID, after, limit)
	if err != nil {
		return Attachment{}, err
	}
	return payloadFreeHistory(attachment), nil
}

// HistoryDetail separately authorizes a payload-bearing page and enforces a
// hard byte budget after the existing frame/page budget.
func (service *Service) HistoryDetail(
	ctx context.Context,
	callID CallID,
	after uint64,
	limit int,
	maxPayloadBytes int,
) (Attachment, error) {
	if err := service.authorizeExisting(
		ctx,
		callID,
		ActionHistoryDetail,
	); err != nil {
		return Attachment{}, err
	}
	attachment, err := service.runtime.Attach(ctx, callID, after, limit)
	if err != nil {
		return Attachment{}, err
	}
	return boundedDetailHistory(attachment, maxPayloadBytes)
}

// SubmitSignal authorizes one existing call before storing the command.
func (service *Service) SubmitSignal(
	ctx context.Context,
	callID CallID,
	commandID string,
	payload []byte,
) (Snapshot, error) {
	if err := service.authorizeExisting(ctx, callID, ActionSignal); err != nil {
		return Snapshot{}, err
	}
	return service.runtime.SubmitSignal(ctx, callID, commandID, payload)
}

// RequestCancel authorizes one existing call before storing the command.
func (service *Service) RequestCancel(
	ctx context.Context,
	callID CallID,
	commandID string,
) (Snapshot, error) {
	if err := service.authorizeExisting(ctx, callID, ActionCancel); err != nil {
		return Snapshot{}, err
	}
	return service.runtime.RequestCancel(ctx, callID, commandID)
}

func (service *Service) authorizeExisting(
	ctx context.Context,
	callID CallID,
	action Action,
) error {
	if service == nil || ctx == nil || service.runtime == nil {
		return ErrInvalidService
	}
	if service.publicAccess {
		return nil
	}
	if service.authorizer == nil || service.ownerships == nil {
		return ErrAccessDenied
	}
	principal, err := service.principal(ctx)
	if err != nil {
		return err
	}
	ownership, err := service.ownerships.Load(ctx, callID)
	if err != nil {
		if errors.Is(err, ErrOwnershipNotFound) {
			return ErrAccessDenied
		}
		return err
	}
	if !ownership.Matches(principal) {
		return ErrAccessDenied
	}
	_, err = service.callAuthorizer(
		ctx,
		principal,
		AuthorizationRequest{
			CallID:    callID,
			Operation: ownership.Operation(),
			Action:    action,
		},
	)
	return err
}

func (service *Service) authorize(
	ctx context.Context,
	request AuthorizationRequest,
) (security.Principal, error) {
	if service.authorizer == nil || service.ownerships == nil {
		return security.Principal{}, ErrAccessDenied
	}
	principal, err := service.principal(ctx)
	if err != nil {
		return security.Principal{}, err
	}
	return principal, service.authorizePrincipal(ctx, principal, request)
}

func (service *Service) authorizePrincipal(
	ctx context.Context,
	principal security.Principal,
	request AuthorizationRequest,
) error {
	_, err := service.callAuthorizer(ctx, principal, request)
	return err
}

func (service *Service) callAuthorizer(
	ctx context.Context,
	principal security.Principal,
	request AuthorizationRequest,
) (decision authz.Decision, err error) {
	target, err := continuationAuthorizationOperation(request.Action)
	if err != nil {
		return authz.Decision{}, ErrAuthorizationFailed
	}
	defer func() {
		if recover() != nil {
			decision = authz.Decision{}
			err = ErrAuthorizationFailed
		}
	}()
	decision, err = service.authorizer.Authorize(
		ctx,
		principal,
		target,
		request,
	)
	if err != nil {
		return authz.Decision{}, errors.Join(ErrAuthorizationFailed, err)
	}
	if !decision.Allowed {
		return decision, ErrAccessDenied
	}
	return decision, nil
}

func (service *Service) principal(
	ctx context.Context,
) (security.Principal, error) {
	principal, exists := security.PrincipalFromContext(ctx)
	if !exists || principal.Validate(service.now().UTC()) != nil {
		return security.Principal{}, ErrAuthenticationRequired
	}
	return principal, nil
}

func continuationAuthorizationOperation(
	action Action,
) (operation.Operation, error) {
	if !action.valid() {
		return operation.Operation{}, ErrInvalidService
	}
	kind := operation.KindUnary
	if action == ActionAttach {
		kind = operation.KindServerStream
	}
	return operation.New(
		"continuation",
		continuationAuthorizationService,
		string(action),
		kind,
	)
}

func isNilServiceValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
