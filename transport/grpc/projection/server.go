// Package projectiongrpc adapts projection Sources to a bounded gRPC stream.
package projectiongrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	projectionv1 "github.com/keelab/keelith/api/projection/v1"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/programmable/projection"
	"github.com/keelab/keelith/security"
	"github.com/keelab/keelith/security/authz"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const defaultServerMaxFrameBytes = maxWireFrameBytes

var (
	// ErrInvalidServer reports an invalid registration or byte budget.
	ErrInvalidServer = errors.New(
		"projection grpc transport: invalid server",
	)
	// ErrDuplicateProjection reports more than one owner for an identity.
	ErrDuplicateProjection = errors.New(
		"projection grpc transport: duplicate projection",
	)
)

// Registration assigns one exact schema to its single owner Source.
type Registration struct {
	Schema projection.Schema
	Source projection.Source
}

// ServerOption configures the projection gRPC server adapter.
type ServerOption interface {
	applyServer(*serverOptions) error
}

type serverOptionFunc func(*serverOptions) error

func (f serverOptionFunc) applyServer(options *serverOptions) error {
	return f(options)
}

type serverOptions struct {
	maxFrameBytes int
	publicAccess  bool
	authorizer    authz.Authorizer
	accessSet     bool
	quota         *projection.QuotaManager
	scheduler     *projection.FairScheduler
}

// WithServerMaxFrameBytes sets the encoded per-frame send budget.
func WithServerMaxFrameBytes(maximum int) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if maximum < 256 || maximum > maxWireFrameBytes {
			return ErrInvalidServer
		}
		options.maxFrameBytes = maximum
		return nil
	})
}

// WithServerPublicAccess explicitly allows unauthenticated subscriptions.
//
// Omitting both public access and an Authorizer makes the server fail closed.
func WithServerPublicAccess() ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if options.accessSet {
			return ErrInvalidServer
		}
		options.publicAccess = true
		options.accessSet = true
		return nil
	})
}

// WithServerAuthorizer requires an authenticated Principal and delegates each
// projection identity decision to the existing Keelith authorization model.
func WithServerAuthorizer(authorizer authz.Authorizer) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if options.accessSet || isNilValue(authorizer) {
			return ErrInvalidServer
		}
		options.authorizer = authorizer
		options.accessSet = true
		return nil
	})
}

// WithServerAdmission enables tenant quota and fair frame scheduling. Tenant
// claims are parsed only after the configured Authorizer allows the request.
func WithServerAdmission(
	quota *projection.QuotaManager,
	scheduler *projection.FairScheduler,
) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if quota == nil || scheduler == nil || options.quota != nil ||
			options.scheduler != nil {
			return ErrInvalidServer
		}
		options.quota = quota
		options.scheduler = scheduler
		return nil
	})
}

// AuthorizationRequest is the bounded request value passed to Authorizer.
type AuthorizationRequest struct {
	Projection projection.ProjectionID
}

type serverProjection struct {
	schema projection.Schema
	source projection.Source
}

// Server exposes registered owner Sources through ProjectionService.
type Server struct {
	projectionv1.UnimplementedProjectionServiceServer

	projections   map[projection.ProjectionID]serverProjection
	maxFrameBytes int
	publicAccess  bool
	authorizer    authz.Authorizer
	quota         *projection.QuotaManager
	scheduler     *projection.FairScheduler
	registered    bool
}

// NewServer validates and snapshots exact projection registrations.
func NewServer(
	registrations []Registration,
	optionList ...ServerOption,
) (*Server, error) {
	if len(registrations) == 0 || len(registrations) > 1024 {
		return nil, ErrInvalidServer
	}
	options := serverOptions{maxFrameBytes: defaultServerMaxFrameBytes}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: option %d is nil",
				ErrInvalidServer,
				index,
			)
		}
		if err := option.applyServer(&options); err != nil {
			return nil, err
		}
	}
	result := &Server{
		projections:   make(map[projection.ProjectionID]serverProjection),
		maxFrameBytes: options.maxFrameBytes,
		publicAccess:  options.publicAccess,
		authorizer:    options.authorizer,
		quota:         options.quota,
		scheduler:     options.scheduler,
	}
	for _, registration := range registrations {
		if err := registration.Schema.Validate(); err != nil ||
			isNilSource(registration.Source) {
			return nil, ErrInvalidServer
		}
		if _, duplicate := result.projections[registration.Schema.ID]; duplicate {
			return nil, fmt.Errorf(
				"%w: %q",
				ErrDuplicateProjection,
				registration.Schema.ID,
			)
		}
		result.projections[registration.Schema.ID] = serverProjection{
			schema: registration.Schema,
			source: registration.Source,
		}
	}
	return result, nil
}

// Register installs the protocol service exactly once.
func (server *Server) Register(registrar grpc.ServiceRegistrar) error {
	if server == nil || registrar == nil || server.registered {
		return ErrInvalidServer
	}
	projectionv1.RegisterProjectionServiceServer(registrar, server)
	server.registered = true
	return nil
}

// Subscribe relays one ordered Source session until disconnect or completion.
func (server *Server) Subscribe(
	request *projectionv1.SubscribeRequest,
	stream grpc.ServerStreamingServer[projectionv1.SubscribeResponse],
) error {
	if server == nil || stream == nil {
		return status.Error(codes.Internal, "projection server unavailable")
	}
	subscription, err := requestFromWire(request)
	if err != nil {
		return status.Error(
			codes.InvalidArgument,
			"invalid projection subscription",
		)
	}
	principal, authenticated, err := server.authorize(
		stream.Context(),
		subscription.Schema.ID,
	)
	if err != nil {
		return err
	}
	var tenant projection.Tenant
	if server.quota != nil {
		tenant, err = admittedTenant(principal, authenticated)
		if err != nil {
			return status.Error(codes.PermissionDenied, "projection tenant denied")
		}
	}
	registered, exists := server.projections[subscription.Schema.ID]
	if !exists {
		return status.Error(codes.NotFound, "projection not registered")
	}
	if !sameSchema(registered.schema, subscription.Schema) {
		return status.Error(
			codes.FailedPrecondition,
			"projection schema mismatch",
		)
	}
	var quotaLease *projection.SessionQuotaLease
	if server.quota != nil {
		quotaLease, err = server.quota.AcquireSession(tenant, time.Now().UTC())
		if err != nil {
			return admissionStatus(err)
		}
		defer func() { _ = quotaLease.Close() }()
	}
	session, err := registered.source.Open(stream.Context(), subscription)
	if err != nil {
		return sourceStatus(err)
	}
	if isNilSession(session) {
		return status.Error(codes.Internal, "projection source unavailable")
	}
	defer func() {
		_ = session.Close()
	}()
	for {
		frame, nextErr := session.Next(stream.Context())
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				return nil
			}
			return sourceStatus(nextErr)
		}
		encoded, encodeErr := frameToWire(frame)
		if encodeErr != nil {
			return status.Error(
				codes.Internal,
				"projection source emitted an invalid frame",
			)
		}
		if proto.Size(encoded) > server.maxFrameBytes {
			return status.Error(
				codes.ResourceExhausted,
				"projection frame exceeds server budget",
			)
		}
		frameBytes := proto.Size(encoded)
		var permit *projection.SchedulePermit
		if server.scheduler != nil {
			permit, err = server.scheduler.Acquire(
				stream.Context(),
				tenant,
				frameBytes,
			)
			if err != nil {
				return admissionStatus(err)
			}
		}
		if quotaLease != nil {
			if err := quotaLease.AllowFrame(
				frameRows(frame),
				frameBytes,
				time.Now().UTC(),
			); err != nil {
				_ = permit.Close()
				return admissionStatus(err)
			}
		}
		sendErr := stream.Send(encoded)
		_ = permit.Close()
		if sendErr != nil {
			return sendErr
		}
	}
}

func (server *Server) authorize(
	ctx context.Context,
	projectionID projection.ProjectionID,
) (security.Principal, bool, error) {
	if server.publicAccess {
		return security.Principal{}, false, nil
	}
	if server.authorizer == nil {
		return security.Principal{}, false,
			status.Error(codes.PermissionDenied, "projection access denied")
	}
	principal, exists := security.PrincipalFromContext(ctx)
	if !exists || principal.Validate(time.Now()) != nil {
		return security.Principal{}, false, status.Error(
			codes.Unauthenticated,
			"projection authentication required",
		)
	}
	target, exists := operation.FromContext(ctx)
	if !exists {
		var err error
		target, err = operation.New(
			"grpc",
			projectionv1.ProjectionService_ServiceDesc.ServiceName,
			"Subscribe",
			operation.KindServerStream,
		)
		if err != nil {
			return security.Principal{}, false, status.Error(
				codes.Internal,
				"projection authorization unavailable",
			)
		}
	}
	decision, err := callAuthorizer(
		ctx,
		server.authorizer,
		principal,
		target,
		AuthorizationRequest{Projection: projectionID},
	)
	if err != nil {
		return security.Principal{}, false, status.Error(
			codes.Internal,
			"projection authorization unavailable",
		)
	}
	if !decision.Allowed {
		return security.Principal{}, false,
			status.Error(codes.PermissionDenied, "projection access denied")
	}
	return principal, true, nil
}

func admittedTenant(
	principal security.Principal,
	authenticated bool,
) (projection.Tenant, error) {
	if !authenticated {
		return projection.NewTenant("public", projection.TenantStandard)
	}
	claims := principal.Claims()
	identity := claims["keelith.tenant"]
	if identity == "" {
		identity = principal.Issuer() + "\x00" + principal.Subject()
	}
	class := projection.TenantClass(claims["keelith.tenant_class"])
	if class == "" {
		class = projection.TenantStandard
	}
	return projection.NewTenant(identity, class)
}

func frameRows(frame projection.Frame) int {
	switch value := frame.(type) {
	case projection.SnapshotChunkFrame:
		return len(value.Mutations)
	case projection.DeltaFrame:
		return len(value.Batch.Mutations)
	default:
		return 0
	}
}

func admissionStatus(err error) error {
	if err == nil {
		return status.Error(codes.Internal, "projection admission failed")
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	retryAfter := time.Duration(0)
	var quota *projection.QuotaExceededError
	if errors.As(err, &quota) {
		retryAfter = quota.RetryAfter
	}
	var full *projection.SchedulerFullError
	if errors.As(err, &full) {
		retryAfter = full.RetryAfter
	}
	if retryAfter <= 0 {
		return status.Error(codes.Unavailable, "projection admission unavailable")
	}
	base := status.New(codes.ResourceExhausted, "projection tenant budget exceeded")
	withDetails, detailErr := base.WithDetails(&errdetails.RetryInfo{
		RetryDelay: durationpb.New(retryAfter),
	})
	if detailErr != nil {
		return base.Err()
	}
	return withDetails.Err()
}

func callAuthorizer(
	ctx context.Context,
	authorizer authz.Authorizer,
	principal security.Principal,
	target operation.Operation,
	request AuthorizationRequest,
) (decision authz.Decision, err error) {
	defer func() {
		if recover() != nil {
			decision = authz.Decision{}
			err = ErrInvalidServer
		}
	}()
	return authorizer.Authorize(ctx, principal, target, request)
}

func sourceStatus(err error) error {
	if err == nil {
		return status.Error(codes.Internal, "projection source failed")
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if errors.Is(err, projection.ErrSchemaMismatch) {
		return status.Error(
			codes.FailedPrecondition,
			"projection schema mismatch",
		)
	}
	if errors.Is(err, projection.ErrInvalidSubscription) ||
		errors.Is(err, projection.ErrInvalidCursor) {
		return status.Error(
			codes.InvalidArgument,
			"invalid projection subscription",
		)
	}
	return status.Error(codes.Unavailable, "projection source unavailable")
}

func isNilSource(source projection.Source) bool {
	return isNilValue(source)
}

func isNilSession(session projection.Session) bool {
	return isNilValue(session)
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}
	reflection := reflect.ValueOf(value)
	switch reflection.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflection.IsNil()
	default:
		return false
	}
}
