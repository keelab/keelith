package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"reflect"

	"github.com/keelab/keelith/programmable/continuation"
	"github.com/keelab/keelith/programmable/projection"
)

const (
	maxProgrammableAdminBody = 4 * 1024
	continuationStatusLimit  = 1_000
)

// ContinuationStatusSource is the payload-free listing capability needed by
// the programmable runtime catalog.
type ContinuationStatusSource interface {
	ListCalls(context.Context, continuation.ListRequest) ([]continuation.CallSummary, error)
}

// ContinuationRuntimeStatus adapts bounded continuation lifecycle counts
// without exposing CallID, operation, workflow identity, frames, or errors.
func ContinuationRuntimeStatus(source ContinuationStatusSource) RuntimeStatusProvider {
	if nilInterface(source) {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		calls, err := source.ListCalls(ctx, continuation.ListRequest{Limit: continuationStatusLimit})
		if err != nil {
			return RuntimeStatus{}, err
		}
		counts := map[continuation.Status]uint64{
			continuation.StatusAccepted:        0,
			continuation.StatusRunning:         0,
			continuation.StatusWaiting:         0,
			continuation.StatusSuspended:       0,
			continuation.StatusCancelRequested: 0,
			continuation.StatusCompleted:       0,
			continuation.StatusFailed:          0,
			continuation.StatusCanceled:        0,
			continuation.StatusExpired:         0,
		}
		active := uint64(0)
		for _, call := range calls {
			if _, known := counts[call.Status]; !known {
				return RuntimeStatus{}, continuation.ErrInvalidRetention
			}
			counts[call.Status]++
			if !call.Status.Terminal() {
				active++
			}
		}
		boundedActive := active
		if boundedActive > uint64(math.MaxInt) {
			boundedActive = uint64(math.MaxInt)
		}
		truncated := uint64(0)
		if len(calls) == continuationStatusLimit {
			truncated = 1
		}
		return RuntimeStatus{
			State: "active", Ready: true, Active: int(boundedActive),
			Counters: []RuntimeCounter{
				{Name: "accepted", Value: counts[continuation.StatusAccepted]},
				{Name: "cancel_requested", Value: counts[continuation.StatusCancelRequested]},
				{Name: "canceled", Value: counts[continuation.StatusCanceled]},
				{Name: "completed", Value: counts[continuation.StatusCompleted]},
				{Name: "expired", Value: counts[continuation.StatusExpired]},
				{Name: "failed", Value: counts[continuation.StatusFailed]},
				{Name: "running", Value: counts[continuation.StatusRunning]},
				{Name: "sampled", Value: uint64(len(calls))},
				{Name: "suspended", Value: counts[continuation.StatusSuspended]},
				{Name: "truncated", Value: truncated},
				{Name: "waiting", Value: counts[continuation.StatusWaiting]},
			},
			Capabilities: []string{
				"bounded-retention", "durable-history", "fenced-lease", "payload-free-admin",
			},
		}, nil
	}
}

// ProgrammableRuntimeProviders are the three required data-plane status
// providers registered under stable, non-user-controlled catalog identities.
type ProgrammableRuntimeProviders struct {
	Continuation RuntimeStatusProvider
	Topology     RuntimeStatusProvider
	Projection   RuntimeStatusProvider
}

// RegisterProgrammableRuntime atomically registers all three data planes.
func RegisterProgrammableRuntime(
	catalog *RuntimeCatalog,
	providers ProgrammableRuntimeProviders,
) error {
	if catalog == nil || providers.Continuation == nil ||
		providers.Topology == nil || providers.Projection == nil {
		return ErrInvalidOption
	}
	return catalog.RegisterAll(
		RuntimeStatusRegistration{Name: "main", Kind: "continuation", Provider: providers.Continuation},
		RuntimeStatusRegistration{Name: "main", Kind: "topology", Provider: providers.Topology},
		RuntimeStatusRegistration{Name: "main", Kind: "projection", Provider: providers.Projection},
	)
}

type continuationCollector interface {
	Collect(context.Context, int) (int, error)
}

// ProgrammableAdminConfig exposes only bounded maintenance capabilities.
// WithProgrammableAdmin additionally requires WithAccessPolicy, even on
// loopback.
type ProgrammableAdminConfig struct {
	Continuation continuationCollector
	Projection   projection.OwnerAdmin
}

// WithProgrammableAdmin explicitly enables protected maintenance POST routes.
// Omitting this option leaves the complete admin prefix unregistered.
func WithProgrammableAdmin(config ProgrammableAdminConfig) Option {
	return optionFunc(func(options *options) error {
		handler, err := newProgrammableAdminHandler(config)
		if err != nil {
			return err
		}
		options.programmableAdmin = handler
		return nil
	})
}

type programmableAdminHandler struct {
	continuation continuationCollector
	projection   projection.OwnerAdmin
}

type collectRequest struct {
	Limit int `json:"limit"`
}

type compactRequest struct {
	Shard      projection.ShardID `json:"shard"`
	Before     projection.Cursor  `json:"before"`
	MaxEntries int                `json:"maxEntries"`
}

type forceSnapshotRequest struct {
	Shard projection.ShardID `json:"shard"`
}

func newProgrammableAdminHandler(config ProgrammableAdminConfig) (http.Handler, error) {
	if nilInterface(config.Continuation) && nilInterface(config.Projection) {
		return nil, fmt.Errorf("programmable admin has no capability")
	}
	return &programmableAdminHandler{
		continuation: config.Continuation,
		projection:   config.Projection,
	}, nil
}

func (handler *programmableAdminHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeDiagnosticError(writer, http.StatusUnsupportedMediaType, "invalid")
		return
	}
	switch request.URL.Path {
	case "/admin/programmable/continuation/collect":
		handler.collect(writer, request)
	case "/admin/programmable/projection/compact":
		handler.compact(writer, request)
	case "/admin/programmable/projection/force-snapshot":
		handler.forceSnapshot(writer, request)
	default:
		http.NotFound(writer, request)
	}
}

func (handler *programmableAdminHandler) collect(writer http.ResponseWriter, request *http.Request) {
	if nilInterface(handler.continuation) {
		writeDiagnosticError(writer, http.StatusNotFound, "unavailable")
		return
	}
	var input collectRequest
	if err := decodeAdminRequest(writer, request, &input); err != nil || input.Limit < 1 || input.Limit > 1_000 {
		writeAdminDecodeError(writer, err)
		return
	}
	deleted, err := safeCollect(request.Context(), handler.continuation, input.Limit)
	if err != nil || deleted < 0 || deleted > input.Limit {
		writeDiagnosticError(writer, http.StatusServiceUnavailable, "unavailable")
		return
	}
	writeDiagnosticJSON(writer, http.StatusOK, struct {
		Deleted int `json:"deleted"`
	}{Deleted: deleted})
}

func (handler *programmableAdminHandler) compact(writer http.ResponseWriter, request *http.Request) {
	if nilInterface(handler.projection) {
		writeDiagnosticError(writer, http.StatusNotFound, "unavailable")
		return
	}
	var input compactRequest
	if err := decodeAdminRequest(writer, request, &input); err != nil {
		writeAdminDecodeError(writer, err)
		return
	}
	command := projection.CompactRequest{
		Shard: input.Shard, Before: input.Before, MaxEntries: input.MaxEntries,
	}
	if command.Validate() != nil {
		writeDiagnosticError(writer, http.StatusBadRequest, "invalid")
		return
	}
	result, err := safeCompact(request.Context(), handler.projection, command)
	if err != nil || result.DeletedEntries > uint64(input.MaxEntries) {
		writeDiagnosticError(writer, http.StatusServiceUnavailable, "unavailable")
		return
	}
	writeDiagnosticJSON(writer, http.StatusOK, struct {
		Advanced       bool   `json:"advanced"`
		DeletedEntries uint64 `json:"deletedEntries"`
	}{Advanced: result.Floor != result.PreviousFloor, DeletedEntries: result.DeletedEntries})
}

func (handler *programmableAdminHandler) forceSnapshot(writer http.ResponseWriter, request *http.Request) {
	if nilInterface(handler.projection) {
		writeDiagnosticError(writer, http.StatusNotFound, "unavailable")
		return
	}
	var input forceSnapshotRequest
	if err := decodeAdminRequest(writer, request, &input); err != nil || input.Shard.Validate() != nil {
		writeAdminDecodeError(writer, err)
		return
	}
	result, err := safeForceSnapshot(request.Context(), handler.projection, input.Shard)
	if err != nil || result.Generation == 0 {
		writeDiagnosticError(writer, http.StatusServiceUnavailable, "unavailable")
		return
	}
	writeDiagnosticJSON(writer, http.StatusOK, struct {
		Generation uint64 `json:"generation"`
	}{Generation: result.Generation})
}

func decodeAdminRequest(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxProgrammableAdminBody)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing request body")
	}
	return nil
}

func writeAdminDecodeError(writer http.ResponseWriter, err error) {
	var maximum *http.MaxBytesError
	if errors.As(err, &maximum) {
		writeDiagnosticError(writer, http.StatusRequestEntityTooLarge, "invalid")
		return
	}
	writeDiagnosticError(writer, http.StatusBadRequest, "invalid")
}

func safeCollect(ctx context.Context, collector continuationCollector, limit int) (deleted int, err error) {
	defer func() {
		if recover() != nil {
			deleted, err = 0, fmt.Errorf("programmable admin panic")
		}
	}()
	return collector.Collect(ctx, limit)
}

func safeCompact(
	ctx context.Context,
	admin projection.OwnerAdmin,
	request projection.CompactRequest,
) (result projection.CompactResult, err error) {
	defer func() {
		if recover() != nil {
			result, err = projection.CompactResult{}, fmt.Errorf("programmable admin panic")
		}
	}()
	return admin.CompactProjection(ctx, request)
}

func safeForceSnapshot(
	ctx context.Context,
	admin projection.OwnerAdmin,
	shard projection.ShardID,
) (result projection.ForcedSnapshot, err error) {
	defer func() {
		if recover() != nil {
			result, err = projection.ForcedSnapshot{}, fmt.Errorf("programmable admin panic")
		}
	}()
	return admin.ForceProjectionSnapshot(ctx, shard)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
