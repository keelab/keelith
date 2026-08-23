package projectiongrpc

import (
	"errors"
	"fmt"
	"time"

	projectionv1 "github.com/keelab/keelith/api/projection/v1"
	"github.com/keelab/keelith/programmable/projection"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxWireMutations  = 10_000
	maxWireFrameBytes = 32 * 1024 * 1024
)

var (
	// ErrInvalidWireFrame reports a malformed projection protocol message.
	ErrInvalidWireFrame = errors.New(
		"projection grpc transport: invalid wire frame",
	)
	// ErrFrameTooLarge reports a frame beyond the configured byte budget.
	ErrFrameTooLarge = errors.New(
		"projection grpc transport: frame too large",
	)
)

func requestToWire(
	request projection.SubscribeRequest,
) *projectionv1.SubscribeRequest {
	return &projectionv1.SubscribeRequest{
		Schema:        schemaToWire(request.Schema),
		After:         string(request.After),
		ForceSnapshot: request.ForceSnapshot,
	}
}

func requestFromWire(
	request *projectionv1.SubscribeRequest,
) (projection.SubscribeRequest, error) {
	if request == nil {
		return projection.SubscribeRequest{}, ErrInvalidWireFrame
	}
	schema, err := schemaFromWire(request.GetSchema())
	if err != nil {
		return projection.SubscribeRequest{}, err
	}
	result := projection.SubscribeRequest{
		Schema:        schema,
		After:         projection.Cursor(request.GetAfter()),
		ForceSnapshot: request.GetForceSnapshot(),
	}
	if err := result.Validate(); err != nil {
		return projection.SubscribeRequest{}, fmt.Errorf(
			"%w: subscription",
			ErrInvalidWireFrame,
		)
	}
	return result, nil
}

func schemaToWire(schema projection.Schema) *projectionv1.Schema {
	return &projectionv1.Schema{
		Id:                     string(schema.ID),
		Fingerprint:            schema.Fingerprint,
		KeyFingerprint:         schema.KeyFingerprint,
		CompatibleFingerprints: schema.CompatibleFingerprints.Values(),
	}
}

func schemaFromWire(
	schema *projectionv1.Schema,
) (projection.Schema, error) {
	if schema == nil {
		return projection.Schema{}, ErrInvalidWireFrame
	}
	compatible, err := projection.NewFingerprintSet(
		schema.GetCompatibleFingerprints()...,
	)
	if err != nil {
		return projection.Schema{}, fmt.Errorf(
			"%w: schema compatibility",
			ErrInvalidWireFrame,
		)
	}
	result := projection.Schema{
		ID:                     projection.ProjectionID(schema.GetId()),
		Fingerprint:            schema.GetFingerprint(),
		KeyFingerprint:         schema.GetKeyFingerprint(),
		CompatibleFingerprints: compatible,
	}
	if err := result.Validate(); err != nil {
		return projection.Schema{}, fmt.Errorf(
			"%w: schema",
			ErrInvalidWireFrame,
		)
	}
	return result, nil
}

func frameToWire(
	frame projection.Frame,
) (*projectionv1.SubscribeResponse, error) {
	switch current := frame.(type) {
	case projection.SnapshotBeginFrame:
		if err := current.Schema.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		return &projectionv1.SubscribeResponse{
			Body: &projectionv1.SubscribeResponse_SnapshotBegin{
				SnapshotBegin: &projectionv1.SnapshotBegin{
					Schema: schemaToWire(current.Schema),
				},
			},
		}, nil
	case projection.SnapshotChunkFrame:
		mutations, err := mutationsToWire(current.Mutations)
		if err != nil || len(mutations) == 0 {
			return nil, ErrInvalidWireFrame
		}
		return &projectionv1.SubscribeResponse{
			Body: &projectionv1.SubscribeResponse_SnapshotChunk{
				SnapshotChunk: &projectionv1.SnapshotChunk{
					Mutations: mutations,
				},
			},
		}, nil
	case projection.SnapshotEndFrame:
		if err := current.Cursor.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		sourceTime, err := timeToWire(current.SourceTime)
		if err != nil {
			return nil, err
		}
		return &projectionv1.SubscribeResponse{
			Body: &projectionv1.SubscribeResponse_SnapshotEnd{
				SnapshotEnd: &projectionv1.SnapshotEnd{
					Cursor:     string(current.Cursor),
					SourceTime: sourceTime,
				},
			},
		}, nil
	case projection.DeltaFrame:
		if err := current.Batch.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		mutations, err := mutationsToWire(current.Batch.Mutations)
		if err != nil {
			return nil, err
		}
		sourceTime, err := timeToWire(current.Batch.SourceTime)
		if err != nil {
			return nil, err
		}
		return &projectionv1.SubscribeResponse{
			Body: &projectionv1.SubscribeResponse_Delta{
				Delta: &projectionv1.Delta{
					Schema:     schemaToWire(current.Batch.Schema),
					Previous:   string(current.Batch.Previous),
					Cursor:     string(current.Batch.Cursor),
					SourceTime: sourceTime,
					Mutations:  mutations,
				},
			},
		}, nil
	case projection.HeartbeatFrame:
		if err := current.Cursor.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		sourceTime, err := timeToWire(current.SourceTime)
		if err != nil {
			return nil, err
		}
		return &projectionv1.SubscribeResponse{
			Body: &projectionv1.SubscribeResponse_Heartbeat{
				Heartbeat: &projectionv1.Heartbeat{
					Cursor:     string(current.Cursor),
					SourceTime: sourceTime,
				},
			},
		}, nil
	case projection.GapFrame:
		if err := current.Requested.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		if err := current.Floor.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		return &projectionv1.SubscribeResponse{
			Body: &projectionv1.SubscribeResponse_Gap{
				Gap: &projectionv1.Gap{
					Requested: string(current.Requested),
					Floor:     string(current.Floor),
				},
			},
		}, nil
	default:
		return nil, ErrInvalidWireFrame
	}
}

func frameFromWire(
	frame *projectionv1.SubscribeResponse,
) (projection.Frame, error) {
	if frame == nil {
		return nil, ErrInvalidWireFrame
	}
	switch body := frame.GetBody().(type) {
	case *projectionv1.SubscribeResponse_SnapshotBegin:
		schema, err := schemaFromWire(body.SnapshotBegin.GetSchema())
		if err != nil {
			return nil, err
		}
		return projection.SnapshotBeginFrame{Schema: schema}, nil
	case *projectionv1.SubscribeResponse_SnapshotChunk:
		mutations, err := mutationsFromWire(
			body.SnapshotChunk.GetMutations(),
		)
		if err != nil || len(mutations) == 0 {
			return nil, ErrInvalidWireFrame
		}
		return projection.SnapshotChunkFrame{Mutations: mutations}, nil
	case *projectionv1.SubscribeResponse_SnapshotEnd:
		cursor := projection.Cursor(body.SnapshotEnd.GetCursor())
		if err := cursor.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		sourceTime, err := timeFromWire(body.SnapshotEnd.GetSourceTime())
		if err != nil {
			return nil, err
		}
		return projection.SnapshotEndFrame{
			Cursor:     cursor,
			SourceTime: sourceTime,
		}, nil
	case *projectionv1.SubscribeResponse_Delta:
		schema, err := schemaFromWire(body.Delta.GetSchema())
		if err != nil {
			return nil, err
		}
		sourceTime, err := timeFromWire(body.Delta.GetSourceTime())
		if err != nil {
			return nil, err
		}
		mutations, err := mutationsFromWire(body.Delta.GetMutations())
		if err != nil {
			return nil, err
		}
		batch := projection.DeltaBatch{
			Schema:     schema,
			Previous:   projection.Cursor(body.Delta.GetPrevious()),
			Cursor:     projection.Cursor(body.Delta.GetCursor()),
			SourceTime: sourceTime,
			Mutations:  mutations,
		}
		if err := batch.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		return projection.DeltaFrame{Batch: batch}, nil
	case *projectionv1.SubscribeResponse_Heartbeat:
		cursor := projection.Cursor(body.Heartbeat.GetCursor())
		if err := cursor.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		sourceTime, err := timeFromWire(body.Heartbeat.GetSourceTime())
		if err != nil {
			return nil, err
		}
		return projection.HeartbeatFrame{
			Cursor:     cursor,
			SourceTime: sourceTime,
		}, nil
	case *projectionv1.SubscribeResponse_Gap:
		requested := projection.Cursor(body.Gap.GetRequested())
		floor := projection.Cursor(body.Gap.GetFloor())
		if err := requested.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		if err := floor.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		return projection.GapFrame{
			Requested: requested,
			Floor:     floor,
		}, nil
	default:
		return nil, ErrInvalidWireFrame
	}
}

func mutationsToWire(
	mutations []projection.Mutation,
) ([]*projectionv1.Mutation, error) {
	if len(mutations) == 0 || len(mutations) > maxWireMutations {
		return nil, ErrInvalidWireFrame
	}
	result := make([]*projectionv1.Mutation, len(mutations))
	for index, mutation := range mutations {
		if err := mutation.Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
		var kind projectionv1.Mutation_Kind
		switch mutation.Kind() {
		case projection.MutationUpsert:
			kind = projectionv1.Mutation_KIND_UPSERT
		case projection.MutationDelete:
			kind = projectionv1.Mutation_KIND_DELETE
		default:
			return nil, ErrInvalidWireFrame
		}
		result[index] = &projectionv1.Mutation{
			Kind:  kind,
			Key:   mutation.Key(),
			Value: mutation.Value(),
		}
	}
	return result, nil
}

func mutationsFromWire(
	mutations []*projectionv1.Mutation,
) ([]projection.Mutation, error) {
	if len(mutations) == 0 || len(mutations) > maxWireMutations {
		return nil, ErrInvalidWireFrame
	}
	result := make([]projection.Mutation, len(mutations))
	for index, mutation := range mutations {
		if mutation == nil {
			return nil, ErrInvalidWireFrame
		}
		switch mutation.GetKind() {
		case projectionv1.Mutation_KIND_UPSERT:
			result[index] = projection.Upsert(
				mutation.GetKey(),
				mutation.GetValue(),
			)
		case projectionv1.Mutation_KIND_DELETE:
			if len(mutation.GetValue()) != 0 {
				return nil, ErrInvalidWireFrame
			}
			result[index] = projection.Delete(mutation.GetKey())
		default:
			return nil, ErrInvalidWireFrame
		}
		if err := result[index].Validate(); err != nil {
			return nil, ErrInvalidWireFrame
		}
	}
	return result, nil
}

func timeToWire(value time.Time) (*timestamppb.Timestamp, error) {
	if value.IsZero() {
		return nil, ErrInvalidWireFrame
	}
	result := timestamppb.New(value.UTC())
	if err := result.CheckValid(); err != nil {
		return nil, ErrInvalidWireFrame
	}
	return result, nil
}

func timeFromWire(
	value *timestamppb.Timestamp,
) (time.Time, error) {
	if value == nil || value.CheckValid() != nil {
		return time.Time{}, ErrInvalidWireFrame
	}
	result := value.AsTime().UTC()
	if result.IsZero() {
		return time.Time{}, ErrInvalidWireFrame
	}
	return result, nil
}

func sameSchema(left, right projection.Schema) bool {
	return right.Accepts(left) == nil
}
