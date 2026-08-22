package continuation

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"
)

const (
	legacySnapshotCodecVersion   = "v1"
	retainedSnapshotCodecVersion = "v2"
	timerSnapshotCodecVersion    = "v3"
	snapshotCodecVersion         = "v4"
	maxEncodedSnapshotSize       = 4 * 1024 * 1024
)

var (
	// ErrInvalidSnapshotEncoding reports malformed or non-canonical data.
	ErrInvalidSnapshotEncoding = errors.New(
		"continuation: invalid snapshot encoding",
	)
	// ErrUnsupportedSnapshotVersion reports an unknown codec version.
	ErrUnsupportedSnapshotVersion = errors.New(
		"continuation: unsupported snapshot version",
	)
	// ErrSnapshotChecksum reports data that failed integrity verification.
	ErrSnapshotChecksum = errors.New("continuation: snapshot checksum mismatch")
	// ErrSnapshotTooLarge reports data beyond the bounded codec size.
	ErrSnapshotTooLarge = errors.New("continuation: snapshot encoding too large")
)

type snapshotDocument struct {
	Version  string        `json:"version"`
	Snapshot *snapshotWire `json:"snapshot"`
	Checksum string        `json:"checksum"`
}

type snapshotWire struct {
	CallID     string        `json:"call_id"`
	Operation  string        `json:"operation"`
	Status     Status        `json:"status"`
	Revision   uint64        `json:"revision"`
	Fence      uint64        `json:"fence"`
	Sequence   uint64        `json:"sequence"`
	FrameFloor uint64        `json:"frame_floor,omitempty"`
	ReadyAt    string        `json:"ready_at,omitempty"`
	Workflow   *workflowWire `json:"workflow,omitempty"`
	Frames     []frameWire   `json:"frames"`
	Commands   []commandWire `json:"commands"`
}

type frameWire struct {
	Sequence uint64    `json:"sequence"`
	Kind     FrameKind `json:"kind"`
	Payload  []byte    `json:"payload"`
}

type commandWire struct {
	ID     string      `json:"id"`
	Kind   commandKind `json:"kind"`
	Digest string      `json:"digest"`
}

// MarshalSnapshot encodes one validated Snapshot using the current versioned
// durable format and a checksum covering both version and snapshot content.
func MarshalSnapshot(snapshot Snapshot) ([]byte, error) {
	if err := validateSnapshot(snapshot); err != nil {
		return nil, ErrInvalidSnapshotEncoding
	}
	wire := snapshotToWire(snapshot)
	canonical, err := json.Marshal(wire)
	if err != nil {
		return nil, ErrInvalidSnapshotEncoding
	}
	checksum := snapshotChecksum(snapshotCodecVersion, canonical)
	document := snapshotDocument{
		Version:  snapshotCodecVersion,
		Snapshot: &wire,
		Checksum: hex.EncodeToString(checksum[:]),
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, ErrInvalidSnapshotEncoding
	}
	if len(encoded) > maxEncodedSnapshotSize {
		return nil, ErrSnapshotTooLarge
	}
	return encoded, nil
}

// ParseSnapshot decodes, verifies, and validates one versioned Snapshot.
func ParseSnapshot(encoded []byte) (Snapshot, error) {
	if len(encoded) == 0 {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}
	if len(encoded) > maxEncodedSnapshotSize {
		return Snapshot{}, ErrSnapshotTooLarge
	}
	if err := validateSnapshotJSONShape(encoded); err != nil {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var document snapshotDocument
	if err := decoder.Decode(&document); err != nil {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}
	if err := requireJSONEnd(decoder); err != nil {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}
	if document.Version != legacySnapshotCodecVersion &&
		document.Version != retainedSnapshotCodecVersion &&
		document.Version != timerSnapshotCodecVersion &&
		document.Version != snapshotCodecVersion {
		return Snapshot{}, ErrUnsupportedSnapshotVersion
	}
	if document.Snapshot == nil {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}

	canonical, err := json.Marshal(*document.Snapshot)
	if err != nil {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}
	expected := snapshotChecksum(document.Version, canonical)
	actual, err := hex.DecodeString(document.Checksum)
	if err != nil ||
		len(actual) != sha256.Size ||
		subtle.ConstantTimeCompare(actual, expected[:]) != 1 {
		return Snapshot{}, ErrSnapshotChecksum
	}
	if document.Version == legacySnapshotCodecVersion &&
		document.Snapshot.FrameFloor != 0 ||
		(document.Version == retainedSnapshotCodecVersion ||
			document.Version == timerSnapshotCodecVersion ||
			document.Version == snapshotCodecVersion) &&
			document.Snapshot.FrameFloor == 0 {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}

	snapshot, err := snapshotFromWire(
		*document.Snapshot,
		document.Version,
	)
	if err != nil {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}
	if err := validateSnapshot(snapshot); err != nil {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}
	return snapshot, nil
}

func snapshotToWire(snapshot Snapshot) snapshotWire {
	frames := make([]frameWire, len(snapshot.frames))
	for index, frame := range snapshot.frames {
		frames[index] = frameWire{
			Sequence: frame.sequence,
			Kind:     frame.kind,
			Payload:  frame.Payload(),
		}
	}
	ids := make([]string, 0, len(snapshot.commands))
	for id := range snapshot.commands {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	commands := make([]commandWire, len(ids))
	for index, id := range ids {
		record := snapshot.commands[id]
		commands[index] = commandWire{
			ID:     id,
			Kind:   record.kind,
			Digest: hex.EncodeToString(record.digest[:]),
		}
	}
	return snapshotWire{
		CallID:     snapshot.callID.value,
		Operation:  snapshot.operation.value,
		Status:     snapshot.status,
		Revision:   snapshot.revision,
		Fence:      snapshot.fence,
		Sequence:   snapshot.sequence,
		FrameFloor: snapshot.frameFloor,
		ReadyAt:    formatReadyAt(snapshot.readyAt),
		Workflow:   workflowToWire(snapshot.workflow),
		Frames:     frames,
		Commands:   commands,
	}
}

func snapshotFromWire(
	wire snapshotWire,
	version string,
) (Snapshot, error) {
	callID, err := NewCallID(wire.CallID)
	if err != nil {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}
	operation, err := NewOperation(wire.Operation)
	if err != nil {
		return Snapshot{}, ErrInvalidSnapshotEncoding
	}
	frames := make([]Frame, len(wire.Frames))
	for index, encoded := range wire.Frames {
		if !encoded.Kind.valid() || len(encoded.Payload) > maxPayloadBytes {
			return Snapshot{}, ErrInvalidSnapshotEncoding
		}
		frames[index] = Frame{
			sequence: encoded.Sequence,
			kind:     encoded.Kind,
			payload:  append([]byte(nil), encoded.Payload...),
		}
	}
	commands := make(map[string]commandRecord, len(wire.Commands))
	for _, encoded := range wire.Commands {
		if _, exists := commands[encoded.ID]; exists {
			return Snapshot{}, ErrInvalidSnapshotEncoding
		}
		digest, decodeErr := hex.DecodeString(encoded.Digest)
		if decodeErr != nil || len(digest) != sha256.Size {
			return Snapshot{}, ErrInvalidSnapshotEncoding
		}
		var value [sha256.Size]byte
		copy(value[:], digest)
		commands[encoded.ID] = commandRecord{
			kind:   encoded.Kind,
			digest: value,
		}
	}
	frameFloor := wire.FrameFloor
	if version == legacySnapshotCodecVersion {
		frameFloor = 1
	}
	var readyAt time.Time
	if (version == timerSnapshotCodecVersion ||
		version == snapshotCodecVersion) && wire.ReadyAt != "" {
		readyAt, err = time.Parse(time.RFC3339Nano, wire.ReadyAt)
		if err != nil || wire.ReadyAt != readyAt.UTC().Format(time.RFC3339Nano) {
			return Snapshot{}, ErrInvalidSnapshotEncoding
		}
		readyAt = readyAt.UTC()
	}
	var workflow *workflowSnapshotState
	if version == snapshotCodecVersion {
		workflow, err = workflowFromWire(wire.Workflow)
		if err != nil {
			return Snapshot{}, ErrInvalidSnapshotEncoding
		}
	}
	return Snapshot{
		callID:     callID,
		operation:  operation,
		status:     wire.Status,
		readyAt:    readyAt,
		revision:   wire.Revision,
		fence:      wire.Fence,
		sequence:   wire.Sequence,
		frameFloor: frameFloor,
		frames:     frames,
		commands:   commands,
		workflow:   workflow,
	}, nil
}

func snapshotChecksum(version string, canonical []byte) [sha256.Size]byte {
	hasher := sha256.New()
	writeDigestPart(hasher, []byte("continuation-snapshot"))
	writeDigestPart(hasher, []byte(version))
	writeDigestPart(hasher, canonical)
	var checksum [sha256.Size]byte
	copy(checksum[:], hasher.Sum(nil))
	return checksum
}

func formatReadyAt(readyAt time.Time) string {
	if readyAt.IsZero() {
		return ""
	}
	return readyAt.UTC().Format(time.RFC3339Nano)
}

func requireJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

func validateSnapshotJSONShape(encoded []byte) error {
	if err := rejectDuplicateJSONKeys(encoded); err != nil {
		return err
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &document); err != nil ||
		!hasExactFields(document, "version", "snapshot", "checksum") {
		return ErrInvalidSnapshotEncoding
	}
	var snapshot map[string]json.RawMessage
	if err := json.Unmarshal(document["snapshot"], &snapshot); err != nil {
		return ErrInvalidSnapshotEncoding
	}
	var version string
	if err := json.Unmarshal(document["version"], &version); err != nil {
		return ErrInvalidSnapshotEncoding
	}
	legacyFields := []string{
		"call_id",
		"operation",
		"status",
		"revision",
		"fence",
		"sequence",
		"frames",
		"commands",
	}
	currentFields := append(
		append([]string(nil), legacyFields...),
		"frame_floor",
	)
	timerFields := append(
		append([]string(nil), currentFields...),
		"ready_at",
	)
	workflowFields := append(
		append([]string(nil), currentFields...),
		"workflow",
	)
	timerWorkflowFields := append(
		append([]string(nil), timerFields...),
		"workflow",
	)
	validShape := false
	switch version {
	case legacySnapshotCodecVersion:
		validShape = hasExactFields(snapshot, legacyFields...)
	case timerSnapshotCodecVersion:
		validShape = hasExactFields(snapshot, currentFields...) ||
			hasExactFields(snapshot, timerFields...)
	case retainedSnapshotCodecVersion:
		validShape = hasExactFields(snapshot, currentFields...)
	case snapshotCodecVersion:
		validShape = hasExactFields(snapshot, currentFields...) ||
			hasExactFields(snapshot, timerFields...) ||
			hasExactFields(snapshot, workflowFields...) ||
			hasExactFields(snapshot, timerWorkflowFields...)
	default:
		validShape = hasExactFields(snapshot, legacyFields...) ||
			hasExactFields(snapshot, currentFields...) ||
			hasExactFields(snapshot, timerFields...) ||
			hasExactFields(snapshot, workflowFields...) ||
			hasExactFields(snapshot, timerWorkflowFields...)
	}
	if !validShape {
		return ErrInvalidSnapshotEncoding
	}
	if anyJSONNull(
		document["version"],
		document["snapshot"],
		document["checksum"],
		snapshot["call_id"],
		snapshot["operation"],
		snapshot["status"],
		snapshot["revision"],
		snapshot["fence"],
		snapshot["sequence"],
		snapshot["frame_floor"],
		snapshot["ready_at"],
		snapshot["workflow"],
		snapshot["frames"],
		snapshot["commands"],
	) {
		return ErrInvalidSnapshotEncoding
	}
	if raw, exists := snapshot["workflow"]; exists {
		if err := validateWorkflowJSONShape(raw); err != nil {
			return ErrInvalidSnapshotEncoding
		}
	}
	var frames []map[string]json.RawMessage
	if err := json.Unmarshal(snapshot["frames"], &frames); err != nil {
		return ErrInvalidSnapshotEncoding
	}
	for _, frame := range frames {
		if !hasExactFields(frame, "sequence", "kind", "payload") ||
			anyJSONNull(frame["sequence"], frame["kind"]) {
			return ErrInvalidSnapshotEncoding
		}
	}
	var commands []map[string]json.RawMessage
	if err := json.Unmarshal(snapshot["commands"], &commands); err != nil {
		return ErrInvalidSnapshotEncoding
	}
	for _, command := range commands {
		if !hasExactFields(command, "id", "kind", "digest") ||
			anyJSONNull(command["id"], command["kind"], command["digest"]) {
			return ErrInvalidSnapshotEncoding
		}
	}
	return nil
}

func hasExactFields(
	fields map[string]json.RawMessage,
	required ...string,
) bool {
	if len(fields) != len(required) {
		return false
	}
	for _, field := range required {
		if _, exists := fields[field]; !exists {
			return false
		}
	}
	return true
}

func anyJSONNull(values ...json.RawMessage) bool {
	for _, value := range values {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return true
		}
	}
	return false
}

func rejectDuplicateJSONKeys(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	return requireJSONEnd(decoder)
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			token, err = decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return ErrInvalidSnapshotEncoding
			}
			if _, exists := seen[key]; exists {
				return ErrInvalidSnapshotEncoding
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
			return ErrInvalidSnapshotEncoding
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		if token, err = decoder.Token(); err != nil || token != json.Delim(']') {
			return ErrInvalidSnapshotEncoding
		}
	default:
		return ErrInvalidSnapshotEncoding
	}
	return nil
}
