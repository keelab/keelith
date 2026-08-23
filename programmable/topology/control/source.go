// Package control applies signed, revisioned topology candidates while
// retaining the last known-good runtime epoch.
package control

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/keelab/keelith/config"
	"github.com/keelab/keelith/programmable/topology"
	"github.com/keelab/keelith/programmable/topology/planfile"
)

const (
	// APIVersion identifies one signed topology control envelope.
	APIVersion       = "keelith.dev/topology-control/v1alpha1"
	maxDocumentBytes = 2 * 1024 * 1024
)

var (
	// ErrInvalidCandidate reports an incomplete or inconsistent control envelope.
	ErrInvalidCandidate = errors.New("topology control: invalid candidate")
	// ErrInvalidSignature reports a missing or invalid candidate signature.
	ErrInvalidSignature = errors.New("topology control: invalid signature")
)

// CandidateSpec constructs one immutable plan candidate.
type CandidateSpec struct {
	Revision  uint64
	Plan      topology.Plan
	Hash      string
	Signature []byte
}

// Candidate is one complete immutable control-plane publication.
type Candidate struct {
	revision  uint64
	plan      topology.Plan
	snapshot  topology.Snapshot
	signature []byte
}

// NewCandidate validates, activates and snapshots a complete candidate.
func NewCandidate(spec CandidateSpec) (Candidate, error) {
	if spec.Revision == 0 || len(spec.Signature) > ed25519.SignatureSize {
		return Candidate{}, ErrInvalidCandidate
	}
	plan := cloneTopologyPlan(spec.Plan)
	snapshot, err := topology.Activate(plan)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: %w", ErrInvalidCandidate, err)
	}
	if spec.Hash != "" && spec.Hash != snapshot.Hash() {
		return Candidate{}, fmt.Errorf("%w: canonical hash mismatch", ErrInvalidCandidate)
	}
	return Candidate{
		revision:  spec.Revision,
		plan:      plan,
		snapshot:  snapshot,
		signature: append([]byte(nil), spec.Signature...),
	}, nil
}

// Revision returns the monotonic control-plane revision.
func (candidate Candidate) Revision() uint64 { return candidate.revision }

// Epoch returns the immutable runtime epoch carried by the plan.
func (candidate Candidate) Epoch() uint64 { return candidate.snapshot.Epoch() }

// Hash returns the plan's canonical SHA-256 identity.
func (candidate Candidate) Hash() string { return candidate.snapshot.Hash() }

// Snapshot returns the validated immutable topology snapshot.
func (candidate Candidate) Snapshot() topology.Snapshot { return candidate.snapshot }

// Plan returns a deep independent copy of the source plan.
func (candidate Candidate) Plan() topology.Plan { return cloneTopologyPlan(candidate.plan) }

// Signature returns an independent copy of the detached signature.
func (candidate Candidate) Signature() []byte {
	return append([]byte(nil), candidate.signature...)
}

// SigningBytes returns the canonical detached-signature payload.
func (candidate Candidate) SigningBytes() []byte {
	return []byte(fmt.Sprintf(
		"keelith-topology-candidate-v1\n%d\n%d\n%s\n",
		candidate.Revision(),
		candidate.Epoch(),
		candidate.Hash(),
	))
}

// Verifier authenticates one complete candidate before any runtime access.
type Verifier interface {
	Verify(context.Context, Candidate) error
}

// VerifierFunc adapts a candidate verification function.
type VerifierFunc func(context.Context, Candidate) error

// Verify invokes the adapted verifier.
func (fn VerifierFunc) Verify(ctx context.Context, candidate Candidate) error {
	return fn(ctx, candidate)
}

type ed25519Verifier struct{ publicKey ed25519.PublicKey }

// NewEd25519Verifier snapshots one public key for detached verification.
func NewEd25519Verifier(publicKey ed25519.PublicKey) (Verifier, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, ErrInvalidSignature
	}
	return ed25519Verifier{publicKey: append(ed25519.PublicKey(nil), publicKey...)}, nil
}

func (verifier ed25519Verifier) Verify(
	ctx context.Context,
	candidate Candidate,
) error {
	if ctx == nil {
		return ErrInvalidSignature
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if len(candidate.signature) != ed25519.SignatureSize ||
		!ed25519.Verify(verifier.publicKey, candidate.SigningBytes(), candidate.signature) {
		return ErrInvalidSignature
	}
	return nil
}

// Source loads and watches complete topology candidates.
type Source interface {
	Load(context.Context) (Candidate, error)
	Watch(context.Context) (Watcher, error)
}

// Watcher returns complete candidates and never partial plan mutations.
type Watcher interface {
	Next(context.Context) (Candidate, error)
	Close() error
}

type document struct {
	APIVersion string          `json:"apiVersion"`
	Revision   uint64          `json:"revision"`
	Hash       string          `json:"hash"`
	Signature  string          `json:"signature,omitempty"`
	Plan       json.RawMessage `json:"plan"`
}

// MarshalDocument emits one strict, newline-terminated control envelope.
func MarshalDocument(candidate Candidate) ([]byte, error) {
	if candidate.revision == 0 || candidate.snapshot.Hash() == "" {
		return nil, ErrInvalidCandidate
	}
	plan, err := planfile.Marshal(candidate.Plan())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCandidate, err)
	}
	payload, err := json.Marshal(document{
		APIVersion: APIVersion,
		Revision:   candidate.Revision(),
		Hash:       candidate.Hash(),
		Signature:  base64.StdEncoding.EncodeToString(candidate.signature),
		Plan:       bytes.TrimSpace(plan),
	})
	if err != nil || len(payload) > maxDocumentBytes {
		return nil, ErrInvalidCandidate
	}
	return append(payload, '\n'), nil
}

// ParseDocument strictly decodes and validates one complete envelope.
func ParseDocument(payload []byte) (Candidate, error) {
	if len(payload) == 0 || len(payload) > maxDocumentBytes ||
		rejectDuplicateKeys(payload) != nil {
		return Candidate{}, ErrInvalidCandidate
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var input document
	if decoder.Decode(&input) != nil || requireDocumentEnd(decoder) != nil ||
		input.APIVersion != APIVersion || input.Revision == 0 ||
		input.Hash == "" || len(input.Plan) == 0 {
		return Candidate{}, ErrInvalidCandidate
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(input.Signature)
	if err != nil || len(signature) != 0 && len(signature) != ed25519.SignatureSize {
		return Candidate{}, ErrInvalidCandidate
	}
	plan, err := planfile.Parse(input.Plan)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: %w", ErrInvalidCandidate, err)
	}
	return NewCandidate(CandidateSpec{
		Revision:  input.Revision,
		Plan:      plan,
		Hash:      input.Hash,
		Signature: signature,
	})
}

type configSource struct{ backend config.Source }

type configShutdown interface {
	Shutdown(context.Context) error
}

func newConfigSource(backend config.Source) (Source, error) {
	if isNilControlValue(backend) {
		return nil, ErrInvalidCandidate
	}
	return &configSource{backend: backend}, nil
}

func (source *configSource) Load(ctx context.Context) (Candidate, error) {
	snapshot, err := source.backend.Load(ctx)
	if err != nil {
		return Candidate{}, err
	}
	return candidateFromConfig(snapshot)
}

func (source *configSource) Watch(ctx context.Context) (Watcher, error) {
	watcher, err := source.backend.Watch(ctx)
	if err != nil {
		return nil, err
	}
	return &configWatcher{watcher: watcher}, nil
}

// Shutdown releases backend-owned clients and active backend watchers.
func (source *configSource) Shutdown(ctx context.Context) error {
	shutdown, ok := source.backend.(configShutdown)
	if !ok {
		return nil
	}
	return shutdown.Shutdown(ctx)
}

type configWatcher struct{ watcher config.Watcher }

func (watcher *configWatcher) Next(ctx context.Context) (Candidate, error) {
	snapshot, err := watcher.watcher.Next(ctx)
	if err != nil {
		return Candidate{}, err
	}
	return candidateFromConfig(snapshot)
}

func (watcher *configWatcher) Close() error { return watcher.watcher.Close() }

func candidateFromConfig(snapshot config.Snapshot) (Candidate, error) {
	payload, err := json.Marshal(snapshot.Values())
	if err != nil {
		return Candidate{}, ErrInvalidCandidate
	}
	return ParseDocument(payload)
}

func cloneTopologyPlan(plan topology.Plan) topology.Plan {
	result := topology.Plan{
		Epoch:      plan.Epoch,
		Placements: make(map[topology.PlacementID]struct{}, len(plan.Placements)),
		Components: make(map[topology.ComponentID]topology.PlacementID, len(plan.Components)),
		Dependencies: make(
			map[topology.ComponentID]map[topology.ComponentID]topology.BindingMode,
			len(plan.Dependencies),
		),
	}
	if plan.Traffic != nil {
		result.Traffic = make([]topology.EpochWeight, len(plan.Traffic))
		copy(result.Traffic, plan.Traffic)
	}
	for placement := range plan.Placements {
		result.Placements[placement] = struct{}{}
	}
	for component, placement := range plan.Components {
		result.Components[component] = placement
	}
	for source, targets := range plan.Dependencies {
		result.Dependencies[source] = make(
			map[topology.ComponentID]topology.BindingMode,
			len(targets),
		)
		for target, mode := range targets {
			result.Dependencies[source][target] = mode
		}
	}
	return result
}

func rejectDuplicateKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	return requireDocumentEnd(decoder)
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
			keyToken, keyErr := decoder.Token()
			key, ok := keyToken.(string)
			if keyErr != nil || !ok {
				return ErrInvalidCandidate
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrInvalidCandidate
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return ErrInvalidCandidate
	}
	_, err = decoder.Token()
	return err
}

func requireDocumentEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidCandidate
	}
	return nil
}

func isNilControlValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
