package control

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/keelab/keelith/programmable/topology"
)

const maximumRollbackWindow = 1_000_000

var (
	// ErrInvalidRollback reports unsafe bounds or incomplete rollback wiring.
	ErrInvalidRollback = errors.New("topology control: invalid rollback")
	// ErrInvalidOutcome rejects values outside the fixed low-cardinality set.
	ErrInvalidOutcome = errors.New("topology control: invalid rollout outcome")
)

// ResultClass is a fixed, low-cardinality request result.
type ResultClass string

const (
	// ResultSuccess reports a successful call.
	ResultSuccess ResultClass = "success"
	// ResultError reports a failed call without retaining the raw error.
	ResultError ResultClass = "error"
)

// LatencyClass is a fixed, low-cardinality latency bucket.
type LatencyClass string

const (
	// LatencyNormal reports a call within the rollout latency objective.
	LatencyNormal LatencyClass = "normal"
	// LatencySlow reports a call outside the rollout latency objective.
	LatencySlow LatencyClass = "slow"
)

// RolloutOutcome intentionally contains neither business identity nor raw
// errors. Callers classify those values before crossing the control boundary.
type RolloutOutcome struct {
	Result  ResultClass
	Latency LatencyClass
}

// Publisher atomically exposes one complete rollback candidate.
type Publisher interface {
	Publish(context.Context, Candidate) error
}

// PublisherFunc adapts a publication function.
type PublisherFunc func(context.Context, Candidate) error

// Publish invokes the adapted publisher.
func (function PublisherFunc) Publish(
	ctx context.Context,
	candidate Candidate,
) error {
	return function(ctx, candidate)
}

type filePublisher struct {
	path string
	mu   sync.Mutex
}

// NewFilePublisher creates an atomic, owner-only rollback publisher accepted
// by NewFileSource. The destination directory must already exist.
func NewFilePublisher(path string) (Publisher, error) {
	if path == "" || strings.TrimSpace(path) != path ||
		filepath.Base(filepath.Clean(path)) == "." {
		return nil, ErrInvalidRollback
	}
	return &filePublisher{path: filepath.Clean(path)}, nil
}

func (publisher *filePublisher) Publish(
	ctx context.Context,
	candidate Candidate,
) (resultErr error) {
	if publisher == nil || ctx == nil {
		return ErrInvalidRollback
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	payload, err := MarshalDocument(candidate)
	if err != nil {
		return err
	}
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	directory := filepath.Dir(publisher.path)
	temporary, err := os.CreateTemp(directory, ".keelith-rollback-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	temporaryClosed := false
	defer func() {
		var closeErr error
		if !temporaryClosed {
			closeErr = temporary.Close()
		}
		removeErr := os.Remove(temporaryPath)
		if !errors.Is(removeErr, os.ErrNotExist) {
			resultErr = errors.Join(resultErr, removeErr)
		}
		resultErr = errors.Join(resultErr, closeErr)
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err = temporary.Write(payload); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	temporaryClosed = true
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err = os.Rename(temporaryPath, publisher.path); err != nil {
		return err
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return err
	}
	syncErr := directoryHandle.Sync()
	closeErr := directoryHandle.Close()
	return errors.Join(syncErr, closeErr)
}

// Signer signs a candidate's canonical SigningBytes.
type Signer interface {
	Sign(context.Context, []byte) ([]byte, error)
}

// SignerFunc adapts a detached-signature function.
type SignerFunc func(context.Context, []byte) ([]byte, error)

// Sign invokes the adapted signer.
func (function SignerFunc) Sign(ctx context.Context, payload []byte) ([]byte, error) {
	return function(ctx, payload)
}

type ed25519Signer struct{ privateKey ed25519.PrivateKey }

// NewEd25519Signer snapshots one private key for rollback publication.
func NewEd25519Signer(privateKey ed25519.PrivateKey) (Signer, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, ErrInvalidSignature
	}
	return ed25519Signer{
		privateKey: append(ed25519.PrivateKey(nil), privateKey...),
	}, nil
}

func (signer ed25519Signer) Sign(
	ctx context.Context,
	payload []byte,
) ([]byte, error) {
	if ctx == nil {
		return nil, ErrInvalidSignature
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	return ed25519.Sign(signer.privateKey, payload), nil
}

// RollbackConfig defines a fixed health window and strict thresholds.
type RollbackConfig struct {
	WindowSize              uint32
	MinimumSamples          uint32
	MaximumErrorBasisPoints uint16
	MaximumSlowBasisPoints  uint16
	Publisher               Publisher
	Signer                  Signer
	AllowUnsigned           bool
}

// RollbackStatus is a bounded, payload-free health and publication summary.
type RollbackStatus struct {
	Armed             bool
	Triggered         bool
	Samples           uint32
	ErrorBasisPoints  uint16
	SlowBasisPoints   uint16
	CandidateRevision uint64
	PublishedRevision uint64
}

// RollbackController publishes a new immutable revision after a fixed health
// window crosses an error or latency threshold.
type RollbackController struct {
	windowSize     uint32
	minimumSamples uint32
	maximumError   uint16
	maximumSlow    uint16
	publisher      Publisher
	signer         Signer
	allowUnsigned  bool

	mu         sync.Mutex
	window     []RolloutOutcome
	next       uint32
	errors     uint32
	slow       uint32
	lastGood   Candidate
	candidate  Candidate
	armed      bool
	publishing bool
	triggered  bool
	published  uint64
}

// NewRollbackController validates and freezes one rollback policy.
func NewRollbackController(config RollbackConfig) (*RollbackController, error) {
	if config.WindowSize == 0 || config.WindowSize > maximumRollbackWindow ||
		config.MinimumSamples == 0 || config.MinimumSamples > config.WindowSize ||
		config.MaximumErrorBasisPoints > topology.TotalBasisPoints ||
		config.MaximumSlowBasisPoints > topology.TotalBasisPoints ||
		isNilControlValue(config.Publisher) ||
		config.AllowUnsigned == !isNilControlValue(config.Signer) {
		return nil, ErrInvalidRollback
	}
	return &RollbackController{
		windowSize: config.WindowSize, minimumSamples: config.MinimumSamples,
		maximumError: config.MaximumErrorBasisPoints,
		maximumSlow:  config.MaximumSlowBasisPoints,
		publisher:    config.Publisher, signer: config.Signer,
		allowUnsigned: config.AllowUnsigned,
		window:        make([]RolloutOutcome, 0, config.WindowSize),
	}, nil
}

// Arm resets the fixed window around one last-good and one canary candidate.
func (controller *RollbackController) Arm(
	lastGood Candidate,
	candidate Candidate,
) error {
	if controller == nil || lastGood.Revision() == 0 || candidate.Revision() == 0 ||
		lastGood.Hash() == "" || candidate.Hash() == "" ||
		candidate.Revision() <= lastGood.Revision() ||
		candidate.Epoch() <= lastGood.Epoch() ||
		candidate.Revision() == ^uint64(0) || candidate.Epoch() == ^uint64(0) {
		return ErrInvalidRollback
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.publishing {
		return ErrInvalidRollback
	}
	controller.window = controller.window[:0]
	controller.next = 0
	controller.errors = 0
	controller.slow = 0
	controller.lastGood = lastGood
	controller.candidate = candidate
	controller.armed = true
	controller.triggered = false
	controller.published = 0
	return nil
}

// Observe adds one classified result and publishes at most one rollback for
// the current arm. A failed publication remains retryable on the next sample.
func (controller *RollbackController) Observe(
	ctx context.Context,
	outcome RolloutOutcome,
) error {
	if controller == nil || ctx == nil {
		return ErrInvalidRollback
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if !validOutcome(outcome) {
		return ErrInvalidOutcome
	}
	controller.mu.Lock()
	if !controller.armed {
		controller.mu.Unlock()
		return ErrInvalidRollback
	}
	if controller.triggered || controller.publishing {
		controller.mu.Unlock()
		return nil
	}
	controller.push(outcome)
	errorBasisPoints, slowBasisPoints := controller.rates()
	thresholdCrossed := uint32(len(controller.window)) >= controller.minimumSamples &&
		(errorBasisPoints > controller.maximumError ||
			slowBasisPoints > controller.maximumSlow)
	if !thresholdCrossed {
		controller.mu.Unlock()
		return nil
	}
	controller.publishing = true
	lastGood := controller.lastGood
	candidate := controller.candidate
	controller.mu.Unlock()

	rollback, err := controller.build(ctx, lastGood, candidate)
	if err == nil {
		err = controller.publisher.Publish(ctx, rollback)
	}
	controller.mu.Lock()
	controller.publishing = false
	if err == nil {
		controller.triggered = true
		controller.published = rollback.Revision()
	}
	controller.mu.Unlock()
	if err != nil {
		return fmt.Errorf("%w: publish: %w", ErrInvalidRollback, err)
	}
	return nil
}

// Status returns a bounded snapshot without raw errors or business keys.
func (controller *RollbackController) Status() RollbackStatus {
	if controller == nil {
		return RollbackStatus{}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	errorBasisPoints, slowBasisPoints := controller.rates()
	return RollbackStatus{
		Armed: controller.armed, Triggered: controller.triggered,
		Samples:          uint32(len(controller.window)),
		ErrorBasisPoints: errorBasisPoints, SlowBasisPoints: slowBasisPoints,
		CandidateRevision: controller.candidate.Revision(),
		PublishedRevision: controller.published,
	}
}

func (controller *RollbackController) push(outcome RolloutOutcome) {
	if uint32(len(controller.window)) < controller.windowSize {
		controller.window = append(controller.window, outcome)
		controller.add(outcome)
		return
	}
	removed := controller.window[controller.next]
	controller.remove(removed)
	controller.window[controller.next] = outcome
	controller.add(outcome)
	controller.next = (controller.next + 1) % controller.windowSize
}

func (controller *RollbackController) add(outcome RolloutOutcome) {
	if outcome.Result == ResultError {
		controller.errors++
	}
	if outcome.Latency == LatencySlow {
		controller.slow++
	}
}

func (controller *RollbackController) remove(outcome RolloutOutcome) {
	if outcome.Result == ResultError {
		controller.errors--
	}
	if outcome.Latency == LatencySlow {
		controller.slow--
	}
}

func (controller *RollbackController) rates() (uint16, uint16) {
	if len(controller.window) == 0 {
		return 0, 0
	}
	samples := uint64(len(controller.window))
	return uint16(uint64(controller.errors) * uint64(topology.TotalBasisPoints) / samples),
		uint16(uint64(controller.slow) * uint64(topology.TotalBasisPoints) / samples)
}

func (controller *RollbackController) build(
	ctx context.Context,
	lastGood Candidate,
	candidate Candidate,
) (Candidate, error) {
	plan := lastGood.Plan()
	plan.Epoch = candidate.Epoch() + 1
	plan.Traffic = []topology.EpochWeight{
		{Epoch: candidate.Epoch(), BasisPoints: 0},
		{Epoch: plan.Epoch, BasisPoints: topology.TotalBasisPoints},
	}
	rollback, err := NewCandidate(CandidateSpec{
		Revision: candidate.Revision() + 1,
		Plan:     plan,
	})
	if err != nil {
		return Candidate{}, err
	}
	if controller.allowUnsigned {
		return rollback, nil
	}
	signature, err := controller.signer.Sign(ctx, rollback.SigningBytes())
	if err != nil {
		return Candidate{}, err
	}
	return NewCandidate(CandidateSpec{
		Revision: rollback.Revision(), Plan: plan,
		Hash: rollback.Hash(), Signature: signature,
	})
}

func validOutcome(outcome RolloutOutcome) bool {
	return (outcome.Result == ResultSuccess || outcome.Result == ResultError) &&
		(outcome.Latency == LatencyNormal || outcome.Latency == LatencySlow)
}
