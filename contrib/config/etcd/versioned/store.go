package etcdversioned

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	core "github.com/keelab/keelith/config/versioned"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"
)

const (
	defaultMaxBytes = 1024 * 1024
	maxMaxBytes     = 1024 * 1024
	recordVersion   = 1
)

var (
	// ErrInvalidOption reports an invalid backend, prefix, clock, or budget.
	ErrInvalidOption = errors.New("versioned etcd config: invalid option")
	// ErrInvalidDocument reports malformed json/yaml or a non-object root.
	ErrInvalidDocument = errors.New("versioned etcd config: invalid document")
	// ErrTooLarge reports a candidate beyond the configured byte budget.
	ErrTooLarge = errors.New("versioned etcd config: document is too large")
	// ErrWatchClosed reports an unexpected active-pointer watch closure.
	ErrWatchClosed = errors.New("versioned etcd config: watch closed")
)

// Options configure one isolated versioned configuration namespace.
type Options struct {
	Prefix      string
	MaxBytes    int
	OwnsBackend bool
	Now         func() time.Time
}

// Description is a bounded, content-free Store snapshot.
type Description struct {
	Closed        bool
	MaxBytes      int
	OwnsBackend   bool
	HasActive     bool
	Generation    uint64
	RevisionCount uint64
	Activations   uint64
}

type revisionEnvelope struct {
	Version  int           `json:"version"`
	Revision core.Revision `json:"revision"`
	Content  []byte        `json:"content"`
}

type activationEnvelope struct {
	Version    int             `json:"version"`
	Activation core.Activation `json:"activation"`
}

// Store owns immutable revision and activation operations for one prefix.
type Store struct {
	backend     Backend
	prefix      string
	maxBytes    int
	ownsBackend bool
	now         func() time.Time

	mu          sync.Mutex
	closed      bool
	hasActive   bool
	generation  uint64
	revisions   uint64
	activations uint64
	closeOnce   sync.Once
	closeErr    error
}

// New constructs a Store around the official etcd v3 client.
func New(client *clientv3.Client, options Options) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidOption)
	}
	return Wrap(&sdkBackend{client: client}, options)
}

// Wrap constructs a Store around a custom atomic backend.
func Wrap(backend Backend, options Options) (*Store, error) {
	if isNilBackend(backend) {
		return nil, fmt.Errorf("%w: backend is nil", ErrInvalidOption)
	}
	prefix := strings.TrimSuffix(strings.TrimSpace(options.Prefix), "/")
	if !validPrefix(prefix) {
		return nil, fmt.Errorf("%w: prefix %q", ErrInvalidOption, prefix)
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxBytes
	}
	if maxBytes < 1 || maxBytes > maxMaxBytes {
		return nil, fmt.Errorf("%w: max bytes %d", ErrInvalidOption, maxBytes)
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Store{
		backend:     backend,
		prefix:      prefix,
		maxBytes:    maxBytes,
		ownsBackend: options.OwnsBackend,
		now:         now,
	}, nil
}

// Stage validates and create-only stores one content-addressed revision.
func (store *Store) Stage(
	ctx context.Context,
	request core.StageRequest,
) (core.Revision, error) {
	if err := store.require(ctx); err != nil {
		return core.Revision{}, err
	}
	request = request.Clone()
	if err := request.Validate(); err != nil {
		return core.Revision{}, err
	}
	if len(request.Content) > store.maxBytes {
		return core.Revision{}, fmt.Errorf("%w: %d bytes exceeds %d", ErrTooLarge, len(request.Content), store.maxBytes)
	}
	if _, err := decodeDocument(request.Content, request.Format, store.maxBytes); err != nil {
		return core.Revision{}, err
	}
	revision := core.Revision{
		ID:        core.RevisionID(request.Content),
		Format:    request.Format,
		Size:      len(request.Content),
		CreatedAt: store.now().UTC(),
		Actor:     strings.TrimSpace(request.Actor),
		Message:   strings.TrimSpace(request.Message),
	}
	if err := revision.Validate(); err != nil {
		return core.Revision{}, err
	}
	payload, err := json.Marshal(revisionEnvelope{
		Version: recordVersion, Revision: revision, Content: request.Content,
	})
	if err != nil {
		return core.Revision{}, fmt.Errorf("versioned etcd config: encode revision: %w", err)
	}
	stored, created, err := store.backend.Create(ctx, store.revisionKey(revision.ID), payload)
	if err != nil {
		return core.Revision{}, fmt.Errorf("versioned etcd config: stage: %w", err)
	}
	if !created {
		existing, _, decodeErr := store.decodeRevision(stored.Value, revision.ID)
		if decodeErr != nil {
			return core.Revision{}, decodeErr
		}
		return existing, nil
	}
	store.mu.Lock()
	store.revisions++
	store.mu.Unlock()
	return revision, nil
}

// Revision returns immutable metadata and an independent content copy.
func (store *Store) Revision(
	ctx context.Context,
	id string,
) (core.Revision, []byte, error) {
	if err := store.require(ctx); err != nil {
		return core.Revision{}, nil, err
	}
	if !core.ValidRevisionID(id) {
		return core.Revision{}, nil, core.ErrInvalidRequest
	}
	value, found, err := store.backend.Get(ctx, store.revisionKey(id))
	if err != nil {
		return core.Revision{}, nil, fmt.Errorf("versioned etcd config: read revision: %w", err)
	}
	if !found {
		return core.Revision{}, nil, core.ErrNotFound
	}
	return store.decodeRevision(value.Value, id)
}

// Active returns the current activation pointer without configuration values.
func (store *Store) Active(ctx context.Context) (core.Activation, error) {
	activation, _, err := store.active(ctx)
	return activation, err
}

// Activate atomically selects a staged revision and appends history.
func (store *Store) Activate(
	ctx context.Context,
	request core.ActivateRequest,
) (core.Activation, error) {
	if err := store.require(ctx); err != nil {
		return core.Activation{}, err
	}
	request.Actor = strings.TrimSpace(request.Actor)
	request.Reason = strings.TrimSpace(request.Reason)
	if err := request.Validate(); err != nil {
		return core.Activation{}, err
	}
	if _, _, err := store.Revision(ctx, request.Revision); err != nil {
		return core.Activation{}, err
	}
	current, currentModRevision, err := store.active(ctx)
	if err != nil && !errors.Is(err, core.ErrNoActive) {
		return core.Activation{}, err
	}
	if errors.Is(err, core.ErrNoActive) {
		current = core.Activation{}
		currentModRevision = 0
	}
	if current.Generation != request.ExpectedGeneration {
		return core.Activation{}, fmt.Errorf("%w: expected generation %d, current %d", core.ErrConflict, request.ExpectedGeneration, current.Generation)
	}
	if current.Revision == request.Revision {
		return current, nil
	}
	if current.Generation == math.MaxUint64 {
		return core.Activation{}, fmt.Errorf("%w: generation exhausted", core.ErrConflict)
	}
	activation := core.Activation{
		Generation:  current.Generation + 1,
		Revision:    request.Revision,
		Previous:    current.Revision,
		ActivatedAt: store.now().UTC(),
		Actor:       request.Actor,
		Reason:      request.Reason,
	}
	if err := activation.Validate(); err != nil {
		return core.Activation{}, err
	}
	payload, err := encodeActivation(activation)
	if err != nil {
		return core.Activation{}, err
	}
	_, committed, err := store.backend.CommitActivation(
		ctx,
		store.revisionKey(request.Revision),
		store.activeKey(),
		currentModRevision,
		payload,
		store.historyKey(activation.Generation),
		payload,
	)
	if err != nil {
		return core.Activation{}, fmt.Errorf("versioned etcd config: activate: %w", err)
	}
	if !committed {
		return core.Activation{}, core.ErrConflict
	}
	store.mu.Lock()
	store.hasActive = true
	store.generation = activation.Generation
	store.activations++
	store.mu.Unlock()
	return activation, nil
}

// History returns newest-first bounded activation records.
func (store *Store) History(
	ctx context.Context,
	limit int,
) ([]core.Activation, error) {
	if err := store.require(ctx); err != nil {
		return nil, err
	}
	normalized, err := core.NormalizeHistoryLimit(limit)
	if err != nil {
		return nil, err
	}
	values, err := store.backend.List(ctx, store.historyPrefix(), normalized)
	if err != nil {
		return nil, fmt.Errorf("versioned etcd config: history: %w", err)
	}
	history := make([]core.Activation, len(values))
	for index, value := range values {
		activation, decodeErr := decodeActivation(value.Value)
		if decodeErr != nil {
			return nil, decodeErr
		}
		history[index] = activation
	}
	return history, nil
}

// Describe returns bounded state without prefix, ids, content, actors, or reasons.
func (store *Store) Describe() Description {
	if store == nil {
		return Description{Closed: true}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return Description{
		Closed: store.closed, MaxBytes: store.maxBytes,
		OwnsBackend: store.ownsBackend, HasActive: store.hasActive,
		Generation: store.generation, RevisionCount: store.revisions,
		Activations: store.activations,
	}
}

// Close closes the logical Store and its backend only when explicitly owned.
func (store *Store) Close() error {
	if store == nil {
		return nil
	}
	store.closeOnce.Do(func() {
		store.mu.Lock()
		store.closed = true
		store.mu.Unlock()
		if store.ownsBackend {
			store.closeErr = store.backend.Close()
		}
	})
	return store.closeErr
}

func (store *Store) active(ctx context.Context) (core.Activation, int64, error) {
	if err := store.require(ctx); err != nil {
		return core.Activation{}, 0, err
	}
	value, found, err := store.backend.Get(ctx, store.activeKey())
	if err != nil {
		return core.Activation{}, 0, fmt.Errorf("versioned etcd config: read active: %w", err)
	}
	if !found {
		return core.Activation{}, 0, core.ErrNoActive
	}
	activation, err := decodeActivation(value.Value)
	if err != nil {
		return core.Activation{}, 0, err
	}
	store.mu.Lock()
	store.hasActive = true
	store.generation = activation.Generation
	store.mu.Unlock()
	return activation, value.ModRevision, nil
}

func (store *Store) decodeRevision(payload []byte, expectedid string) (core.Revision, []byte, error) {
	var envelope revisionEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil || envelope.Version != recordVersion {
		return core.Revision{}, nil, fmt.Errorf("%w: malformed revision envelope", core.ErrTampered)
	}
	if err := envelope.Revision.Validate(); err != nil || envelope.Revision.ID != expectedid ||
		envelope.Revision.Size != len(envelope.Content) ||
		core.RevisionID(envelope.Content) != envelope.Revision.ID || len(envelope.Content) > store.maxBytes {
		return core.Revision{}, nil, core.ErrTampered
	}
	if _, err := decodeDocument(envelope.Content, envelope.Revision.Format, store.maxBytes); err != nil {
		return core.Revision{}, nil, fmt.Errorf("%w: %w", core.ErrTampered, err)
	}
	return envelope.Revision, append([]byte(nil), envelope.Content...), nil
}

func (store *Store) require(ctx context.Context) error {
	if store == nil || ctx == nil {
		return ErrInvalidOption
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return core.ErrClosed
	}
	return nil
}

func (store *Store) revisionKey(id string) string { return store.prefix + "/revisions/" + id }
func (store *Store) activeKey() string            { return store.prefix + "/active" }
func (store *Store) historyPrefix() string        { return store.prefix + "/history/" }
func (store *Store) historyKey(generation uint64) string {
	return fmt.Sprintf("%s%020d", store.historyPrefix(), generation)
}

func encodeActivation(activation core.Activation) ([]byte, error) {
	payload, err := json.Marshal(activationEnvelope{Version: recordVersion, Activation: activation})
	if err != nil {
		return nil, fmt.Errorf("versioned etcd config: encode activation: %w", err)
	}
	return payload, nil
}

func decodeActivation(payload []byte) (core.Activation, error) {
	var envelope activationEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil || envelope.Version != recordVersion {
		return core.Activation{}, fmt.Errorf("%w: malformed activation envelope", core.ErrTampered)
	}
	if err := envelope.Activation.Validate(); err != nil {
		return core.Activation{}, fmt.Errorf("%w: %w", core.ErrTampered, err)
	}
	return envelope.Activation, nil
}

func decodeDocument(content []byte, format core.Format, maxBytes int) (map[string]any, error) {
	if len(content) > maxBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", ErrTooLarge, len(content), maxBytes)
	}
	var values map[string]any
	switch format {
	case core.FormatJSON:
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.UseNumber()
		if err := decoder.Decode(&values); err != nil {
			return nil, fmt.Errorf("%w: json: %w", ErrInvalidDocument, err)
		}
		if err := requireEOF(decoder.Decode(new(any))); err != nil {
			return nil, fmt.Errorf("%w: json: %w", ErrInvalidDocument, err)
		}
	case core.FormatYAML:
		decoder := yaml.NewDecoder(bytes.NewReader(content))
		if err := decoder.Decode(&values); err != nil {
			return nil, fmt.Errorf("%w: yaml: %w", ErrInvalidDocument, err)
		}
		var extra any
		if err := requireEOF(decoder.Decode(&extra)); err != nil {
			return nil, fmt.Errorf("%w: yaml: %w", ErrInvalidDocument, err)
		}
	default:
		return nil, fmt.Errorf("%w: format %q", ErrInvalidDocument, format)
	}
	if values == nil {
		return nil, fmt.Errorf("%w: root must be an object", ErrInvalidDocument)
	}
	return values, nil
}

func requireEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple documents are not allowed")
	}
	return err
}

func validPrefix(prefix string) bool {
	if len(prefix) < 2 || len(prefix) > 512 || prefix[0] != '/' ||
		!utf8.ValidString(prefix) || strings.Contains(prefix, "//") {
		return false
	}
	for _, character := range prefix {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func isNilBackend(backend Backend) bool {
	if backend == nil {
		return true
	}
	value := reflect.ValueOf(backend)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ core.Store = (*Store)(nil)
