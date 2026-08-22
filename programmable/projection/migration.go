package projection

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidMigration reports malformed, duplicate, or cyclic steps.
	ErrInvalidMigration = errors.New("projection: invalid migration")
	// ErrMigrationMissing reports a payload fingerprint with no generated step.
	ErrMigrationMissing = errors.New("projection: migration missing")
	// ErrMigrationMajor reports an attempted migration across schema majors.
	ErrMigrationMajor = errors.New("projection: migration crosses schema major")
)

// Upcaster decodes one historical wire payload into the current typed value.
// Generated upcasters use concrete protobuf types and direct field assignment;
// implementations must not infer fields with reflection.
type Upcaster[V any] func([]byte) (V, error)

// MigrationStep is one explicit historical fingerprint -> current contract.
type MigrationStep[V any] struct {
	PreviousFingerprint string
	CurrentFingerprint  string
	PreviousMajor       uint32
	CurrentMajor        uint32
	Upcast              Upcaster[V]
}

// MigrationRegistry is an immutable direct-to-current upcaster graph.
type MigrationRegistry[V any] struct {
	currentFingerprint string
	currentMajor       uint32
	upcasters          map[string]Upcaster[V]
}

// NewMigrationRegistry validates a bounded acyclic direct migration graph.
func NewMigrationRegistry[V any](
	currentFingerprint string,
	currentMajor uint32,
	steps ...MigrationStep[V],
) (*MigrationRegistry[V], error) {
	if !validFingerprint(currentFingerprint) || currentMajor == 0 ||
		len(steps) > maxCompatibleSchemas {
		return nil, ErrInvalidMigration
	}
	result := &MigrationRegistry[V]{
		currentFingerprint: currentFingerprint,
		currentMajor:       currentMajor,
		upcasters:          make(map[string]Upcaster[V], len(steps)),
	}
	for index, step := range steps {
		if !validFingerprint(step.PreviousFingerprint) ||
			!validFingerprint(step.CurrentFingerprint) ||
			step.PreviousFingerprint == step.CurrentFingerprint ||
			step.CurrentFingerprint != currentFingerprint ||
			step.PreviousMajor == 0 || step.CurrentMajor == 0 ||
			step.Upcast == nil {
			return nil, fmt.Errorf("%w: step %d", ErrInvalidMigration, index)
		}
		if step.PreviousMajor != step.CurrentMajor ||
			step.CurrentMajor != currentMajor {
			return nil, fmt.Errorf("%w: step %d", ErrMigrationMajor, index)
		}
		if _, duplicate := result.upcasters[step.PreviousFingerprint]; duplicate {
			return nil, fmt.Errorf("%w: duplicate previous fingerprint", ErrInvalidMigration)
		}
		result.upcasters[step.PreviousFingerprint] = step.Upcast
	}
	return result, nil
}

// CompatibleFingerprints returns an independent canonical compatibility set.
func (registry *MigrationRegistry[V]) CompatibleFingerprints() []string {
	if registry == nil {
		return nil
	}
	values := make([]string, 0, len(registry.upcasters))
	for fingerprint := range registry.upcasters {
		values = append(values, fingerprint)
	}
	set, err := NewFingerprintSet(values...)
	if err != nil {
		return nil
	}
	return set.Values()
}

// Decoders returns independent functions suitable for a typed Replica.
func (registry *MigrationRegistry[V]) Decoders() map[string]ValueDecoder[V] {
	if registry == nil {
		return nil
	}
	result := make(map[string]ValueDecoder[V], len(registry.upcasters))
	for fingerprint, upcast := range registry.upcasters {
		result[fingerprint] = ValueDecoder[V](upcast)
	}
	return result
}

// Upcast decodes one bounded historical payload with its exact generated step.
func (registry *MigrationRegistry[V]) Upcast(
	fingerprint string,
	payload []byte,
) (V, error) {
	var zero V
	if registry == nil || !validFingerprint(fingerprint) ||
		len(payload) > maxValueBytes {
		return zero, ErrInvalidMigration
	}
	upcast := registry.upcasters[fingerprint]
	if upcast == nil {
		return zero, fmt.Errorf("%w: %w", ErrMigrationMissing, &SchemaMismatchError{
			Field:    "migration_fingerprint",
			Expected: registry.currentFingerprint,
			Actual:   fingerprint,
		})
	}
	value, err := upcast(cloneBytes(payload))
	if err != nil {
		return zero, fmt.Errorf("projection: generated upcast: %w", err)
	}
	return value, nil
}
