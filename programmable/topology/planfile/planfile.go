// Package planfile defines Keelith's strict, canonical topology plan document.
package planfile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/keelab/keelith/programmable/topology"
)

const (
	// APIVersion is the stable topology document contract.
	APIVersion = "keelith.dev/topology/v1alpha1"

	maximumDocumentBytes = 1024 * 1024
	maximumPlacements    = 1024
	maximumComponents    = 4096
	maximumDependencies  = 16 * 1024
	maximumTrafficEpochs = 64
)

var (
	// ErrInvalidDocument reports malformed JSON or an invalid topology plan.
	ErrInvalidDocument = errors.New("topology planfile: invalid document")
	// ErrDocumentTooLarge reports input beyond the one MiB plan budget.
	ErrDocumentTooLarge = errors.New(
		"topology planfile: document too large",
	)
)

type document struct {
	APIVersion   string       `json:"apiVersion"`
	Epoch        uint64       `json:"epoch"`
	Placements   []string     `json:"placements"`
	Components   []component  `json:"components"`
	Dependencies []dependency `json:"dependencies"`
	Traffic      []traffic    `json:"traffic,omitempty"`
}

type component struct {
	ID        string `json:"id"`
	Placement string `json:"placement"`
}

type dependency struct {
	Source string               `json:"source"`
	Target string               `json:"target"`
	Mode   topology.BindingMode `json:"mode"`
}

type traffic struct {
	Epoch       uint64 `json:"epoch"`
	BasisPoints uint16 `json:"basisPoints"`
}

// Parse strictly decodes, bounds, deduplicates, and validates one document.
func Parse(payload []byte) (topology.Plan, error) {
	if len(payload) == 0 {
		return topology.Plan{}, ErrInvalidDocument
	}
	if len(payload) > maximumDocumentBytes {
		return topology.Plan{}, ErrDocumentTooLarge
	}
	if err := rejectDuplicateKeys(payload); err != nil {
		return topology.Plan{}, ErrInvalidDocument
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var input document
	if err := decoder.Decode(&input); err != nil {
		return topology.Plan{}, ErrInvalidDocument
	}
	if err := requireEnd(decoder); err != nil {
		return topology.Plan{}, ErrInvalidDocument
	}
	if input.APIVersion != APIVersion ||
		input.Placements == nil ||
		input.Components == nil ||
		input.Dependencies == nil ||
		len(input.Placements) == 0 ||
		len(input.Placements) > maximumPlacements ||
		len(input.Components) == 0 ||
		len(input.Components) > maximumComponents ||
		len(input.Dependencies) > maximumDependencies ||
		len(input.Traffic) > maximumTrafficEpochs {
		return topology.Plan{}, ErrInvalidDocument
	}

	plan := topology.Plan{
		Epoch:      input.Epoch,
		Placements: make(map[topology.PlacementID]struct{}),
		Components: make(map[topology.ComponentID]topology.PlacementID),
		Dependencies: make(
			map[topology.ComponentID]map[topology.ComponentID]topology.BindingMode,
		),
	}
	for _, value := range input.Placements {
		placement := topology.PlacementID(value)
		if _, duplicate := plan.Placements[placement]; duplicate {
			return topology.Plan{}, ErrInvalidDocument
		}
		plan.Placements[placement] = struct{}{}
	}
	for _, value := range input.Components {
		id := topology.ComponentID(value.ID)
		if _, duplicate := plan.Components[id]; duplicate {
			return topology.Plan{}, ErrInvalidDocument
		}
		plan.Components[id] = topology.PlacementID(value.Placement)
	}
	for _, value := range input.Dependencies {
		source := topology.ComponentID(value.Source)
		target := topology.ComponentID(value.Target)
		targets := plan.Dependencies[source]
		if targets == nil {
			targets = make(map[topology.ComponentID]topology.BindingMode)
			plan.Dependencies[source] = targets
		}
		if _, duplicate := targets[target]; duplicate {
			return topology.Plan{}, ErrInvalidDocument
		}
		targets[target] = value.Mode
	}
	if input.Traffic != nil {
		plan.Traffic = make([]topology.EpochWeight, 0, len(input.Traffic))
		for _, value := range input.Traffic {
			plan.Traffic = append(plan.Traffic, topology.EpochWeight{
				Epoch: value.Epoch, BasisPoints: value.BasisPoints,
			})
		}
	}
	if _, err := topology.Activate(plan); err != nil {
		return topology.Plan{}, fmt.Errorf(
			"%w: %w",
			ErrInvalidDocument,
			err,
		)
	}
	return plan, nil
}

// Marshal validates and emits one deterministic newline-terminated document.
func Marshal(plan topology.Plan) ([]byte, error) {
	if len(plan.Placements) == 0 ||
		len(plan.Placements) > maximumPlacements ||
		len(plan.Components) == 0 ||
		len(plan.Components) > maximumComponents {
		return nil, ErrInvalidDocument
	}
	if _, err := topology.Activate(plan); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDocument, err)
	}
	output := document{
		APIVersion:   APIVersion,
		Epoch:        plan.Epoch,
		Placements:   make([]string, 0, len(plan.Placements)),
		Components:   make([]component, 0, len(plan.Components)),
		Dependencies: make([]dependency, 0),
	}
	if plan.Traffic != nil {
		output.Traffic = make([]traffic, 0, len(plan.Traffic))
		for _, weight := range plan.Traffic {
			output.Traffic = append(output.Traffic, traffic{
				Epoch: weight.Epoch, BasisPoints: weight.BasisPoints,
			})
		}
		sort.Slice(output.Traffic, func(left, right int) bool {
			return output.Traffic[left].Epoch < output.Traffic[right].Epoch
		})
	}
	for placement := range plan.Placements {
		output.Placements = append(output.Placements, string(placement))
	}
	sort.Strings(output.Placements)
	for id, placement := range plan.Components {
		output.Components = append(output.Components, component{
			ID:        string(id),
			Placement: string(placement),
		})
	}
	sort.Slice(output.Components, func(left, right int) bool {
		return output.Components[left].ID < output.Components[right].ID
	})
	for source, targets := range plan.Dependencies {
		for target, mode := range targets {
			output.Dependencies = append(output.Dependencies, dependency{
				Source: string(source),
				Target: string(target),
				Mode:   mode,
			},
			)
		}
	}
	if len(output.Dependencies) > maximumDependencies {
		return nil, ErrInvalidDocument
	}
	sort.Slice(output.Dependencies, func(left, right int) bool {
		first := output.Dependencies[left]
		second := output.Dependencies[right]
		if first.Source != second.Source {
			return first.Source < second.Source
		}
		if first.Target != second.Target {
			return first.Target < second.Target
		}
		return first.Mode < second.Mode
	})
	payload, err := json.Marshal(output)
	if err != nil {
		return nil, ErrInvalidDocument
	}
	payload = append(payload, '\n')
	if len(payload) > maximumDocumentBytes {
		return nil, ErrDocumentTooLarge
	}
	return payload, nil
}

func rejectDuplicateKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := scanValue(decoder); err != nil {
		return err
	}
	return requireEnd(decoder)
}

func scanValue(decoder *json.Decoder) error {
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
				return ErrInvalidDocument
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrInvalidDocument
			}
			seen[key] = struct{}{}
			if err := scanValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanValue(decoder); err != nil {
				return err
			}
		}
	default:
		return ErrInvalidDocument
	}
	_, err = decoder.Token()
	return err
}

func requireEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidDocument
	}
	return nil
}
