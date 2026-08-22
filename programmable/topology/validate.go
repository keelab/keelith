package topology

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maxIdentityBytes = 512

// Activate validates, snapshots, and canonically identifies one plan.
func Activate(plan Plan) (Snapshot, error) {
	snapshot := clonePlan(plan)
	if err := validateSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	hash, err := canonicalHash(snapshot)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: canonical hash: %w", ErrInvalidPlan, err)
	}
	snapshot.hash = hash
	return snapshot, nil
}

func clonePlan(plan Plan) Snapshot {
	placements := make(map[PlacementID]struct{}, len(plan.Placements))
	for placement := range plan.Placements {
		placements[placement] = struct{}{}
	}
	components := make(map[ComponentID]PlacementID, len(plan.Components))
	for component, placement := range plan.Components {
		components[component] = placement
	}
	dependencies := make(
		map[ComponentID]map[ComponentID]BindingMode,
		len(plan.Dependencies),
	)
	for source, targets := range plan.Dependencies {
		clonedTargets := make(map[ComponentID]BindingMode, len(targets))
		for target, mode := range targets {
			clonedTargets[target] = mode
		}
		dependencies[source] = clonedTargets
	}
	var traffic []EpochWeight
	if plan.Traffic != nil {
		traffic = make([]EpochWeight, len(plan.Traffic))
		copy(traffic, plan.Traffic)
		sort.Slice(traffic, func(first, second int) bool {
			return traffic[first].Epoch < traffic[second].Epoch
		})
	}
	return Snapshot{
		epoch:        plan.Epoch,
		placements:   placements,
		components:   components,
		dependencies: dependencies,
		traffic:      traffic,
	}
}

func validateSnapshot(snapshot Snapshot) error {
	if snapshot.epoch == 0 {
		return fmt.Errorf("%w: epoch must be positive", ErrInvalidPlan)
	}
	if len(snapshot.placements) == 0 {
		return fmt.Errorf("%w: no placements", ErrInvalidPlan)
	}
	if len(snapshot.components) == 0 {
		return fmt.Errorf("%w: no components", ErrInvalidPlan)
	}
	for placement := range snapshot.placements {
		if !validIdentity(string(placement)) {
			return fmt.Errorf("%w: placement %q is malformed", ErrInvalidPlan, placement)
		}
	}
	for component, placement := range snapshot.components {
		if !validIdentity(string(component)) {
			return fmt.Errorf("%w: component %q is malformed", ErrInvalidPlan, component)
		}
		if _, exists := snapshot.placements[placement]; !exists {
			return fmt.Errorf("%w: component %q references unknown placement %q", ErrInvalidPlan, component, placement)
		}
	}
	for source, targets := range snapshot.dependencies {
		sourcePlacement, exists := snapshot.components[source]
		if !exists {
			return fmt.Errorf("%w: dependency source %q is unknown", ErrInvalidPlan, source)
		}
		for target, mode := range targets {
			targetPlacement, targetExists := snapshot.components[target]
			if !targetExists {
				return fmt.Errorf("%w: dependency %q -> %q references an unknown target", ErrInvalidPlan, source, target)
			}
			switch mode {
			case BindingAuto, BindingRemote:
			case BindingLocal:
				if sourcePlacement != targetPlacement {
					return fmt.Errorf("%w: local dependency %q -> %q crosses placements", ErrInvalidPlan, source, target)
				}
			default:
				return fmt.Errorf("%w: dependency %q -> %q has invalid mode %q", ErrInvalidPlan, source, target, mode)
			}
		}
	}
	if snapshot.traffic != nil {
		if _, err := NewTrafficSelector(snapshot.traffic); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidPlan, err)
		}
		currentWeight := uint16(0)
		for _, weight := range snapshot.traffic {
			if weight.Epoch > snapshot.epoch {
				return fmt.Errorf("%w: traffic epoch %d is newer than plan epoch %d", ErrInvalidPlan, weight.Epoch, snapshot.epoch)
			}
			if weight.Epoch == snapshot.epoch {
				currentWeight = weight.BasisPoints
			}
		}
		if currentWeight == 0 {
			return fmt.Errorf("%w: current epoch %d must have positive traffic", ErrInvalidPlan, snapshot.epoch)
		}
	}
	return nil
}

func validIdentity(value string) bool {
	if value == "" ||
		len(value) > maxIdentityBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

type canonicalPlan struct {
	Epoch        uint64                `json:"epoch"`
	Placements   []PlacementID         `json:"placements"`
	Components   []canonicalComponent  `json:"components"`
	Dependencies []canonicalDependency `json:"dependencies"`
	Traffic      []canonicalTraffic    `json:"traffic,omitempty"`
}

type canonicalComponent struct {
	ID        ComponentID `json:"id"`
	Placement PlacementID `json:"placement"`
}

type canonicalDependency struct {
	Source ComponentID `json:"source"`
	Target ComponentID `json:"target"`
	Mode   BindingMode `json:"mode"`
}

type canonicalTraffic struct {
	Epoch       uint64 `json:"epoch"`
	BasisPoints uint16 `json:"basisPoints"`
}

func canonicalHash(snapshot Snapshot) (string, error) {
	document := canonicalPlan{Epoch: snapshot.epoch}
	for placement := range snapshot.placements {
		document.Placements = append(document.Placements, placement)
	}
	slices.Sort(document.Placements)
	for component, placement := range snapshot.components {
		document.Components = append(document.Components, canonicalComponent{
			ID:        component,
			Placement: placement,
		})
	}
	sort.Slice(document.Components, func(first, second int) bool {
		return document.Components[first].ID < document.Components[second].ID
	})
	for source, targets := range snapshot.dependencies {
		for target, mode := range targets {
			document.Dependencies = append(document.Dependencies, canonicalDependency{
				Source: source,
				Target: target,
				Mode:   mode,
			})
		}
	}

	sort.Slice(document.Dependencies, func(first, second int) bool {
		left := document.Dependencies[first]
		right := document.Dependencies[second]
		if left.Source != right.Source {
			return left.Source < right.Source
		}
		if left.Target != right.Target {
			return left.Target < right.Target
		}
		return left.Mode < right.Mode
	})
	if snapshot.traffic != nil {
		document.Traffic = make([]canonicalTraffic, 0, len(snapshot.traffic))
		for _, weight := range snapshot.traffic {
			document.Traffic = append(document.Traffic, canonicalTraffic(weight))
		}
		sort.Slice(document.Traffic, func(first, second int) bool {
			return document.Traffic[first].Epoch < document.Traffic[second].Epoch
		})
	}
	payload, err := json.Marshal(document)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
