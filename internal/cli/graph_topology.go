package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/keelab/keelith/programmable/topology"
	"github.com/keelab/keelith/programmable/topology/planfile"
)

const maxGraphTopologyPlanSize = 1024 * 1024

type graphTopology struct {
	Plan       string                   `json:"plan"`
	APIVersion string                   `json:"apiVersion"`
	Epoch      uint64                   `json:"epoch"`
	Hash       string                   `json:"hash"`
	Components []graphTopologyComponent `json:"components"`
	Bindings   []graphTopologyBinding   `json:"bindings"`
}

type graphTopologyComponent struct {
	ID        string `json:"id"`
	Placement string `json:"placement"`
}

type graphTopologyBinding struct {
	Source          string `json:"source"`
	Target          string `json:"target"`
	Constraint      string `json:"constraint"`
	Mode            string `json:"mode"`
	SourcePlacement string `json:"sourcePlacement"`
	TargetPlacement string `json:"targetPlacement"`
}

func loadGraphTopologyPlan(
	ctx context.Context,
	project string,
	relative string,
) (topology.Plan, error) {
	if cause := context.Cause(ctx); cause != nil {
		return topology.Plan{}, cause
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if filepath.IsAbs(clean) ||
		clean == ".." ||
		len(clean) > 3 && clean[:3] == ".."+string(filepath.Separator) {
		return topology.Plan{}, fmt.Errorf(
			"topology plan %q escapes the project root",
			relative,
		)
	}
	path, err := safeProjectFile(project, relative, false)
	if err != nil {
		return topology.Plan{}, fmt.Errorf("topology plan: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return topology.Plan{}, fmt.Errorf("inspect topology plan %q: %w", relative, err)
	}
	if !info.Mode().IsRegular() {
		return topology.Plan{}, fmt.Errorf(
			"topology plan %q is not a regular file",
			relative,
		)
	}
	if info.Size() <= 0 {
		return topology.Plan{}, fmt.Errorf("topology plan %q is empty", relative)
	}
	if info.Size() > maxGraphTopologyPlanSize {
		return topology.Plan{}, fmt.Errorf(
			"topology plan %q is too large; limit is %d bytes",
			relative,
			maxGraphTopologyPlanSize,
		)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return topology.Plan{}, fmt.Errorf("read topology plan %q: %w", relative, err)
	}
	if cause := context.Cause(ctx); cause != nil {
		return topology.Plan{}, cause
	}
	plan, err := planfile.Parse(payload)
	if err != nil {
		return topology.Plan{}, fmt.Errorf("parse topology plan %q: %w", relative, err)
	}
	return plan, nil
}

func attachGraphTopology(
	graph graphResult,
	relative string,
	plan topology.Plan,
) (graphResult, error) {
	snapshot, err := topology.Activate(plan)
	if err != nil {
		return graphResult{}, fmt.Errorf("activate topology plan %q: %w", relative, err)
	}
	componentIDs := make([]topology.ComponentID, 0, len(plan.Components))
	for component := range plan.Components {
		componentIDs = append(componentIDs, component)
	}
	sort.Slice(componentIDs, func(left, right int) bool {
		return componentIDs[left] < componentIDs[right]
	})
	components := make([]graphTopologyComponent, 0, len(componentIDs))
	for _, component := range componentIDs {
		components = append(components, graphTopologyComponent{
			ID:        string(component),
			Placement: string(plan.Components[component]),
		})
	}

	type dependency struct {
		source     topology.ComponentID
		target     topology.ComponentID
		constraint topology.BindingMode
	}
	dependencies := make([]dependency, 0)
	for source, targets := range plan.Dependencies {
		for target, constraint := range targets {
			dependencies = append(dependencies, dependency{
				source:     source,
				target:     target,
				constraint: constraint,
			})
		}
	}
	sort.Slice(dependencies, func(left, right int) bool {
		if dependencies[left].source != dependencies[right].source {
			return dependencies[left].source < dependencies[right].source
		}
		return dependencies[left].target < dependencies[right].target
	})
	bindings := make([]graphTopologyBinding, 0, len(dependencies))
	for _, dependency := range dependencies {
		resolved, err := snapshot.Resolve(dependency.source, dependency.target)
		if err != nil {
			return graphResult{}, fmt.Errorf(
				"resolve topology binding %q -> %q: %w",
				dependency.source,
				dependency.target,
				err,
			)
		}
		bindings = append(bindings, graphTopologyBinding{
			Source:          string(resolved.Source),
			Target:          string(resolved.Target),
			Constraint:      string(dependency.constraint),
			Mode:            string(resolved.Mode),
			SourcePlacement: string(resolved.SourcePlacement),
			TargetPlacement: string(resolved.TargetPlacement),
		})
	}

	graph.SchemaVersion = "v3"
	graph.Topology = &graphTopology{
		Plan:       filepath.ToSlash(relative),
		APIVersion: planfile.APIVersion,
		Epoch:      snapshot.Epoch(),
		Hash:       snapshot.Hash(),
		Components: components,
		Bindings:   bindings,
	}
	return graph, nil
}
