package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/keelab/keelith/contract"
)

const (
	contractManifestSuffix = ".keelith.manifest.json"
	maxProjectManifests    = 256
	maxProjectManifestSize = 4 * 1024 * 1024
)

type graphOptions struct {
	path   string
	plan   string
	format string
}

type graphManifest struct {
	File    string `json:"file"`
	Source  string `json:"source"`
	Package string `json:"package"`
}

type graphService struct {
	Name       string   `json:"name"`
	Source     string   `json:"source"`
	Operations []string `json:"operations"`
	Transports []string `json:"transports"`
	HTTPRoutes int      `json:"httpRoutes"`
}

type graphDependency struct {
	Source     string   `json:"source"`
	Kind       string   `json:"kind"`
	Service    string   `json:"service"`
	Transport  string   `json:"transport"`
	Reason     string   `json:"reason"`
	Operations []string `json:"operations"`
	Resolved   bool     `json:"resolved"`
	Owner      string   `json:"owner,omitempty"`
}

type graphResult struct {
	SchemaVersion string            `json:"schemaVersion"`
	Manifests     []graphManifest   `json:"manifests"`
	Services      []graphService    `json:"services"`
	Dependencies  []graphDependency `json:"dependencies"`
	Topology      *graphTopology    `json:"topology,omitempty"`
}

type scannedManifest struct {
	file     string
	manifest contract.Manifest
}

func executeGraph(
	ctx context.Context,
	options graphOptions,
	stdout io.Writer,
	stderr io.Writer,
) int {
	project, err := existingProjectRoot(options.path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith graph: %v\n", err)
		return 1
	}
	scanned, err := scanProjectManifests(ctx, project)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith graph: %v\n", err)
		return 1
	}
	result, err := buildProjectGraph(scanned)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith graph: %v\n", err)
		return 1
	}
	if options.plan != "" {
		plan, err := loadGraphTopologyPlan(ctx, project, options.plan)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith graph: %v\n", err)
			return 1
		}
		result, err = attachGraphTopology(result, options.plan, plan)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith graph: %v\n", err)
			return 1
		}
	}
	switch options.format {
	case "json":
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(result); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith graph: encode result: %v\n", err)
			return 1
		}
	case "dot":
		if _, err := io.WriteString(stdout, renderGraphDOT(result)); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith graph: write result: %v\n", err)
			return 1
		}
	default:
		if _, err := io.WriteString(stdout, renderGraphText(result)); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith graph: write result: %v\n", err)
			return 1
		}
	}
	return 0
}

func scanProjectManifests(
	ctx context.Context,
	project string,
) ([]scannedManifest, error) {
	result := make([]scannedManifest, 0)
	err := filepath.WalkDir(project, func(
		path string,
		entry fs.DirEntry,
		walkErr error,
	) error {
		if walkErr != nil {
			return walkErr
		}
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if path != project && entry.IsDir() && ignoredGraphDirectory(entry.Name()) {
			return filepath.SkipDir
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if strings.HasSuffix(entry.Name(), contractManifestSuffix) {
				return fmt.Errorf(
					"contract manifest %q is a symlink",
					entry.Name(),
				)
			}
			return nil
		}
		if entry.IsDir() ||
			!strings.HasSuffix(entry.Name(), contractManifestSuffix) {
			return nil
		}
		if len(result) >= maxProjectManifests {
			return fmt.Errorf(
				"contract manifest count exceeds %d",
				maxProjectManifests,
			)
		}
		relative, err := filepath.Rel(project, path)
		if err != nil {
			return fmt.Errorf("resolve contract manifest path: %w", err)
		}
		relative = filepath.ToSlash(relative)
		safePath, err := safeProjectFile(project, relative, false)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect contract manifest %s: %w", relative, err)
		}
		if info.Size() <= 0 || info.Size() > maxProjectManifestSize {
			return fmt.Errorf(
				"contract manifest %s is empty or exceeds %d bytes",
				relative,
				maxProjectManifestSize,
			)
		}
		payload, err := os.ReadFile(safePath)
		if err != nil {
			return fmt.Errorf("read contract manifest %s: %w", relative, err)
		}
		manifest, err := contract.Parse(payload)
		if err != nil {
			return fmt.Errorf("parse contract manifest %s: %w", relative, err)
		}
		result = append(result, scannedManifest{
			file:     relative,
			manifest: manifest,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errors.New(
			"no generated contract manifests found; run `keelith generate` first",
		)
	}
	sort.Slice(result, func(first, second int) bool {
		return result[first].file < result[second].file
	})
	return result, nil
}

func ignoredGraphDirectory(name string) bool {
	switch name {
	case ".git", "node_modules", "vendor":
		return true
	default:
		return false
	}
}

func buildProjectGraph(scanned []scannedManifest) (graphResult, error) {
	manifests := make([]contract.Manifest, 0, len(scanned))
	filesBySource := make(map[string]string, len(scanned))
	for _, entry := range scanned {
		manifests = append(manifests, entry.manifest)
		if previous, exists := filesBySource[entry.manifest.Source]; exists {
			return graphResult{}, fmt.Errorf(
				"source %q is emitted by %s and %s",
				entry.manifest.Source,
				previous,
				entry.file,
			)
		}
		filesBySource[entry.manifest.Source] = entry.file
	}
	catalog, err := contract.NewCatalog(manifests...)
	if err != nil {
		return graphResult{}, err
	}
	description := catalog.Describe()
	owners := make(map[string]string, len(description.Services))
	for _, manifest := range catalog.Manifests() {
		for _, service := range manifest.Services {
			owners[service.Name] = manifest.Source
		}
	}
	result := graphResult{
		SchemaVersion: "v2",
		Manifests:     make([]graphManifest, 0, len(scanned)),
		Services:      make([]graphService, 0, len(description.Services)),
		Dependencies:  make([]graphDependency, 0, len(description.Dependencies)),
	}
	for _, manifest := range catalog.Manifests() {
		result.Manifests = append(result.Manifests, graphManifest{
			File:    filesBySource[manifest.Source],
			Source:  manifest.Source,
			Package: manifest.Package,
		})
	}
	for _, service := range description.Services {
		result.Services = append(result.Services, graphService{
			Name:       service.Name,
			Source:     owners[service.Name],
			Operations: append([]string(nil), service.Operations...),
			Transports: append([]string(nil), service.Transports...),
			HTTPRoutes: service.HTTPRoutes,
		})
	}
	for _, manifest := range catalog.Manifests() {
		for _, dependency := range manifest.Dependencies {
			owner, resolved := owners[dependency.Service]
			operations := append([]string(nil), dependency.Operations...)
			sort.Strings(operations)
			result.Dependencies = append(
				result.Dependencies,
				graphDependency{
					Source:     manifest.Source,
					Kind:       dependency.Kind,
					Service:    dependency.Service,
					Transport:  dependency.Transport,
					Reason:     dependency.Reason,
					Operations: operations,
					Resolved:   resolved,
					Owner:      owner,
				},
			)
		}
	}
	sort.Slice(result.Dependencies, func(first, second int) bool {
		left := result.Dependencies[first]
		right := result.Dependencies[second]
		switch {
		case left.Source != right.Source:
			return left.Source < right.Source
		case left.Service != right.Service:
			return left.Service < right.Service
		case left.Transport != right.Transport:
			return left.Transport < right.Transport
		default:
			return left.Reason < right.Reason
		}
	})
	return result, nil
}

func renderGraphText(graph graphResult) string {
	var output strings.Builder
	_, _ = fmt.Fprintf(
		&output,
		"contract graph: %d manifests, %d services, %d dependencies\n",
		len(graph.Manifests),
		len(graph.Services),
		len(graph.Dependencies),
	)
	output.WriteString("services:\n")
	for _, service := range graph.Services {
		_, _ = fmt.Fprintf(
			&output,
			"- %s (%s; transports=%s; operations=%d; httpRoutes=%d)\n",
			service.Name,
			service.Source,
			strings.Join(service.Transports, ","),
			len(service.Operations),
			service.HTTPRoutes,
		)
	}
	output.WriteString("dependencies:\n")
	for _, dependency := range graph.Dependencies {
		status := "external"
		if dependency.Resolved {
			status = "resolved:" + dependency.Owner
		}
		_, _ = fmt.Fprintf(
			&output,
			"- %s -> %s (%s; %s; %s; %s; operations=%d)\n",
			dependency.Source,
			dependency.Service,
			dependency.Kind,
			dependency.Transport,
			dependency.Reason,
			status,
			len(dependency.Operations),
		)
	}
	if graph.Topology != nil {
		_, _ = fmt.Fprintf(
			&output,
			"topology: %s apiVersion=%s epoch=%d hash=%s\n",
			graph.Topology.Plan,
			graph.Topology.APIVersion,
			graph.Topology.Epoch,
			graph.Topology.Hash,
		)
		output.WriteString("placements:\n")
		for _, component := range graph.Topology.Components {
			_, _ = fmt.Fprintf(
				&output,
				"- %s placement=%s\n",
				component.ID,
				component.Placement,
			)
		}
		output.WriteString("bindings:\n")
		for _, binding := range graph.Topology.Bindings {
			_, _ = fmt.Fprintf(
				&output,
				"- %s -> %s (constraint=%s; mode=%s; sourcePlacement=%s; targetPlacement=%s)\n",
				binding.Source,
				binding.Target,
				binding.Constraint,
				binding.Mode,
				binding.SourcePlacement,
				binding.TargetPlacement,
			)
		}
	}
	return output.String()
}

func renderGraphDOT(graph graphResult) string {
	var output strings.Builder
	output.WriteString("digraph keelith_contracts {\n")
	output.WriteString("  rankdir=LR;\n")
	if graph.Topology != nil {
		_, _ = fmt.Fprintf(
			&output,
			"  label=%s;\n",
			dotQuote(
				fmt.Sprintf(
					"topology epoch=%d hash=%s",
					graph.Topology.Epoch,
					graph.Topology.Hash,
				),
			),
		)
		output.WriteString("  labelloc=\"t\";\n")
	}
	for _, manifest := range graph.Manifests {
		_, _ = fmt.Fprintf(
			&output,
			"  %s [shape=box,label=%s];\n",
			dotQuote("source:"+manifest.Source),
			dotQuote(manifest.Source),
		)
	}
	for _, service := range graph.Services {
		_, _ = fmt.Fprintf(
			&output,
			"  %s [shape=ellipse,label=%s];\n",
			dotQuote("service:"+service.Name),
			dotQuote(service.Name),
		)
		_, _ = fmt.Fprintf(
			&output,
			"  %s -> %s [label=\"provides\"];\n",
			dotQuote("source:"+service.Source),
			dotQuote("service:"+service.Name),
		)
	}
	external := make(map[string]struct{})
	for _, dependency := range graph.Dependencies {
		if !dependency.Resolved {
			external[dependency.Service] = struct{}{}
		}
	}
	externalNames := make([]string, 0, len(external))
	for service := range external {
		externalNames = append(externalNames, service)
	}
	sort.Strings(externalNames)
	for _, service := range externalNames {
		_, _ = fmt.Fprintf(
			&output,
			"  %s [shape=ellipse,style=dashed,label=%s];\n",
			dotQuote("service:"+service),
			dotQuote(service),
		)
	}
	for _, dependency := range graph.Dependencies {
		_, _ = fmt.Fprintf(
			&output,
			"  %s -> %s [style=dashed,label=%s];\n",
			dotQuote("source:"+dependency.Source),
			dotQuote("service:"+dependency.Service),
			dotQuote(
				dependency.Kind+" / "+
					dependency.Transport+" / "+
					dependency.Reason,
			),
		)
	}
	if graph.Topology != nil {
		for _, component := range graph.Topology.Components {
			_, _ = fmt.Fprintf(
				&output,
				"  %s [shape=box,style=rounded,label=%s];\n",
				dotQuote("topology:"+component.ID),
				dotQuote(
					component.ID+"\nplacement="+component.Placement,
				),
			)
		}
		for _, binding := range graph.Topology.Bindings {
			_, _ = fmt.Fprintf(
				&output,
				"  %s -> %s [color=blue,label=%s];\n",
				dotQuote("topology:"+binding.Source),
				dotQuote("topology:"+binding.Target),
				dotQuote(binding.Constraint+" -> "+binding.Mode),
			)
		}
	}
	output.WriteString("}\n")
	return output.String()
}

func dotQuote(value string) string {
	return strconv.Quote(value)
}
