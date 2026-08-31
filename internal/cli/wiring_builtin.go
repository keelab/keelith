package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/keelab/keelith/di"
	"github.com/keelab/keelith/internal/scaffold"
	"github.com/keelab/keelith/internal/wiringcompiler"
	"github.com/spf13/cobra"
)

const builtinWiringProjectFile = "keelith.project.json"

type builtinWiringOptions struct {
	path string
}

func newWiringSyncCommand(runtime *commandRuntime) *cobra.Command {
	options := builtinWiringOptions{path: "."}
	return newBuiltinWiringCommand(runtime, options, false)
}

func newWiringCheckCommand(runtime *commandRuntime) *cobra.Command {
	options := builtinWiringOptions{path: "."}
	return newBuiltinWiringCommand(runtime, options, true)
}

func newBuiltinWiringCommand(
	runtime *commandRuntime,
	options builtinWiringOptions,
	check bool,
) *cobra.Command {
	use, short := "sync", "Generate project wiring artifacts"
	if check {
		use, short = "check", "Check project wiring artifacts for drift"
	}
	command := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if strings.TrimSpace(options.path) == "" {
				return errors.New("--path must not be empty")
			}
			return runCommand(command, runtime, func(ctx context.Context) int {
				project, err := existingProjectRoot(options.path)
				if err != nil {
					_, _ = fmt.Fprintf(runtime.stderr, "keelith wiring: %v\n", err)
					return 1
				}
				return executeBuiltinWiringProject(ctx, project, check, runtime.stdout, runtime.stderr)
			})
		},
	}
	command.Flags().StringVar(&options.path, "path", options.path, "project directory")
	return command
}

func executeBuiltinWiringProject(
	ctx context.Context,
	project string,
	check bool,
	stdout io.Writer,
	stderr io.Writer,
) int {
	if cause := context.Cause(ctx); cause != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: %v\n", cause)
		return 1
	}
	declarationPath, err := safeProjectFile(project, builtinWiringProjectFile, false)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: %v\n", err)
		return 1
	}
	declaration, err := os.ReadFile(declarationPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: read project declaration: %v\n", err)
		return 1
	}
	spec, err := scaffold.ParseWiringSpec(declaration)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: %v\n", err)
		return 1
	}
	if !check {
		if _, err := scaffold.SyncServices(ctx, project); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith wiring: sync services: %v\n", err)
			return 1
		}
		if _, err := scaffold.SyncComponents(ctx, project, spec.Module); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith wiring: sync components: %v\n", err)
			return 1
		}
	}
	components, err := scaffold.LoadWiringComponents(ctx, project)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: load components: %v\n", err)
		return 1
	}
	spec.Components = components
	services, err := scaffold.LoadWiringServices(ctx, project)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: load services: %v\n", err)
		return 1
	}
	spec.Services = services
	providers, err := wiringcompiler.LoadProviders(ctx, project, spec.Module, spec.Providers)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: resolve providers: %v\n", err)
		return 1
	}
	hasProfileProvider := false
	for _, provider := range providers {
		if provider.OutputType == "*service.Profile" {
			hasProfileProvider = true
			break
		}
	}
	if !hasProfileProvider {
		providers = append(providers, wiringcompiler.Provider{
			Spec:             scaffold.ProviderSpec{Constructor: spec.Module + "/internal/service.NewProfile"},
			ImportPath:       spec.Module + "/internal/service",
			PackageName:      "service",
			Name:             "NewProfile",
			InputTypes:       nil,
			InputExpressions: []string{strconv.Quote(spec.Name)},
			OutputType:       "*service.Profile",
			OutputKey:        "*service.Profile",
			ReturnsError:     true,
		})
	}
	graphServices := make([]wiringcompiler.Service, 0, len(spec.Services))
	for _, service := range spec.Services {
		graphServices = append(graphServices, wiringcompiler.Service{
			Name: service.Name, Operations: append([]string(nil), service.Operations...),
			Transports: append([]string(nil), service.Transports...), HTTPRoutes: service.HTTPRoutes,
		})
	}
	graphRoots := make([]wiringcompiler.Root, 0, len(spec.Roots))
	for _, root := range spec.Roots {
		graphRoots = append(graphRoots, wiringcompiler.Root{Name: root.Name, Kind: root.Kind, Provider: root.Provider})
	}
	graph, err := wiringcompiler.BuildGraphWithServicesAndRoots(spec.Module, "*service.Profile", providers, graphServices, graphRoots)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: build graph: %v\n", err)
		return 1
	}
	providers = graph.Providers
	resolved := make([]scaffold.ResolvedProvider, 0, len(providers))
	for _, provider := range providers {
		resolved = append(resolved, scaffold.ResolvedProvider{
			Constructor:      provider.Spec.Constructor,
			ImportPath:       provider.ImportPath,
			PackageName:      provider.PackageName,
			Name:             provider.Name,
			BindingName:      provider.Spec.Name,
			Group:            provider.Spec.Group,
			Scope:            provider.Spec.Scope,
			InputTypes:       append([]string(nil), provider.InputTypes...),
			InputExpressions: append([]string(nil), provider.InputExpressions...),
			OutputType:       provider.OutputType,
			OutputKey:        provider.OutputKey,
			ReturnsError:     provider.ReturnsError,
			ReturnsCleanup:   provider.ReturnsCleanup,
			Outputs: func() []scaffold.ResolvedOutput {
				outputs := make([]scaffold.ResolvedOutput, 0, len(provider.Outputs))
				for _, output := range provider.Outputs {
					outputs = append(outputs, scaffold.ResolvedOutput{Field: output.Field, Type: output.Type, Name: output.Name, Group: output.Group, Key: output.Key})
				}
				return outputs
			}(),
		})
	}
	artifacts, err := scaffold.BuiltinWiringArtifactsWithProviders(spec, resolved)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: %v\n", err)
		return 1
	}
	manifestPath, err := safeProjectFile(project, artifacts.ManifestPath, true)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: manifest path: %v\n", err)
		return 1
	}
	applicationPath, err := safeProjectFile(project, artifacts.ApplicationPath, true)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: application path: %v\n", err)
		return 1
	}
	changed := false
	var previousManifest []byte
	manifestExisted := false
	for _, item := range []struct {
		path string
		data []byte
		name string
	}{
		{path: manifestPath, data: artifacts.Manifest, name: artifacts.ManifestPath},
		{path: applicationPath, data: artifacts.Application, name: artifacts.ApplicationPath},
	} {
		current, readErr := os.ReadFile(item.path)
		if errors.Is(readErr, os.ErrNotExist) {
			_, _ = fmt.Fprintf(stderr, "missing: %s\n", item.name)
			changed = true
			continue
		}
		if readErr != nil {
			_, _ = fmt.Fprintf(stderr, "keelith wiring: read %s: %v\n", item.name, readErr)
			return 1
		}
		if item.path == manifestPath {
			previousManifest = append([]byte(nil), current...)
			manifestExisted = true
		}
		if string(current) != string(item.data) {
			if item.path == applicationPath && !bytes.HasPrefix(current, []byte("// Code generated by Keelith. DO NOT EDIT.\n")) {
				_, _ = fmt.Fprintf(stderr, "keelith wiring: refusing to overwrite unmarked file %s\n", item.name)
				return 1
			}
			if item.path == manifestPath {
				parsed, parseErr := di.ParseManifest(current)
				if parseErr != nil || parsed.Description.Generator != "keelith wiring" {
					_, _ = fmt.Fprintf(stderr, "keelith wiring: refusing to overwrite unmarked file %s\n", item.name)
					return 1
				}
			}
			_, _ = fmt.Fprintf(stderr, "outdated: %s\n", item.name)
			changed = true
		}
	}
	generatedDir := filepath.Join(project, "internal", "keelithgen")
	entries, readDirErr := os.ReadDir(generatedDir)
	if readDirErr == nil {
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".keelith.gen.go") || entry.Name() == "application.keelith.gen.go" {
				continue
			}
			_, _ = fmt.Fprintf(stderr, "unexpected: internal/keelithgen/%s\n", entry.Name())
			changed = true
		}
	} else if !errors.Is(readDirErr, os.ErrNotExist) {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: inspect generated directory: %v\n", readDirErr)
		return 1
	}
	if check {
		if changed {
			return 1
		}
		_, _ = fmt.Fprintln(stdout, "dependency wiring is current")
		return 0
	}
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: create manifest directory: %v\n", err)
		return 1
	}
	if err := atomicWriteFile(manifestPath, artifacts.Manifest, 0o600); err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: write %s: %v\n", artifacts.ManifestPath, err)
		return 1
	}
	if err := os.MkdirAll(filepath.Dir(applicationPath), 0o755); err != nil {
		_ = restoreWiringManifest(manifestPath, previousManifest, manifestExisted)
		_, _ = fmt.Fprintf(stderr, "keelith wiring: create application directory: %v\n", err)
		return 1
	}
	if err := atomicWriteFile(applicationPath, artifacts.Application, 0o600); err != nil {
		_ = restoreWiringManifest(manifestPath, previousManifest, manifestExisted)
		_, _ = fmt.Fprintf(stderr, "keelith wiring: write %s: %v\n", artifacts.ApplicationPath, err)
		return 1
	}
	_, _ = fmt.Fprintf(
		stdout,
		"dependency wiring synchronized: manifest=%s application=%s\n",
		artifacts.ManifestPath,
		artifacts.ApplicationPath,
	)
	return 0
}

func restoreWiringManifest(path string, previous []byte, existed bool) error {
	if existed {
		return atomicWriteFile(path, previous, 0o600)
	}
	return os.Remove(path)
}
