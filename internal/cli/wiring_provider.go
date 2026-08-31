package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/keelab/keelith/internal/scaffold"
	"github.com/keelab/keelith/internal/wiringcompiler"
	"github.com/spf13/cobra"
)

func newWiringProviderCommands(runtime *commandRuntime) []*cobra.Command {
	addRoot := &cobra.Command{
		Use: "add-root NAME", Short: "Register an application entrypoint", Args: cobra.ExactArgs(1),
	}
	var rootKind, rootProvider, rootPath string
	addRoot.Flags().StringVar(&rootKind, "kind", "", "entrypoint kind, for example http or worker")
	addRoot.Flags().StringVar(&rootProvider, "provider", "", "optional root constructor symbol")
	addRoot.Flags().StringVar(&rootPath, "path", ".", "project directory")
	addRoot.RunE = func(command *cobra.Command, args []string) error {
		return runCommand(command, runtime, func(ctx context.Context) int {
			return executeAddWiringRoot(ctx, rootPath, scaffold.RootSpec{Name: args[0], Kind: rootKind, Provider: rootProvider}, runtime.stdout, runtime.stderr)
		})
	}
	removeRoot := &cobra.Command{Use: "remove-root NAME", Short: "Remove an application entrypoint", Args: cobra.ExactArgs(1)}
	removeRoot.Flags().StringVar(&rootPath, "path", ".", "project directory")
	removeRoot.RunE = func(command *cobra.Command, args []string) error {
		return runCommand(command, runtime, func(ctx context.Context) int {
			return executeRemoveWiringRoot(ctx, rootPath, args[0], runtime.stdout, runtime.stderr)
		})
	}
	add := &cobra.Command{
		Use:   "add-provider CONSTRUCTOR",
		Short: "Register a business constructor for generated wiring",
		Args:  cobra.ExactArgs(1),
	}
	var name, as, group, scope, path string
	var inputs []string
	add.RunE = func(command *cobra.Command, args []string) error {
		if strings.TrimSpace(path) == "" {
			return errors.New("--path must not be empty")
		}
		return runCommand(command, runtime, func(ctx context.Context) int {
			return executeAddWiringProvider(ctx, path, scaffold.ProviderSpec{
				Constructor: args[0], Name: name, As: as, Group: group, Scope: scope, Inputs: inputs,
			}, runtime.stdout, runtime.stderr)
		})
	}
	add.Flags().StringVar(&path, "path", ".", "project directory")
	add.Flags().StringVar(&name, "name", "", "binding name")
	add.Flags().StringVar(&as, "as", "", "interface or exported type")
	add.Flags().StringVar(&group, "group", "", "binding group")
	add.Flags().StringVar(&scope, "scope", "", "binding scope: application or transient")
	add.Flags().StringSliceVar(&inputs, "input", nil, "dependency binding keys in parameter order")

	list := &cobra.Command{
		Use:   "list",
		Short: "List registered generated wiring providers",
		Args:  cobra.NoArgs,
	}
	var listPath string
	list.RunE = func(command *cobra.Command, _ []string) error {
		return runCommand(command, runtime, func(ctx context.Context) int {
			return executeListWiringProviders(ctx, listPath, runtime.stdout, runtime.stderr)
		})
	}
	list.Flags().StringVar(&listPath, "path", ".", "project directory")
	remove := &cobra.Command{Use: "remove-provider CONSTRUCTOR", Short: "Remove a registered wiring provider", Args: cobra.ExactArgs(1)}
	var removePath string
	remove.Flags().StringVar(&removePath, "path", ".", "project directory")
	remove.RunE = func(command *cobra.Command, args []string) error {
		return runCommand(command, runtime, func(ctx context.Context) int {
			return executeRemoveWiringProvider(ctx, removePath, args[0], runtime.stdout, runtime.stderr)
		})
	}
	return []*cobra.Command{add, remove, list, addRoot, removeRoot}
}

func executeAddWiringRoot(ctx context.Context, path string, root scaffold.RootSpec, stdout, stderr io.Writer) int {
	project, err := existingProjectRoot(path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-root: %v\n", err)
		return 1
	}
	if cause := context.Cause(ctx); cause != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-root: %v\n", cause)
		return 1
	}
	spec, err := loadBuiltinWiringSpec(project)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-root: %v\n", err)
		return 1
	}
	if root.Provider != "" {
		registered := false
		for _, provider := range spec.Providers {
			if provider.Constructor == root.Provider {
				registered = true
				break
			}
		}
		if !registered {
			spec.Providers = append(spec.Providers, scaffold.ProviderSpec{Constructor: root.Provider})
		}
	}
	spec.Roots = append(spec.Roots, root)
	document, err := scaffold.MarshalWiringSpec(spec)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-root: %v\n", err)
		return 1
	}
	if err := atomicWriteFile(filepath.Join(project, builtinWiringProjectFile), append(document, '\n'), 0o600); err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-root: write declaration: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "registered wiring root: %s\n", root.Name)
	return 0
}

func executeRemoveWiringRoot(ctx context.Context, path, name string, stdout, stderr io.Writer) int {
	project, err := existingProjectRoot(path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-root: %v\n", err)
		return 1
	}
	if cause := context.Cause(ctx); cause != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-root: %v\n", cause)
		return 1
	}
	spec, err := loadBuiltinWiringSpec(project)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-root: %v\n", err)
		return 1
	}
	roots := spec.Roots[:0]
	found := false
	for _, root := range spec.Roots {
		if root.Name == name {
			found = true
			continue
		}
		roots = append(roots, root)
	}
	if !found {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-root: root %q is not registered\n", name)
		return 1
	}
	spec.Roots = roots
	document, err := scaffold.MarshalWiringSpec(spec)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-root: %v\n", err)
		return 1
	}
	if err := atomicWriteFile(filepath.Join(project, builtinWiringProjectFile), append(document, '\n'), 0o600); err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-root: write declaration: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "removed wiring root: %s\n", name)
	return 0
}

func executeRemoveWiringProvider(ctx context.Context, path, constructor string, stdout, stderr io.Writer) int {
	project, err := existingProjectRoot(path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-provider: %v\n", err)
		return 1
	}
	if cause := context.Cause(ctx); cause != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-provider: %v\n", cause)
		return 1
	}
	spec, err := loadBuiltinWiringSpec(project)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-provider: %v\n", err)
		return 1
	}
	providers := spec.Providers[:0]
	found := false
	for _, provider := range spec.Providers {
		if provider.Constructor == constructor {
			found = true
			continue
		}
		providers = append(providers, provider)
	}
	if !found {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-provider: provider %q is not registered\n", constructor)
		return 1
	}
	spec.Providers = providers
	document, err := scaffold.MarshalWiringSpec(spec)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-provider: %v\n", err)
		return 1
	}
	if err := atomicWriteFile(filepath.Join(project, builtinWiringProjectFile), append(document, '\n'), 0o600); err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring remove-provider: write declaration: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "removed wiring provider: %s\n", constructor)
	return 0
}

func executeAddWiringProvider(
	ctx context.Context,
	path string,
	provider scaffold.ProviderSpec,
	stdout io.Writer,
	stderr io.Writer,
) int {
	project, err := existingProjectRoot(path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-provider: %v\n", err)
		return 1
	}
	if cause := context.Cause(ctx); cause != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-provider: %v\n", cause)
		return 1
	}
	spec, err := loadBuiltinWiringSpec(project)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-provider: %v\n", err)
		return 1
	}
	spec.Providers = append(spec.Providers, provider)
	if _, err := wiringcompiler.LoadProviders(ctx, project, spec.Module, []scaffold.ProviderSpec{provider}); err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-provider: validate provider: %v\n", err)
		return 1
	}
	document, err := scaffold.MarshalWiringSpec(spec)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-provider: %v\n", err)
		return 1
	}
	target := filepath.Join(project, builtinWiringProjectFile)
	if err := atomicWriteFile(target, append(document, '\n'), 0o600); err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring add-provider: write declaration: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "registered wiring provider: %s\n", provider.Constructor)
	return 0
}

func executeListWiringProviders(ctx context.Context, path string, stdout, stderr io.Writer) int {
	project, err := existingProjectRoot(path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring list: %v\n", err)
		return 1
	}
	spec, err := loadBuiltinWiringSpec(project)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring list: %v\n", err)
		return 1
	}
	if cause := context.Cause(ctx); cause != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring list: %v\n", cause)
		return 1
	}
	for _, provider := range spec.Providers {
		_, _ = fmt.Fprintln(stdout, provider.Constructor)
	}
	return 0
}

func loadBuiltinWiringSpec(project string) (scaffold.WiringSpec, error) {
	path := filepath.Join(project, builtinWiringProjectFile)
	document, err := os.ReadFile(path)
	if err != nil {
		return scaffold.WiringSpec{}, fmt.Errorf("read %s: %w", builtinWiringProjectFile, err)
	}
	return scaffold.ParseWiringSpec(document)
}
