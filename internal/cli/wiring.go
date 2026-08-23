package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/keelab/keelith/di"
	"github.com/spf13/cobra"
)

type wiringOptions struct {
	path, manifest, format string
}

func newWiringCommand(runtime *commandRuntime) *cobra.Command {
	command := commandGroup("wiring", "Synchronize, verify, and inspect dependency wiring artifacts")
	command.AddCommand(newWiringSyncCommand(runtime), newWiringCheckCommand(runtime), newWiringVerifyCommand(runtime), newWiringGraphCommand(runtime))
	return command
}

func bindWiringInputFlags(command *cobra.Command, options *wiringOptions) {
	command.Flags().StringVar(&options.path, "path", ".", "project directory")
	command.Flags().StringVar(&options.manifest, "manifest", "dependency-graph.json", "project-relative DI manifest")
}

func newWiringVerifyCommand(runtime *commandRuntime) *cobra.Command {
	options := wiringOptions{}
	command := &cobra.Command{Use: "verify", Short: "Validate a portable dependency graph manifest", Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runCommand(command, runtime, func(ctx context.Context) int {
				return executeWiringVerify(ctx, options, runtime.stdout, runtime.stderr)
			})
		}}
	bindWiringInputFlags(command, &options)
	return command
}

func newWiringGraphCommand(runtime *commandRuntime) *cobra.Command {
	options := wiringOptions{format: "text"}
	command := &cobra.Command{Use: "graph", Short: "Render the application dependency graph", Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if options.format != "text" && options.format != "json" && options.format != "dot" {
				return fmt.Errorf("--format must be text, json, or dot")
			}
			return runCommand(command, runtime, func(ctx context.Context) int { return executeWiringGraph(ctx, options, runtime.stdout, runtime.stderr) })
		}}
	bindWiringInputFlags(command, &options)
	command.Flags().StringVar(&options.format, "format", options.format, "output format: text, json, or dot")
	return command
}

func loadWiringManifest(ctx context.Context, options wiringOptions) (string, di.Manifest, bool, error) {
	if cause := context.Cause(ctx); cause != nil {
		return "", di.Manifest{}, false, cause
	}
	project, err := existingProjectRoot(options.path)
	if err != nil {
		return "", di.Manifest{}, false, err
	}
	path, err := safeProjectFile(project, options.manifest, false)
	if err != nil {
		return "", di.Manifest{}, false, err
	}
	document, err := os.ReadFile(path)
	if err != nil {
		return "", di.Manifest{}, false, fmt.Errorf("read DI manifest: %w", err)
	}
	manifest, err := di.ParseManifest(document)
	if err != nil {
		return "", di.Manifest{}, false, err
	}
	canonical, err := di.MarshalManifest(manifest.Description)
	if err != nil {
		return "", di.Manifest{}, false, err
	}
	isCanonical := bytes.Equal(document, canonical) || bytes.Equal(document, append(canonical, '\n'))
	return project, manifest, isCanonical, nil
}

func executeWiringVerify(ctx context.Context, options wiringOptions, stdout, stderr io.Writer) int {
	_, manifest, canonical, err := loadWiringManifest(ctx, options)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring verify: %v\n", err)
		return 1
	}
	if !canonical {
		_, _ = fmt.Fprintln(stderr, "keelith wiring verify: dependency manifest is valid but not canonical; regenerate it with the project wiring command")
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "dependency wiring valid: root=%s providers=%d edges=%d\n", manifest.Description.Root, len(manifest.Description.Providers), len(manifest.Description.Edges))
	return 0
}

func executeWiringGraph(ctx context.Context, options wiringOptions, stdout, stderr io.Writer) int {
	_, manifest, _, err := loadWiringManifest(ctx, options)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring graph: %v\n", err)
		return 1
	}
	var output []byte
	switch options.format {
	case "json":
		output, err = di.MarshalManifest(manifest.Description)
	case "dot":
		output = []byte(di.RenderDOT(manifest.Description))
	default:
		output = []byte(di.RenderText(manifest.Description))
	}
	if err == nil {
		_, err = stdout.Write(output)
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring graph: %v\n", err)
		return 1
	}
	return 0
}
