package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/keelab/keelith/internal/scaffold"
	"github.com/spf13/cobra"
)

type newOptions struct {
	path             string
	module           string
	template         string
	frameworkVersion string
	format           string
}

type newResult struct {
	Project          string   `json:"project"`
	Module           string   `json:"module"`
	Template         string   `json:"template"`
	FrameworkVersion string   `json:"frameworkVersion"`
	Created          []string `json:"created"`
}

func newNewCommand(runtime *commandRuntime) *cobra.Command {
	options := newOptions{template: "minimal", format: "text"}
	command := &cobra.Command{
		Use:   "new NAME",
		Short: "Create a runnable Keelith project",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			options.template = strings.ToLower(strings.TrimSpace(options.template))
			if err := validateTextJSON(options.format); err != nil {
				return err
			}
			switch options.template {
			case "minimal", "service", "production":
			default:
				return errors.New("--template must be minimal, service, or production")
			}
			if strings.TrimSpace(options.path) == "" {
				options.path = args[0]
			}
			return runCommand(command, runtime, func(ctx context.Context) int {
				return executeNew(ctx, options, runtime.stdout, runtime.stderr)
			})
		},
	}
	flags := command.Flags()
	flags.StringVar(&options.path, "path", options.path, "target project directory")
	flags.StringVar(&options.module, "module", options.module, "Go module path")
	flags.StringVar(&options.frameworkVersion, "framework-version", options.frameworkVersion, "Keelith module version")
	flags.StringVar(
		&options.template,
		"template",
		options.template,
		"project template: minimal, service, or production",
	)
	flags.StringVar(&options.format, "format", options.format, "output format: text or json")
	return command
}

func executeNew(ctx context.Context, options newOptions, stdout, stderr io.Writer) int {
	result, err := scaffold.NewProject(ctx, scaffold.NewProjectOptions{
		Project:          options.path,
		Module:           options.module,
		Template:         options.template,
		FrameworkVersion: options.frameworkVersion,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith new: %v\n", err)
		return 1
	}
	output := newResult{
		Project:          result.Project,
		Module:           result.Module,
		Template:         result.Template,
		FrameworkVersion: result.FrameworkVersion,
		Created:          result.Created,
	}
	if options.format == "json" {
		if err := json.NewEncoder(stdout).Encode(output); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith new: encode result: %v\n", err)
			return 1
		}
		return 0
	}
	_, _ = fmt.Fprintf(
		stdout,
		"created %s project in %s\nnext: cd %s && go run .\n",
		output.Template,
		output.Project,
		filepath.ToSlash(output.Project),
	)
	return 0
}
