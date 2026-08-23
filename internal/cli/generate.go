package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/keelab/keelith/internal/projectinfo"
	"github.com/keelab/keelith/internal/scaffold"
)

type generateOptions struct {
	path   string
	format string
}

type generateResult struct {
	Path         string `json:"path"`
	Status       string `json:"status"`
	Services     int    `json:"services"`
	Dependencies int    `json:"dependencies"`
	Components   int    `json:"components"`
}

func executeGenerate(
	ctx context.Context,
	options generateOptions,
	stdout io.Writer,
	stderr io.Writer,
) int {
	path, err := existingProjectRoot(options.path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith generate: %v\n", err)
		return 1
	}
	goPath, err := exec.LookPath("go")
	if err != nil {
		_, _ = fmt.Fprintln(
			stderr,
			"keelith generate: go was not found in PATH; run `keelith doctor`",
		)
		return 1
	}
	module, requiresBuf, err := generationProjectIdentity(path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith generate: %v\n", err)
		return 1
	}
	if detail, err := runGenerationCommand(
		ctx,
		path,
		goPath,
		"mod",
		"download",
		"all",
	); err != nil {
		_, _ = fmt.Fprintf(
			stderr,
			"keelith generate: prepare Go tools: %s\n",
			detail,
		)
		return 1
	}
	if requiresBuf {
		bufPath, lookupErr := exec.LookPath("buf")
		if lookupErr != nil {
			_, _ = fmt.Fprintln(
				stderr,
				"keelith generate: buf was not found in PATH; run `keelith doctor`",
			)
			return 1
		}
		detail, generationErr := runGenerationCommand(
			ctx,
			path,
			bufPath,
			"generate",
		)
		if generationErr != nil {
			_, _ = fmt.Fprintf(stderr, "keelith generate: %s\n", detail)
			return 1
		}
	}
	synchronized, err := scaffold.SyncServices(ctx, path)
	if err != nil {
		_, _ = fmt.Fprintf(
			stderr,
			"keelith generate: synchronize services: %v\n",
			err,
		)
		return 1
	}
	componentSync, err := scaffold.SyncComponents(ctx, path, module)
	if err != nil {
		_, _ = fmt.Fprintf(
			stderr,
			"keelith generate: synchronize components: %v\n",
			err,
		)
		return 1
	}
	result := generateResult{
		Path:         path,
		Status:       "generated",
		Services:     synchronized.Services,
		Dependencies: synchronized.Dependencies,
		Components:   componentSync.Components,
	}
	if options.format == "json" {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith generate: encode result: %v\n", err)
			return 1
		}
		return 0
	}
	_, _ = fmt.Fprintf(
		stdout,
		"generated adapters and synchronized %d services, %d dependencies, %d components in %s\n",
		result.Services,
		result.Dependencies,
		result.Components,
		path,
	)
	return 0
}

func generationProjectIdentity(project string) (string, bool, error) {
	identity, err := projectinfo.Load(project)
	if err != nil {
		return "", false, fmt.Errorf("read Go project: %w", err)
	}
	return identity.Module, identity.BufConfig != "", nil
}

func runGenerationCommand(
	ctx context.Context,
	directory string,
	executable string,
	arguments ...string,
) (string, error) {
	var commandOutput bytes.Buffer
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Dir = directory
	command.Stdout = &commandOutput
	command.Stderr = &commandOutput
	if err := command.Run(); err != nil {
		detail := strings.TrimSpace(commandOutput.String())
		if detail == "" {
			detail = err.Error()
		}
		return detail, err
	}
	return strings.TrimSpace(commandOutput.String()), nil
}
