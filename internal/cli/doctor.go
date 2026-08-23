package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/keelab/keelith/internal/projectinfo"
)

type doctorOptions struct {
	path    string
	format  string
	inspect bool
}

type doctorCheck struct {
	Name     string `json:"name"`
	Required bool   `json:"required"`
	OK       bool   `json:"ok"`
	Path     string `json:"path,omitempty"`
	Version  string `json:"version,omitempty"`
	Message  string `json:"message,omitempty"`
}

type doctorResult struct {
	OK     bool          `json:"ok"`
	Checks []doctorCheck `json:"checks"`
}

func executeDoctor(
	ctx context.Context,
	options doctorOptions,
	stdout io.Writer,
	stderr io.Writer,
) int {
	projectChecks := []doctorCheck(nil)
	requireBuf := false
	if options.inspect {
		projectChecks, requireBuf = inspectProject(options.path)
	}
	result := inspectToolchain(ctx, requireBuf)
	result.Checks = append(result.Checks, projectChecks...)
	result.OK = requiredChecksOK(result.Checks)
	if options.format == "json" {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith: encode doctor result: %v\n", err)
			return 1
		}
	} else {
		for _, check := range result.Checks {
			status := "ok"
			if !check.OK {
				status = "warn"
				if check.Required {
					status = "failed"
				}
			}
			detail := check.Version
			if detail == "" {
				detail = check.Message
			}
			if detail != "" {
				detail = " (" + detail + ")"
			}
			_, _ = fmt.Fprintf(
				stdout,
				"%-8s %-7s%s\n",
				check.Name,
				status,
				detail,
			)
		}
	}
	if !result.OK {
		return 1
	}
	return 0
}

func inspectToolchain(ctx context.Context, requireBuf bool) doctorResult {
	tools := []struct {
		name     string
		required bool
		version  []string
	}{
		{name: "go", required: true, version: []string{"version"}},
		{name: "git", version: []string{"--version"}},
		{name: "buf", required: requireBuf, version: []string{"--version"}},
		{name: "docker", version: []string{"--version"}},
	}
	result := doctorResult{OK: true, Checks: make([]doctorCheck, 0, len(tools))}
	for _, tool := range tools {
		check := doctorCheck{Name: tool.name, Required: tool.required}
		path, err := exec.LookPath(tool.name)
		if err != nil {
			check.Message = "not found in PATH"
			if tool.required {
				result.OK = false
			}
			result.Checks = append(result.Checks, check)
			continue
		}
		check.OK = true
		check.Path = path
		output, err := exec.CommandContext(ctx, path, tool.version...).Output()
		if err != nil {
			check.Message = "version unavailable"
			if tool.required {
				check.OK = false
				result.OK = false
			}
		} else {
			check.Version = strings.TrimSpace(string(output))
		}
		result.Checks = append(result.Checks, check)
	}
	return result
}

func inspectProject(path string) ([]doctorCheck, bool) {
	checks := make([]doctorCheck, 0, 3)
	project, err := existingProjectRoot(path)
	if err != nil {
		return []doctorCheck{{
			Name:     "project",
			Required: true,
			Message:  "project path is unavailable",
		}}, false
	}
	info, err := os.Stat(project)
	if err != nil || !info.IsDir() {
		return []doctorCheck{{
			Name:     "project",
			Required: true,
			Path:     project,
			Message:  "not a directory",
		}}, false
	}
	checks = append(checks, doctorCheck{
		Name:     "project",
		Required: true,
		OK:       true,
		Path:     project,
	})

	module := doctorCheck{
		Name:     "module",
		Required: true,
		Path:     filepath.Join(project, "go.mod"),
	}
	identity, identityErr := projectinfo.Load(project)
	if identityErr == nil {
		module.OK = true
		module.Version = identity.Module
	} else {
		module.Message = identityErr.Error()
	}
	checks = append(checks, module)
	if identityErr != nil {
		return checks, false
	}
	if identity.BufConfig != "" {
		checks = append(checks, doctorCheck{
			Name:    "buf-config",
			OK:      true,
			Path:    identity.BufConfig,
			Message: "protobuf generation enabled",
		})
	}
	return checks, identity.BufConfig != ""
}

func requiredChecksOK(checks []doctorCheck) bool {
	for _, check := range checks {
		if check.Required && !check.OK {
			return false
		}
	}
	return true
}
