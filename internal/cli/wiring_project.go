package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/keelab/keelith/di"
	"github.com/spf13/cobra"
)

const (
	wiringContractSchema = "keelith.wiring/v1"
	wiringOutputEnv      = "KEELITH_WIRING_OUTPUT_DIR"
	maxWiringSourceBytes = 16 << 20
)

type wiringProjectOptions struct {
	path, contract string
	timeout        time.Duration
}

type wiringProjectContract struct {
	Schema   string                 `json:"schema"`
	Frontend string                 `json:"frontend"`
	Manifest string                 `json:"manifest"`
	Embedded wiringEmbeddedContract `json:"embedded"`
	Static   string                 `json:"static,omitempty"`
}

type wiringEmbeddedContract struct {
	Path     string `json:"path"`
	Package  string `json:"package"`
	Variable string `json:"variable"`
}

type wiringArtifacts struct {
	manifestPath, embeddedPath, staticPath string
	manifest, embedded, static             []byte
}

func newWiringSyncCommand(runtime *commandRuntime) *cobra.Command {
	return newWiringProjectCommand(runtime, false)
}

func newWiringCheckCommand(runtime *commandRuntime) *cobra.Command {
	return newWiringProjectCommand(runtime, true)
}

func newWiringProjectCommand(runtime *commandRuntime, check bool) *cobra.Command {
	options := wiringProjectOptions{path: ".", contract: "keelith.wiring.json", timeout: 2 * time.Minute}
	use, short := "sync", "Regenerate project dependency wiring artifacts"
	if check {
		use, short = "check", "Rebuild dependency wiring in isolation and report drift"
	}
	command := &cobra.Command{Use: use, Short: short, Args: cobra.NoArgs, RunE: func(command *cobra.Command, _ []string) error {
		return runCommand(command, runtime, func(ctx context.Context) int {
			return executeWiringProject(ctx, options, check, runtime.stdout, runtime.stderr)
		})
	}}
	command.Flags().StringVar(&options.path, "path", options.path, "project directory")
	command.Flags().StringVar(&options.contract, "contract", options.contract, "project-relative wiring contract")
	command.Flags().DurationVar(&options.timeout, "timeout", options.timeout, "frontend execution timeout")
	return command
}

func executeWiringProject(ctx context.Context, options wiringProjectOptions, check bool, stdout, stderr io.Writer) int {
	project, contract, err := loadWiringProjectContract(options)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: %v\n", err)
		return 1
	}
	if options.timeout <= 0 {
		_, _ = fmt.Fprintln(stderr, "keelith wiring: timeout must be positive")
		return 1
	}
	runCtx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()
	artifacts, err := buildWiringArtifacts(runCtx, project, contract)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: %v\n", err)
		return 1
	}
	changed, err := compareWiringArtifacts(artifacts, stderr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: %v\n", err)
		return 1
	}
	if check {
		if changed {
			return 1
		}
		_, _ = fmt.Fprintln(stdout, "dependency wiring is current")
		return 0
	}
	if err := publishWiringArtifacts(artifacts); err != nil {
		_, _ = fmt.Fprintf(stderr, "keelith wiring: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "dependency wiring synchronized: manifest=%s embedded=%s", contract.Manifest, contract.Embedded.Path)
	if contract.Static != "" {
		_, _ = fmt.Fprintf(stdout, " static=%s", contract.Static)
	}
	_, _ = fmt.Fprintln(stdout)
	return 0
}

func loadWiringProjectContract(options wiringProjectOptions) (string, wiringProjectContract, error) {
	project, err := existingProjectRoot(options.path)
	if err != nil {
		return "", wiringProjectContract{}, err
	}
	path, err := safeProjectFile(project, options.contract, false)
	if err != nil {
		return "", wiringProjectContract{}, err
	}
	document, err := os.ReadFile(path)
	if err != nil {
		return "", wiringProjectContract{}, fmt.Errorf("read contract: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	var contract wiringProjectContract
	if err := decoder.Decode(&contract); err != nil {
		return "", wiringProjectContract{}, fmt.Errorf("decode contract: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return "", wiringProjectContract{}, errors.New("contract contains multiple JSON values")
	}
	if contract.Schema != wiringContractSchema {
		return "", wiringProjectContract{}, fmt.Errorf("unsupported contract schema %q", contract.Schema)
	}
	if contract.Frontend == "" || contract.Manifest == "" || contract.Embedded.Path == "" || contract.Embedded.Package == "" || contract.Embedded.Variable == "" {
		return "", wiringProjectContract{}, errors.New("contract is incomplete")
	}
	if _, err := safeProjectDirectory(project, contract.Frontend); err != nil {
		return "", wiringProjectContract{}, fmt.Errorf("frontend: %w", err)
	}
	for name, relative := range map[string]string{"manifest": contract.Manifest, "embedded": contract.Embedded.Path} {
		if _, err := safeProjectFile(project, relative, true); err != nil {
			return "", wiringProjectContract{}, fmt.Errorf("%s: %w", name, err)
		}
	}
	if contract.Static != "" {
		if _, err := safeProjectFile(project, contract.Static, true); err != nil {
			return "", wiringProjectContract{}, fmt.Errorf("static: %w", err)
		}
	}
	targets := []string{contract.Manifest, contract.Embedded.Path}
	if contract.Static != "" {
		targets = append(targets, contract.Static)
	}
	seenTargets := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if _, duplicate := seenTargets[target]; duplicate {
			return "", wiringProjectContract{}, fmt.Errorf("wiring target %q is duplicated", target)
		}
		seenTargets[target] = struct{}{}
	}
	return project, contract, nil
}

func buildWiringArtifacts(ctx context.Context, project string, contract wiringProjectContract) (wiringArtifacts, error) {
	temporary, err := os.MkdirTemp("", "keelith-wiring-*")
	if err != nil {
		return wiringArtifacts{}, fmt.Errorf("create temporary output: %w", err)
	}
	defer func() { _ = os.RemoveAll(temporary) }()
	workspace := filepath.Join(temporary, "workspace")
	output := filepath.Join(temporary, "output")
	if err := os.MkdirAll(output, 0o700); err != nil {
		return wiringArtifacts{}, fmt.Errorf("create frontend output: %w", err)
	}
	if err := copyWiringWorkspace(ctx, project, workspace); err != nil {
		return wiringArtifacts{}, err
	}
	command := exec.CommandContext(ctx, "go", "run", "-mod=mod", "./"+filepath.ToSlash(contract.Frontend))
	command.Dir = workspace
	command.Env = wiringFrontendEnvironment(output)
	var logs bytes.Buffer
	command.Stdout, command.Stderr = &logs, &logs
	if err := command.Run(); err != nil {
		detail := strings.TrimSpace(logs.String())
		if detail == "" {
			detail = err.Error()
		}
		return wiringArtifacts{}, fmt.Errorf("run frontend: %s", detail)
	}
	manifestDocument, err := os.ReadFile(filepath.Join(output, "dependency-graph.json"))
	if err != nil {
		return wiringArtifacts{}, fmt.Errorf("frontend did not produce dependency-graph.json: %w", err)
	}
	manifest, err := di.ParseManifest(manifestDocument)
	if err != nil {
		return wiringArtifacts{}, fmt.Errorf("frontend manifest: %w", err)
	}
	canonical, err := di.MarshalManifest(manifest.Description)
	if err != nil {
		return wiringArtifacts{}, err
	}
	canonical = append(canonical, '\n')
	embedded, err := di.GenerateManifestGo(contract.Embedded.Package, contract.Embedded.Variable, manifest.Description)
	if err != nil {
		return wiringArtifacts{}, err
	}
	result := wiringArtifacts{manifest: canonical, embedded: embedded}
	result.manifestPath, _ = safeProjectFile(project, contract.Manifest, true)
	result.embeddedPath, _ = safeProjectFile(project, contract.Embedded.Path, true)
	if contract.Static != "" {
		result.static, err = os.ReadFile(filepath.Join(output, "static.keelith.gen.go"))
		if err != nil {
			return wiringArtifacts{}, fmt.Errorf("frontend did not produce static.keelith.gen.go: %w", err)
		}
		if !bytes.Contains(result.static, []byte("Code generated by Keelith")) {
			return wiringArtifacts{}, errors.New("frontend static wiring lacks Keelith generated marker")
		}
		result.staticPath, _ = safeProjectFile(project, contract.Static, true)
	}
	return result, nil
}

func copyWiringWorkspace(ctx context.Context, source, target string) error {
	if err := os.MkdirAll(target, 0o700); err != nil {
		return fmt.Errorf("create temporary workspace: %w", err)
	}
	excluded := map[string]struct{}{
		".git": {}, ".gitnexus": {}, ".hg": {}, ".svn": {}, ".idea": {}, ".vscode": {}, ".cache": {}, ".local": {},
		".secrets": {}, "node_modules": {}, "vendor": {}, "dist": {}, "build": {}, "bin": {},
		"target": {},
	}
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		if !entry.IsDir() && (entry.Name() == ".env" || strings.HasPrefix(entry.Name(), ".env.")) {
			return nil
		}
		if entry.IsDir() {
			if _, skip := excluded[entry.Name()]; skip {
				return filepath.SkipDir
			}
			return os.MkdirAll(filepath.Join(target, relative), 0o700)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("wiring source %q is a symlink", filepath.ToSlash(relative))
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("wiring source %q is not a regular file", filepath.ToSlash(relative))
		}
		if info.Size() > maxWiringSourceBytes {
			return fmt.Errorf("wiring source %q exceeds %d bytes", filepath.ToSlash(relative), maxWiringSourceBytes)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			return err
		}
		return os.WriteFile(destination, content, info.Mode().Perm()&0o755)
	})
}

func wiringFrontendEnvironment(output string) []string {
	allowed := map[string]struct{}{
		"PATH": {}, "HOME": {}, "TMPDIR": {}, "USER": {},
		"GOCACHE": {}, "GOMODCACHE": {}, "GOPATH": {}, "GOPROXY": {},
		"GOPRIVATE": {}, "GONOPROXY": {}, "GONOSUMDB": {}, "GOSUMDB": {},
		"GOENV": {}, "GOTOOLCHAIN": {}, "GOFLAGS": {}, "GOWORK": {},
		"CGO_ENABLED": {}, "CC": {}, "CXX": {}, "SDKROOT": {},
	}
	result := make([]string, 0, len(allowed)+1)
	for _, item := range os.Environ() {
		name, _, found := strings.Cut(item, "=")
		if _, ok := allowed[name]; found && ok {
			result = append(result, item)
		}
	}
	return append(result, wiringOutputEnv+"="+output)
}

func compareWiringArtifacts(artifacts wiringArtifacts, stderr io.Writer) (bool, error) {
	changed := false
	for _, item := range []struct {
		path    string
		content []byte
	}{
		{artifacts.manifestPath, artifacts.manifest},
		{artifacts.embeddedPath, artifacts.embedded},
		{artifacts.staticPath, artifacts.static},
	} {
		if item.path == "" {
			continue
		}
		current, err := os.ReadFile(item.path)
		if errors.Is(err, os.ErrNotExist) || err == nil && !bytes.Equal(current, item.content) {
			changed = true
			_, _ = fmt.Fprintf(stderr, "%s is stale\n", filepath.ToSlash(item.path))
			continue
		}
		if err != nil {
			return false, err
		}
	}
	return changed, nil
}

func publishWiringArtifacts(artifacts wiringArtifacts) error {
	return publishWiringArtifactsWithWriter(artifacts, atomicWriteFile)
}

type wiringArtifactMutation struct {
	path      string
	content   []byte
	generated bool
	original  []byte
	existed   bool
}

func publishWiringArtifactsWithWriter(
	artifacts wiringArtifacts,
	writeFile func(string, []byte, os.FileMode) error,
) error {
	items := []struct {
		path      string
		content   []byte
		generated bool
	}{
		{artifacts.manifestPath, artifacts.manifest, false},
		{artifacts.embeddedPath, artifacts.embedded, true},
		{artifacts.staticPath, artifacts.static, true},
	}
	mutations := make([]wiringArtifactMutation, 0, len(items))
	for _, item := range items {
		if item.path == "" {
			continue
		}
		current, err := os.ReadFile(item.path)
		if !item.generated && err == nil {
			if _, parseErr := di.ParseManifest(current); parseErr != nil {
				return fmt.Errorf("refusing to overwrite non-Keelith manifest %s", filepath.ToSlash(item.path))
			}
		}
		if item.generated {
			if err == nil && !bytes.Contains(current, []byte("Code generated by Keelith")) {
				return fmt.Errorf("refusing to overwrite handwritten file %s", filepath.ToSlash(item.path))
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		mutations = append(mutations, wiringArtifactMutation{
			path: item.path, content: item.content, generated: item.generated,
			original: append([]byte(nil), current...), existed: err == nil,
		})
	}
	for index, mutation := range mutations {
		if err := writeFile(mutation.path, mutation.content, 0o644); err != nil {
			return rollbackWiringArtifacts(err, mutations[:index], writeFile)
		}
	}
	return nil
}

func rollbackWiringArtifacts(
	cause error,
	mutations []wiringArtifactMutation,
	writeFile func(string, []byte, os.FileMode) error,
) error {
	failures := []error{cause}
	for index := len(mutations) - 1; index >= 0; index-- {
		mutation := mutations[index]
		if mutation.existed {
			if err := writeFile(mutation.path, mutation.original, 0o644); err != nil {
				failures = append(failures, fmt.Errorf("restore %s: %w", filepath.ToSlash(mutation.path), err))
			}
			continue
		}
		if err := os.Remove(mutation.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = append(failures, fmt.Errorf("remove %s: %w", filepath.ToSlash(mutation.path), err))
		}
	}
	return errors.Join(failures...)
}
