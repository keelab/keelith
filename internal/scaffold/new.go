package scaffold

import (
	"context"
	"errors"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

const (
	// Keep this version and the self-module checksums in templates.go aligned
	// with the first published release that contains the root facade. The
	// current checkout still uses the last published baseline until that
	// release is tagged.
	defaultFrameworkVersion = "v0.0.4"
	defaultGoVersion        = "1.26.6"
)

// NewProjectOptions describes a new project to create.
type NewProjectOptions struct {
	Project          string
	Module           string
	Template         string
	FrameworkVersion string
}

// NewProjectResult describes the files created by NewProject.
type NewProjectResult struct {
	Project          string
	Module           string
	Template         string
	FrameworkVersion string
	Created          []string
}

// NewProject creates a small runnable Go application without contacting any
// external service. Existing non-empty directories are never modified.
func NewProject(ctx context.Context, options NewProjectOptions) (NewProjectResult, error) {
	if ctx == nil {
		return NewProjectResult{}, fmt.Errorf("%w: context is nil", ErrInvalidInput)
	}
	if cause := context.Cause(ctx); cause != nil {
		return NewProjectResult{}, cause
	}

	project, err := normalizeNewProjectPath(options.Project)
	if err != nil {
		return NewProjectResult{}, err
	}
	template := strings.ToLower(strings.TrimSpace(options.Template))
	if template == "" {
		template = "minimal"
	}
	if !supportedProjectTemplate(template) {
		return NewProjectResult{}, fmt.Errorf(
			"%w: template %q is unsupported; use minimal, service, or production",
			ErrInvalidInput,
			template,
		)
	}

	name := filepath.Base(project)
	appName := identifierName(name)
	module := strings.TrimSpace(options.Module)
	if module == "" {
		module = "example.com/" + name
	}
	if err := validateModule(module); err != nil {
		return NewProjectResult{}, err
	}
	frameworkVersion := strings.TrimSpace(options.FrameworkVersion)
	if frameworkVersion == "" {
		frameworkVersion = defaultFrameworkVersion
	}
	if err := validateFrameworkVersion(frameworkVersion); err != nil {
		return NewProjectResult{}, err
	}

	files, err := projectFiles(template, module, appName, frameworkVersion)
	if err != nil {
		return NewProjectResult{}, err
	}
	for relative, content := range files {
		if filepath.Ext(relative) != ".go" {
			continue
		}
		formatted, formatErr := format.Source(content)
		if formatErr != nil {
			return NewProjectResult{}, fmt.Errorf(
				"%w: format generated %s: %w",
				ErrInvalidInput,
				relative,
				formatErr,
			)
		}
		files[relative] = formatted
	}
	if err := createEmptyProject(project); err != nil {
		return NewProjectResult{}, err
	}
	paths := make([]string, 0, len(files))
	for relative := range files {
		paths = append(paths, relative)
	}
	sort.Strings(paths)
	created := make([]string, 0, len(paths))
	rollback := func(cause error) (NewProjectResult, error) {
		var rollbackErrors []error
		for index := len(created) - 1; index >= 0; index-- {
			path, pathErr := safeAddOutputPath(project, created[index], true)
			if pathErr != nil {
				rollbackErrors = append(rollbackErrors, pathErr)
				continue
			}
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				rollbackErrors = append(rollbackErrors, removeErr)
			}
		}
		if removeErr := removeEmptyProjectDirectories(project); removeErr != nil {
			rollbackErrors = append(rollbackErrors, removeErr)
		}
		if removeErr := os.Remove(project); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			rollbackErrors = append(rollbackErrors, removeErr)
		}
		return NewProjectResult{}, errors.Join(append([]error{cause}, rollbackErrors...)...)
	}

	for _, relative := range paths {
		if cause := context.Cause(ctx); cause != nil {
			return rollback(cause)
		}
		path, pathErr := safeAddOutputPath(project, relative, true)
		if pathErr != nil {
			return rollback(pathErr)
		}
		if mkdirErr := os.MkdirAll(filepath.Dir(path), 0o755); mkdirErr != nil {
			return rollback(fmt.Errorf("create %s directory: %w", relative, mkdirErr))
		}
		if createErr := createExclusiveFile(path, files[relative]); createErr != nil {
			return rollback(fmt.Errorf("%w: create %s: %w", ErrConflict, relative, createErr))
		}
		created = append(created, relative)
	}
	if template != "minimal" {
		synchronized, syncErr := SyncServices(ctx, project)
		if syncErr != nil {
			return rollback(fmt.Errorf("synchronize generated services: %w", syncErr))
		}
		created = append(created, synchronized.Created...)
		components, componentErr := SyncComponents(ctx, project, module)
		if componentErr != nil {
			return rollback(fmt.Errorf("synchronize generated components: %w", componentErr))
		}
		created = append(created, components.Created...)
		sort.Strings(created)
	}

	return NewProjectResult{
		Project:          project,
		Module:           module,
		Template:         template,
		FrameworkVersion: frameworkVersion,
		Created:          created,
	}, nil
}

func validateFrameworkVersion(version string) error {
	if version == "" || strings.TrimSpace(version) != version ||
		!frameworkVersionPattern.MatchString(version) {
		return fmt.Errorf("%w: framework version %q is invalid", ErrInvalidInput, version)
	}
	return nil
}

func removeEmptyProjectDirectories(project string) error {
	var directories []string
	err := filepath.WalkDir(project, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path != project && entry.IsDir() {
			directories = append(directories, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("rollback project directories: %w", err)
	}
	sort.Slice(directories, func(first, second int) bool {
		return len(directories[first]) > len(directories[second])
	})
	for _, directory := range directories {
		if err := os.Remove(directory); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("rollback directory %s: %w", filepath.ToSlash(directory), err)
		}
	}
	return nil
}

func normalizeNewProjectPath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%w: project path is empty", ErrInvalidInput)
	}
	project, err := filepath.Abs(value)
	if err != nil {
		return "", fmt.Errorf("%w: project path: %w", ErrInvalidInput, err)
	}
	if project == string(filepath.Separator) {
		return "", fmt.Errorf("%w: project path cannot be filesystem root", ErrInvalidInput)
	}
	return filepath.Clean(project), nil
}

func createEmptyProject(project string) error {
	parent := filepath.Dir(project)
	// Follow an explicitly supplied parent path here. On macOS, /tmp is a
	// symlink to /private/tmp and should remain a valid target for `keelith new`.
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return fmt.Errorf("%w: inspect project parent: %w", ErrInvalidInput, err)
	}
	if !parentInfo.IsDir() {
		return fmt.Errorf("%w: project parent is not a regular directory", ErrInvalidInput)
	}
	if err := os.Mkdir(project, 0o755); err != nil {
		return fmt.Errorf("%w: create project directory: %w", ErrConflict, err)
	}
	return nil
}

func minimalProjectFiles(module, name, frameworkVersion string) map[string][]byte {
	return map[string][]byte{
		".gitignore": []byte(".secrets/\n"),
		"README.md": fmt.Appendf(nil,
			"# %s\n\n"+
				"最小 Keelith HTTP 服务示例。\n\n"+
				"运行：\n\n"+
				"```bash\n"+
				"go run .\n"+
				"```\n\n"+
				"然后访问：\n\n"+
				"```bash\n"+
				"curl http://127.0.0.1:8080/ping\n"+
				"```\n\n"+
				"本示例不连接数据库、缓存、消息队列或远程配置；需要这些能力时，再升级到标准服务模板。\n",
			name,
		),
		"go.mod": renderProjectGoMod(module, frameworkVersion, false),
		"go.sum": renderProjectGoSum(frameworkVersion),
		"main.go": fmt.Appendf(nil, `package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	keelith "github.com/keelab/keelith"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application, err := keelith.New(
		keelith.WithName(%q),
		keelith.WithHTTP(":8080"),
		keelith.WithRoute(http.MethodGet, "/ping", func(context.Context, *http.Request) (any, error) {
			return map[string]string{"message": "pong"}, nil
		}),
	)
	if err != nil {
		return err
	}
	if err := application.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
	`, name),
	}
}

func identifierName(value string) string {
	var builder strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' {
			if r == '-' {
				builder.WriteByte('_')
			} else {
				builder.WriteRune(r)
			}
			continue
		}
		builder.WriteByte('_')
	}
	result := builder.String()
	if result == "" {
		return "keelith_app"
	}
	first := rune(result[0])
	if !unicode.IsLetter(first) && first != '_' {
		return "app_" + result
	}
	return result
}
