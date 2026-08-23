package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/keelab/keelith/internal/generator/kitexfacade"
)

const (
	maxKitexFacadeClients = 256
	maxKitexClientBytes   = 4 * 1024 * 1024
)

type kitexFacadeOptions struct {
	path   string
	source string
	format string
	check  bool
}

type kitexFacadeFileResult struct {
	Source     string   `json:"source"`
	Output     string   `json:"output"`
	Status     string   `json:"status"`
	Interfaces []string `json:"interfaces"`
}

type kitexFacadeResult struct {
	Path      string                  `json:"path"`
	Source    string                  `json:"source"`
	Status    string                  `json:"status"`
	Created   int                     `json:"created"`
	Updated   int                     `json:"updated"`
	Unchanged int                     `json:"unchanged"`
	Drifted   int                     `json:"drifted"`
	Files     []kitexFacadeFileResult `json:"files"`
}

type kitexFacadeMutation struct {
	source     string
	output     string
	path       string
	content    []byte
	before     []byte
	exists     bool
	interfaces []string
	written    bool
}

func executeGenerateKitexFacade(
	ctx context.Context,
	options kitexFacadeOptions,
	stdout io.Writer,
	stderr io.Writer,
) int {
	project, err := existingProjectRoot(options.path)
	if err != nil {
		_, _ = fmt.Fprintf(
			stderr,
			"keelith generate kitex-facade: %v\n",
			err,
		)
		return 1
	}
	sourceRoot, err := safeProjectDirectory(project, options.source)
	if err != nil {
		_, _ = fmt.Fprintf(
			stderr,
			"keelith generate kitex-facade: %v\n",
			err,
		)
		return 1
	}
	mutations, err := planKitexFacades(ctx, project, sourceRoot)
	if err != nil {
		_, _ = fmt.Fprintf(
			stderr,
			"keelith generate kitex-facade: %v\n",
			err,
		)
		return 1
	}
	result := describeKitexFacades(
		project,
		options.source,
		mutations,
		options.check,
	)
	if !options.check {
		if err := writeKitexFacades(ctx, project, mutations); err != nil {
			_, _ = fmt.Fprintf(
				stderr,
				"keelith generate kitex-facade: %v\n",
				err,
			)
			return 1
		}
	}
	if options.format == "json" {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			_, _ = fmt.Fprintf(
				stderr,
				"keelith generate kitex-facade: encode result: %v\n",
				err,
			)
			return 1
		}
	} else {
		writeKitexFacadeText(stdout, result)
	}
	if options.check && result.Drifted > 0 {
		return 1
	}
	return 0
}

func safeProjectDirectory(root string, relative string) (string, error) {
	normalized := filepath.ToSlash(filepath.Clean(filepath.FromSlash(relative)))
	if normalized != filepath.ToSlash(relative) {
		return "", fmt.Errorf("project directory %q is not canonical", relative)
	}
	directory, err := managedPath(root, relative)
	if err != nil {
		return "", err
	}
	parts := strings.Split(
		filepath.Clean(filepath.FromSlash(relative)),
		string(filepath.Separator),
	)
	current := root
	for _, part := range parts {
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) {
			return "", fmt.Errorf(
				"project directory %q does not exist",
				relative,
			)
		}
		if statErr != nil {
			return "", fmt.Errorf(
				"inspect project directory %q: %w",
				relative,
				statErr,
			)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf(
				"project directory %q traverses a symlink",
				relative,
			)
		}
		if !info.IsDir() {
			return "", fmt.Errorf(
				"project directory %q is not a directory",
				relative,
			)
		}
	}
	return directory, nil
}

func planKitexFacades(
	ctx context.Context,
	project string,
	sourceRoot string,
) ([]kitexFacadeMutation, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	mutations := make([]kitexFacadeMutation, 0)
	err := filepath.WalkDir(sourceRoot, func(
		filePath string,
		entry fs.DirEntry,
		walkErr error,
	) error {
		if walkErr != nil {
			return walkErr
		}
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf(
				"kitex source tree contains symlink %s",
				filePath,
			)
		}
		if entry.IsDir() || entry.Name() != "client.go" {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect %s: %w", filePath, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("kitex client source is not regular: %s", filePath)
		}
		if len(mutations) >= maxKitexFacadeClients {
			return fmt.Errorf(
				"kitex client count exceeds %d",
				maxKitexFacadeClients,
			)
		}
		source, err := readBoundedKitexClient(filePath)
		if err != nil {
			return err
		}
		generated, err := kitexfacade.Generate(filePath, source)
		if err != nil {
			return err
		}
		outputPath := filepath.Join(
			filepath.Dir(filePath),
			"client.keelith.gen.go",
		)
		outputRelative, err := filepath.Rel(project, outputPath)
		if err != nil {
			return fmt.Errorf("resolve Kitex facade output: %w", err)
		}
		outputRelative = filepath.ToSlash(outputRelative)
		outputPath, err = safeProjectFile(project, outputRelative, true)
		if err != nil {
			return err
		}
		if err := ensureKitexFacadeSymbolsAvailable(
			filepath.Dir(filePath),
			filePath,
			outputPath,
			generated.Symbols,
		); err != nil {
			return err
		}
		before, exists, err := readOptional(outputPath)
		if err != nil {
			return err
		}
		if exists && !bytes.HasPrefix(
			before,
			[]byte(kitexfacade.GeneratedHeader),
		) {
			return fmt.Errorf(
				"refusing to overwrite handwritten file %s",
				outputRelative,
			)
		}
		sourceRelative, err := filepath.Rel(project, filePath)
		if err != nil {
			return fmt.Errorf("resolve Kitex client source: %w", err)
		}
		mutations = append(mutations, kitexFacadeMutation{
			source:     filepath.ToSlash(sourceRelative),
			output:     outputRelative,
			path:       outputPath,
			content:    generated.Content,
			before:     before,
			exists:     exists,
			interfaces: append([]string(nil), generated.Interfaces...),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan Kitex client sources: %w", err)
	}
	if len(mutations) == 0 {
		return nil, fmt.Errorf(
			"no Kitex-generated client.go files found under %s",
			sourceRoot,
		)
	}
	sort.Slice(mutations, func(first, second int) bool {
		return mutations[first].output < mutations[second].output
	})
	return mutations, nil
}

func readBoundedKitexClient(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("read Kitex client %s: %w", filePath, err)
	}
	content, readErr := io.ReadAll(io.LimitReader(file, maxKitexClientBytes+1))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, fmt.Errorf("read Kitex client %s: %w", filePath, err)
	}
	if len(content) > maxKitexClientBytes {
		return nil, fmt.Errorf(
			"kitex client %s exceeds %d bytes",
			filePath,
			maxKitexClientBytes,
		)
	}
	return content, nil
}

func ensureKitexFacadeSymbolsAvailable(
	directory string,
	sourcePath string,
	outputPath string,
	symbols []string,
) error {
	wanted := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		wanted[symbol] = struct{}{}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("inspect Kitex client package %s: %w", directory, err)
	}
	for _, entry := range entries {
		filePath := filepath.Join(directory, entry.Name())
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf(
				"kitex client package contains symlink %s",
				filePath,
			)
		}
		if entry.IsDir() ||
			filepath.Ext(entry.Name()) != ".go" ||
			filePath == sourcePath ||
			filePath == outputPath {
			continue
		}
		content, err := readBoundedKitexClient(filePath)
		if err != nil {
			return err
		}
		parsed, err := parser.ParseFile(
			token.NewFileSet(),
			filePath,
			content,
			parser.SkipObjectResolution,
		)
		if err != nil {
			return fmt.Errorf(
				"parse Kitex client package file %s: %w",
				filePath,
				err,
			)
		}
		for _, declaration := range parsed.Decls {
			for _, name := range topLevelDeclarationNames(declaration) {
				if _, conflict := wanted[name]; conflict {
					return fmt.Errorf(
						"generated symbol %s conflicts with %s",
						name,
						filePath,
					)
				}
			}
		}
	}
	return nil
}

func topLevelDeclarationNames(declaration ast.Decl) []string {
	switch typed := declaration.(type) {
	case *ast.FuncDecl:
		if typed.Recv == nil {
			return []string{typed.Name.Name}
		}
	case *ast.GenDecl:
		var names []string
		for _, raw := range typed.Specs {
			switch spec := raw.(type) {
			case *ast.TypeSpec:
				names = append(names, spec.Name.Name)
			case *ast.ValueSpec:
				for _, name := range spec.Names {
					names = append(names, name.Name)
				}
			}
		}
		return names
	}
	return nil
}

func describeKitexFacades(
	project string,
	source string,
	mutations []kitexFacadeMutation,
	check bool,
) kitexFacadeResult {
	result := kitexFacadeResult{
		Path:   project,
		Source: filepath.ToSlash(source),
		Status: "unchanged",
		Files:  make([]kitexFacadeFileResult, 0, len(mutations)),
	}
	for _, mutation := range mutations {
		status := "unchanged"
		switch {
		case bytes.Equal(mutation.before, mutation.content) && mutation.exists:
			result.Unchanged++
		case check:
			status = "drifted"
			result.Drifted++
		case mutation.exists:
			status = "updated"
			result.Updated++
		default:
			status = "created"
			result.Created++
		}
		result.Files = append(result.Files, kitexFacadeFileResult{
			Source:     mutation.source,
			Output:     mutation.output,
			Status:     status,
			Interfaces: append([]string(nil), mutation.interfaces...),
		})
	}
	switch {
	case result.Drifted > 0:
		result.Status = "drifted"
	case result.Created > 0 || result.Updated > 0:
		result.Status = "generated"
	}
	return result
}

func writeKitexFacades(
	ctx context.Context,
	project string,
	mutations []kitexFacadeMutation,
) error {
	rollback := func(cause error) error {
		failures := []error{cause}
		for index := len(mutations) - 1; index >= 0; index-- {
			mutation := &mutations[index]
			if !mutation.written {
				continue
			}
			current, exists, err := readOptional(mutation.path)
			if err != nil {
				failures = append(
					failures,
					fmt.Errorf("inspect %s for rollback: %w", mutation.output, err),
				)
				continue
			}
			if !exists || !bytes.Equal(current, mutation.content) {
				failures = append(
					failures,
					fmt.Errorf(
						"refusing to rollback concurrently changed file %s",
						mutation.output,
					),
				)
				continue
			}
			if mutation.exists {
				err = atomicWriteFile(mutation.path, mutation.before, 0o644)
			} else {
				err = os.Remove(mutation.path)
			}
			if err != nil {
				failures = append(
					failures,
					fmt.Errorf("rollback %s: %w", mutation.output, err),
				)
			}
		}
		return errors.Join(failures...)
	}
	for index := range mutations {
		mutation := &mutations[index]
		if mutation.exists && bytes.Equal(mutation.before, mutation.content) {
			continue
		}
		if cause := context.Cause(ctx); cause != nil {
			return rollback(cause)
		}
		safePath, err := safeProjectFile(project, mutation.output, true)
		if err != nil {
			return rollback(err)
		}
		if safePath != mutation.path {
			return rollback(fmt.Errorf(
				"kitex facade output changed path: %s",
				mutation.output,
			))
		}
		current, exists, err := readOptional(mutation.path)
		if err != nil {
			return rollback(err)
		}
		if exists != mutation.exists || !bytes.Equal(current, mutation.before) {
			return rollback(fmt.Errorf(
				"kitex facade output changed during generation: %s",
				mutation.output,
			))
		}
		if mutation.exists {
			err = atomicWriteFile(mutation.path, mutation.content, 0o644)
		} else {
			err = createExclusiveKitexFacadeFile(
				mutation.path,
				mutation.content,
			)
		}
		if err != nil {
			return rollback(fmt.Errorf("write %s: %w", mutation.output, err))
		}
		mutation.written = true
	}
	return nil
}

func createExclusiveKitexFacadeFile(path string, content []byte) error {
	file, err := os.OpenFile(
		path,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o644,
	)
	if err != nil {
		return err
	}
	var writeErr error
	if _, err := file.Write(content); err != nil {
		writeErr = err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

func writeKitexFacadeText(output io.Writer, result kitexFacadeResult) {
	for _, file := range result.Files {
		_, _ = fmt.Fprintf(
			output,
			"%s Kitex facade %s from %s (%s)\n",
			file.Status,
			file.Output,
			file.Source,
			strings.Join(file.Interfaces, ", "),
		)
	}
}
