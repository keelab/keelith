package projectinfo

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const maxGoModBytes = 1024 * 1024

// Project is the standard Go project identity used by Keelith developer tools.
type Project struct {
	Root      string
	Module    string
	BufConfig string
}

// Load resolves root and reads its regular, non-symlinked go.mod. A Buf
// configuration is optional and is reported when present.
func Load(root string) (Project, error) {
	resolved, err := filepath.Abs(strings.TrimSpace(root))
	if err != nil {
		return Project{}, fmt.Errorf("resolve project: %w", err)
	}
	resolved, err = filepath.EvalSymlinks(resolved)
	if err != nil {
		return Project{}, fmt.Errorf("resolve project symlinks: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return Project{}, errors.New("project is not a directory")
	}
	module, err := readModule(filepath.Join(resolved, "go.mod"))
	if err != nil {
		return Project{}, err
	}
	bufConfig, err := optionalRegularFile(resolved, "buf.yaml")
	if err != nil {
		return Project{}, err
	}
	return Project{Root: resolved, Module: module, BufConfig: bufConfig}, nil
}

func readModule(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("inspect go.mod: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("go.mod must be a regular file")
	}
	if info.Size() > maxGoModBytes {
		return "", fmt.Errorf("go.mod exceeds %d bytes", maxGoModBytes)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read go.mod: %w", err)
	}
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(strings.SplitN(line, "//", 2)[0])
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "module" {
			continue
		}
		module := fields[1]
		if unquoted, unquoteErr := strconv.Unquote(module); unquoteErr == nil {
			module = unquoted
		}
		if module == "" || strings.ContainsAny(module, "\r\n\t ") {
			break
		}
		return module, nil
	}
	return "", errors.New("go.mod does not declare a valid module path")
}

func optionalRegularFile(root, relative string) (string, error) {
	path := filepath.Join(root, relative)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("inspect %s: %w", relative, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s must be a regular file", relative)
	}
	return path, nil
}
