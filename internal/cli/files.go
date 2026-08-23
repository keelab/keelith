package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func managedPath(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("unsafe managed path %q", relative)
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == "." ||
		clean == ".." ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe managed path %q", relative)
	}
	path := filepath.Join(root, clean)
	within, err := filepath.Rel(root, path)
	if err != nil ||
		within == ".." ||
		strings.HasPrefix(within, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("managed path escapes project %q", relative)
	}
	return path, nil
}

func readOptional(path string) ([]byte, bool, error) {
	content, err := os.ReadFile(path)
	if err == nil {
		return content, true, nil
	}
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("read %s: %w", path, err)
}

func atomicWriteFile(path string, content []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".keelith-write-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
