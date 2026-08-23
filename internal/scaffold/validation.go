package scaffold

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	// ErrInvalidInput reports invalid project, contract, or declaration data.
	ErrInvalidInput = errors.New("scaffold: invalid input")
	// ErrConflict reports an existing declaration or file that cannot be safely
	// merged with the requested change.
	ErrConflict = errors.New("scaffold: conflict")
)

var (
	modulePattern  = regexp.MustCompile(`^[A-Za-z0-9._~/-]+$`)
	servicePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

func validateModule(module string) error {
	if module == "" ||
		!modulePattern.MatchString(module) ||
		strings.HasPrefix(module, "/") ||
		strings.HasSuffix(module, "/") ||
		strings.Contains(module, "//") ||
		strings.Contains(module, "/../") ||
		strings.Contains(module, "/./") {
		return fmt.Errorf("%w: module path %q", ErrInvalidInput, module)
	}
	return nil
}

func safeOutputPath(root, relative string) (string, error) {
	if filepath.IsAbs(relative) {
		return "", fmt.Errorf("%w: absolute output path %q", ErrInvalidInput, relative)
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == "." ||
		clean == ".." ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: unsafe output path %q", ErrInvalidInput, relative)
	}
	output := filepath.Join(root, clean)
	relativeToRoot, err := filepath.Rel(root, output)
	if err != nil ||
		relativeToRoot == ".." ||
		strings.HasPrefix(relativeToRoot, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: output escapes root %q", ErrInvalidInput, relative)
	}
	return output, nil
}
