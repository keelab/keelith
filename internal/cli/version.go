package cli

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/keelab/keelith/internal/version"
)

type versionResult struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func executeVersion(format string, stdout, stderr io.Writer) int {
	result := versionResult{Name: "keelith", Version: version.String()}
	if format == "json" {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			_, _ = fmt.Fprintf(stderr, "keelith: encode version: %v\n", err)
			return 1
		}
		return 0
	}
	_, _ = fmt.Fprintf(stdout, "%s %s\n", result.Name, result.Version)
	return 0
}
