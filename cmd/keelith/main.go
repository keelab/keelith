// Command keelith provides project generation and diagnostics.
package main

import (
	"context"
	"os"

	"github.com/keelab/keelith/internal/cli"
)

func main() {
	os.Exit(cli.Execute(
		context.Background(),
		os.Args[1:],
		os.Stdout,
		os.Stderr,
	))
}
