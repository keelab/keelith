// Package cli implements the Keelith developer command.
package cli

import (
	"context"
	"fmt"
	"io"
)

// Execute runs a command and returns a process-compatible exit code.
func Execute(
	ctx context.Context,
	arguments []string,
	stdout io.Writer,
	stderr io.Writer,
) int {
	if ctx == nil {
		_, _ = fmt.Fprintln(stderr, "keelith: context is nil")
		return 1
	}
	return execute(ctx, arguments, stdout, stderr, openConfigStore)
}

func execute(
	ctx context.Context,
	arguments []string,
	stdout io.Writer,
	stderr io.Writer,
	opener configStoreOpener,
) int {
	runtime := &commandRuntime{
		stdout: stdout, stderr: stderr, configOpener: opener,
	}
	root := newRootCommand(runtime)
	root.SetArgs(arguments)
	executed, err := root.ExecuteContextC(ctx)
	if err == nil {
		return runtime.exitCode
	}
	_, _ = fmt.Fprintf(stderr, "keelith: %v\n", err)
	if executed != nil {
		executed.SetOut(stderr)
		_ = executed.Usage()
	}
	return 2
}
