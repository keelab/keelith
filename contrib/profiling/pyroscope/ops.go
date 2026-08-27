package pyroscope

import (
	"context"

	"github.com/keelab/keelith/ops"
)

// RuntimeStatus adapts continuous profiling to the value-free Ops catalog.
func RuntimeStatus(runtime *Runtime) ops.RuntimeStatusProvider {
	if runtime == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ops.ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := runtime.Describe()
		active := 0
		if description.State == StateRunning {
			active = 1
		}
		capabilities := []string{
			"backend:pyroscope",
		}
		if description.WatchesCredential {
			capabilities = append(capabilities, "credential-watch")
		}
		if description.UsesCPU {
			capabilities = append(
				capabilities,
				"pprof-labels",
				"process-cpu-owner",
			)
		}
		capabilities = append(
			capabilities,
			"push",
			"secret-reference",
		)
		if description.TLS {
			capabilities = append(capabilities, "transport:https")
		} else {
			capabilities = append(capabilities, "transport:http")
		}
		return ops.RuntimeStatus{
			State:    string(description.State),
			Ready:    description.Ready,
			Degraded: description.Degraded,
			Active:   active,
			Counters: []ops.RuntimeCounter{
				{Name: "credential_failures", Value: description.CredentialFailures},
				{Name: "credential_reconnects", Value: description.CredentialReconnects},
				{Name: "credential_rotations", Value: description.CredentialRotations},
				{Name: "export_failures", Value: description.ExportFailures},
				{Name: "failures", Value: description.Failures},
				{Name: "starts", Value: description.Starts},
				{Name: "stops", Value: description.Stops},
			},
			Capabilities: capabilities,
		}, nil
	}
}
