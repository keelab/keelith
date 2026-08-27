package jwt

import (
	"context"

	"github.com/keelab/keelith/ops"
)

// RemoteRuntimeStatus adapts a rotating JWKS provider to Keelith's bounded,
// value-free runtime catalog.
func RemoteRuntimeStatus(set *RemoteKeySet) ops.RuntimeStatusProvider {
	if set == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ops.ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := set.Description()
		return ops.RuntimeStatus{
			State:    string(description.State),
			Ready:    description.Ready,
			Degraded: description.LastFailed,
			Active:   description.KeyCount,
			Counters: []ops.RuntimeCounter{
				{Name: "failures", Value: description.Failures},
				{Name: "key_misses", Value: description.KeyMisses},
				{Name: "refreshes", Value: description.Refreshes},
			},
			Capabilities: []string{
				"asymmetric-only",
				"last-good",
				"on-demand-refresh",
				"periodic-refresh",
			},
		}, nil
	}
}

// AuthenticatorRuntimeStatus adapts aggregate authentication results without
// exposing issuer, audience, subject, claim, key, or credential values.
func AuthenticatorRuntimeStatus(
	authenticator *Authenticator,
) ops.RuntimeStatusProvider {
	if authenticator == nil {
		return nil
	}
	return func(ctx context.Context) (ops.RuntimeStatus, error) {
		if ctx == nil {
			return ops.RuntimeStatus{}, ops.ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return ops.RuntimeStatus{}, cause
		}
		description := authenticator.Description()
		return ops.RuntimeStatus{
			State: "active",
			Ready: true,
			Counters: []ops.RuntimeCounter{
				{Name: "key_failures", Value: description.KeyFailures},
				{Name: "rejected", Value: description.Rejected},
				{Name: "successful", Value: description.Successful},
			},
			Capabilities: []string{
				"asymmetric-only",
				"claim-validation",
				"principal-mapping",
			},
		}, nil
	}
}
