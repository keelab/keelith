package ops

import (
	"context"
	"math"

	kapp "github.com/keelab/keelith/app"
	kclient "github.com/keelab/keelith/client"
	"github.com/keelab/keelith/governance/breaker"
	"github.com/keelab/keelith/governance/inbound"
	"github.com/keelab/keelith/governance/loadshed"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/programmable/projection"
	"github.com/keelab/keelith/registry/configured"
	"github.com/keelab/keelith/saga"
	"github.com/keelab/keelith/security/authz"
	kgrpc "github.com/keelab/keelith/transport/grpc"
	"github.com/keelab/keelith/transport/grpcauth"
	khttp "github.com/keelab/keelith/transport/http"
	"github.com/keelab/keelith/transport/tlsconfig"
)

// ApplicationRuntimeStatus adapts an App lifecycle without exposing terminal
// errors, server identities, resource addresses, or hook details.
func ApplicationRuntimeStatus(
	instance *kapp.App,
) RuntimeStatusProvider {
	if instance == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := instance.Description()
		return RuntimeStatus{
			State:    description.State.String(),
			Ready:    description.State == kapp.StateReady,
			Degraded: description.Failed,
			Capabilities: []string{
				"explicit-drain",
				"failure-terminal-state",
				"single-run",
			},
		}, nil
	}
}

// ProjectionRuntimeStatus adapts one projection shard without exposing row
// keys, values, cursor contents, tenant identities, or source errors.
func ProjectionRuntimeStatus(
	admin projection.OwnerAdmin,
	shard projection.ShardID,
) RuntimeStatusProvider {
	if admin == nil || shard.Validate() != nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		stats, err := admin.ProjectionStats(ctx, shard)
		if err != nil {
			return RuntimeStatus{}, err
		}
		if err := stats.Validate(); err != nil {
			return RuntimeStatus{}, err
		}
		lag := stats.HeadOffset - stats.ProtectedOffset
		gap := uint64(0)
		if stats.FloorOffset > 0 {
			gap = 1
		}
		active := stats.ActiveSubscribers
		if active > uint64(math.MaxInt) {
			active = uint64(math.MaxInt)
		}
		return RuntimeStatus{
			State:    "active",
			Ready:    true,
			Degraded: lag > stats.HeadOffset/2 && lag > 100,
			Active:   int(active),
			Counters: []RuntimeCounter{
				{Name: "floor", Value: stats.FloorOffset},
				{Name: "gap", Value: gap},
				{Name: "head", Value: stats.HeadOffset},
				{Name: "lag", Value: lag},
				{Name: "log_bytes", Value: stats.LogBytes},
				{Name: "row_bytes", Value: stats.RowBytes},
				{Name: "rows", Value: stats.Rows},
				{Name: "subscribers", Value: stats.ActiveSubscribers},
			},
			Capabilities: []string{
				"bounded-compaction",
				"forced-snapshot",
				"protected-checkpoint",
			},
		}, nil
	}
}

// AuthorizationRuntimeStatus adapts dynamic RBAC without exposing revisions,
// operation matchers, roles, scopes, principals, or decision reasons.
func AuthorizationRuntimeStatus(
	binding *authz.ConfigBinding,
) RuntimeStatusProvider {
	if binding == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := binding.Description()
		state := "bootstrap"
		if description.Loaded {
			state = "active"
		}
		return RuntimeStatus{
			State:    state,
			Ready:    description.Loaded,
			Degraded: description.Failed,
			Active:   description.Rules,
			Counters: []RuntimeCounter{
				{Name: "allowed", Value: description.Allowed},
				{Name: "denied", Value: description.Denied},
				{Name: "evaluations", Value: description.Evaluations},
				{Name: "rules", Value: uint64(description.Rules)},
				{Name: "updates", Value: description.Updates},
			},
			Capabilities: []string{
				"atomic-reload",
				"deny-by-default",
				"last-good",
				"rbac",
			},
		}, nil
	}
}

// PolicyConfigRuntimeStatus adapts a dynamic Method Policy binding to the
// value-free runtime catalog schema.
//
// The provider exposes only lifecycle state and bounded rule counts. It never
// exposes policy values, operation identities, configuration paths, revisions,
// or validation errors.
func PolicyConfigRuntimeStatus(
	binding *policy.ConfigBinding,
) RuntimeStatusProvider {
	if binding == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := binding.Description()
		state := "bootstrap"
		if description.Loaded {
			state = "active"
		}
		return RuntimeStatus{
			State:    state,
			Ready:    description.Loaded,
			Degraded: description.Failed,
			Active: description.ServiceRules +
				description.MatcherRules +
				description.MethodRules,
			Counters: []RuntimeCounter{
				{
					Name:  "matcher_rules",
					Value: uint64(description.MatcherRules),
				},
				{
					Name:  "method_rules",
					Value: uint64(description.MethodRules),
				},
				{
					Name:  "service_rules",
					Value: uint64(description.ServiceRules),
				},
			},
			Capabilities: []string{
				"atomic-reload",
				"hierarchical-resolution",
			},
		}, nil
	}
}

// ConfiguredDiscoveryRuntimeStatus adapts configured discovery to a bounded,
// value-free runtime status.
//
// Service names, instance IDs, endpoints, metadata, configuration paths, and
// revisions are intentionally omitted.
func ConfiguredDiscoveryRuntimeStatus(
	binding *configured.ConfigBinding,
) RuntimeStatusProvider {
	if binding == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := binding.Description()
		state := "bootstrap"
		if description.Loaded {
			state = "active"
		}
		return RuntimeStatus{
			State:    state,
			Ready:    description.Loaded,
			Degraded: description.Failed,
			Active:   description.Watchers,
			Counters: []RuntimeCounter{
				{
					Name:  "instances",
					Value: uint64(description.Instances),
				},
				{
					Name:  "services",
					Value: uint64(description.Services),
				},
				{
					Name:  "updates",
					Value: description.Updates,
				},
				{
					Name:  "watchers",
					Value: uint64(description.Watchers),
				},
			},
			Capabilities: []string{
				"atomic-reload",
				"full-snapshot-watch",
			},
		}, nil
	}
}

// InboundRuntimeStatus adapts server admission and overload governance to a
// bounded aggregate status.
//
// Operation keys and resolved policy values are never copied.
func InboundRuntimeStatus(
	runtime *inbound.Runtime,
) RuntimeStatusProvider {
	if runtime == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := runtime.Describe()
		rateAccepted := uint64(0)
		rateRejected := uint64(0)
		rateInflight := 0
		for _, limiter := range description.RateLimits {
			rateAccepted += limiter.Accepted
			rateRejected += limiter.Rejected
			rateInflight += limiter.Inflight
		}
		shedAccepted := uint64(0)
		shedRejected := uint64(0)
		shedInflight := 0
		for _, shedder := range description.LoadShedders {
			shedAccepted += shedder.Accepted
			shedRejected += shedder.Rejected
			shedInflight += shedder.Inflight
		}
		active := rateInflight
		if shedInflight > active {
			active = shedInflight
		}
		return RuntimeStatus{
			State:    "active",
			Ready:    true,
			Degraded: rateRejected > 0 || shedRejected > 0,
			Active:   active,
			Counters: []RuntimeCounter{
				{
					Name:  "load_shed_accepted",
					Value: shedAccepted,
				},
				{
					Name:  "load_shed_rejected",
					Value: shedRejected,
				},
				{
					Name:  "load_shedders",
					Value: uint64(len(description.LoadShedders)),
				},
				{
					Name:  "rate_limit_accepted",
					Value: rateAccepted,
				},
				{
					Name:  "rate_limit_rejected",
					Value: rateRejected,
				},
				{
					Name:  "rate_limiters",
					Value: uint64(len(description.RateLimits)),
				},
			},
			Capabilities: []string{
				"adaptive-load-shedding",
				"local-rate-limit",
				"policy-snapshot",
				"request-timeout",
			},
		}, nil
	}
}

// RuntimeCPURuntimeStatus adapts the runtime CPU sampler without exposing
// scheduler internals or raw runtime metrics.
func RuntimeCPURuntimeStatus(
	cpu *loadshed.RuntimeCPU,
) RuntimeStatusProvider {
	if cpu == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := cpu.Describe()
		usage := math.Round(description.Usage * 10_000)
		usage = math.Max(0, math.Min(10_000, usage))
		return RuntimeStatus{
			State:    string(description.State),
			Ready:    description.Running,
			Degraded: description.Failed,
			Counters: []RuntimeCounter{
				{
					Name:  "failures",
					Value: description.Failures,
				},
				{
					Name:  "samples",
					Value: description.Samples,
				},
				{
					Name:  "usage_basis_points",
					Value: uint64(usage),
				},
			},
			Capabilities: []string{
				"lock-free-read",
				"runtime-cpu-sampling",
			},
		}, nil
	}
}

// GRPCManagedDependencyRuntimeStatus adapts one lifecycle-owned dynamic gRPC
// dependency to bounded, value-free runtime diagnostics.
//
// Service identity, endpoints, topology revision, and provider errors are not
// copied from the detailed Description.
func GRPCManagedDependencyRuntimeStatus(
	dependency *kgrpc.ManagedDependency,
) RuntimeStatusProvider {
	if dependency == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := dependency.Describe()
		routerRunning := description.Router.State == kclient.StateRunning
		connectionRunning := description.Connection.State ==
			kgrpc.DiscoveryStateRunning
		degraded := description.Router.Stale ||
			routerRunning && !description.Router.Connected ||
			routerRunning && description.Router.Instances == 0 ||
			description.Connection.DialFailures > 0
		counters := []RuntimeCounter{
			{
				Name:  "connections",
				Value: uint64(description.Connection.Connections),
			},
			{
				Name:  "dial_failures",
				Value: description.Connection.DialFailures,
			},
			{
				Name:  "dialing",
				Value: uint64(description.Connection.Dialing),
			},
			{
				Name:  "instances",
				Value: uint64(description.Router.Instances),
			},
			{
				Name:  "locality_tiers",
				Value: uint64(description.PreferenceTiers),
			},
			{
				Name:  "reconnects",
				Value: description.Router.Reconnects,
			},
			{
				Name:  "retired",
				Value: uint64(description.Connection.Retired),
			},
		}
		capabilities := []string{
			"dynamic-discovery",
			"latest-topology",
			"pooled-connections",
			"selector-feedback",
		}
		if description.PreferenceTiers != 0 {
			capabilities = append(capabilities, "ordered-locality")
		}
		return RuntimeStatus{
			State:        string(description.Connection.State),
			Ready:        routerRunning && connectionRunning,
			Degraded:     degraded,
			Active:       description.Connection.Active,
			Counters:     counters,
			Capabilities: capabilities,
		}, nil
	}
}

// HTTPBearerRuntimeStatus exposes the shared Secret-backed bearer lifecycle
// under an HTTP-specific profile name without exposing token material.
func HTTPBearerRuntimeStatus(
	bearer *grpcauth.Bearer,
) RuntimeStatusProvider {
	return GRPCBearerRuntimeStatus(bearer)
}

// HTTPManagedDependencyRuntimeStatus adapts one lifecycle-owned dynamic HTTP
// dependency to bounded, value-free runtime diagnostics.
//
// Service identity, endpoint values, topology revision, and provider errors
// are intentionally omitted.
func HTTPManagedDependencyRuntimeStatus(
	dependency *khttp.ManagedDependency,
) RuntimeStatusProvider {
	if dependency == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := dependency.Describe()
		routerRunning := description.Router.State == kclient.StateRunning
		degraded := description.Router.Stale ||
			routerRunning && !description.Router.Connected ||
			routerRunning && description.Router.Instances == 0
		capabilities := []string{
			"dynamic-discovery",
			"http-connection-pooling",
			"selector-feedback",
		}
		if description.Schemes > 1 {
			capabilities = append(capabilities, "multi-scheme")
		}
		if description.PreferenceTiers != 0 {
			capabilities = append(capabilities, "ordered-locality")
		}
		return RuntimeStatus{
			State:    string(description.State),
			Ready:    description.State == khttp.ManagedDependencyRunning && routerRunning,
			Degraded: degraded,
			Active:   description.ActiveRequests,
			Counters: []RuntimeCounter{
				{
					Name:  "instances",
					Value: uint64(description.Router.Instances),
				},
				{
					Name:  "locality_tiers",
					Value: uint64(description.PreferenceTiers),
				},
				{
					Name:  "reconnects",
					Value: description.Router.Reconnects,
				},
				{
					Name:  "schemes",
					Value: uint64(description.Schemes),
				},
			},
			Capabilities: capabilities,
		}, nil
	}
}

// OutboundRuntimeStatus adapts a client Outbound to bounded, value-free
// runtime diagnostics.
//
// Dependency identities, operation keys, endpoints, policy values, revisions,
// and errors are intentionally reduced to aggregate counters.
func OutboundRuntimeStatus(
	outbound *kclient.Outbound,
) RuntimeStatusProvider {
	if outbound == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := outbound.Describe()
		unhealthyBreakers := 0
		for _, item := range description.Dependency.Breakers {
			if item.State != breaker.StateClosed {
				unhealthyBreakers++
			}
		}
		bulkheadInflight := 0
		bulkheadQueued := 0
		for _, item := range description.Dependency.Bulkheads {
			bulkheadInflight += item.Inflight
			bulkheadQueued += item.Queued
		}
		ejectedInstances := 0
		for _, item := range description.Dependency.Instances {
			if item.Ejected {
				ejectedInstances++
			}
		}
		counters := []RuntimeCounter{
			{
				Name:  "breaker_unhealthy",
				Value: uint64(unhealthyBreakers),
			},
			{
				Name:  "breakers",
				Value: uint64(len(description.Dependency.Breakers)),
			},
			{
				Name:  "bulkhead_inflight",
				Value: uint64(bulkheadInflight),
			},
			{
				Name:  "bulkhead_queued",
				Value: uint64(bulkheadQueued),
			},
			{
				Name:  "bulkheads",
				Value: uint64(len(description.Dependency.Bulkheads)),
			},
			{
				Name:  "ejected_instances",
				Value: uint64(ejectedInstances),
			},
			{
				Name:  "instances",
				Value: uint64(len(description.Dependency.Instances)),
			},
			{
				Name:  "middleware",
				Value: uint64(len(description.Middleware)),
			},
			{
				Name:  "retry_budgets",
				Value: uint64(len(description.Dependency.Retry)),
			},
			{
				Name:  "stream_middleware",
				Value: uint64(len(description.StreamMiddleware)),
			},
		}
		capabilities := []string{
			"instance-outlier",
			"logical-call-timeout",
			"policy-snapshot",
			"retry-hedging",
			"service-breaker",
		}
		if description.Dependency.Admission.Enabled {
			counters = append(counters,
				RuntimeCounter{
					Name:  "admission_accepted",
					Value: description.Dependency.Admission.Accepted,
				},
				RuntimeCounter{
					Name:  "admission_categories",
					Value: uint64(description.Dependency.Admission.Categories),
				},
				RuntimeCounter{
					Name:  "admission_dropped",
					Value: description.Dependency.Admission.Dropped,
				},
				RuntimeCounter{
					Name:  "admission_services",
					Value: uint64(description.Dependency.Admission.Services),
				},
				RuntimeCounter{
					Name:  "admission_updates",
					Value: description.Dependency.Admission.Updates,
				},
			)
			capabilities = append(capabilities, "dynamic-admission")
		}
		return RuntimeStatus{
			State:        "active",
			Ready:        true,
			Degraded:     unhealthyBreakers > 0 || ejectedInstances > 0,
			Active:       bulkheadInflight,
			Counters:     counters,
			Capabilities: capabilities,
		}, nil
	}
}

// SagaRuntimeStatus adapts aggregate orchestration counters without exposing
// instance IDs, step names, durable failure reasons, or business payloads.
func SagaRuntimeStatus(engine *saga.Engine) RuntimeStatusProvider {
	if engine == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := engine.Description()
		return RuntimeStatus{
			State:  "available",
			Ready:  true,
			Active: int(description.Active),
			Counters: []RuntimeCounter{
				{Name: "action_failures", Value: description.ActionFailures},
				{Name: "compensated", Value: description.Compensated},
				{
					Name:  "compensation_failures",
					Value: description.CompensationFailures,
				},
				{Name: "completed", Value: description.Completed},
				{Name: "contended", Value: description.Contended},
				{Name: "lease_losses", Value: description.LeaseLosses},
				{
					Name:  "repository_failures",
					Value: description.RepositoryFailures,
				},
				{Name: "started", Value: description.Started},
				{
					Name:  "terminal_failures",
					Value: description.TerminalFailures,
				},
			},
			Capabilities: []string{
				"compensation",
				"durable-resume",
				"fencing",
				"revision-cas",
			},
		}, nil
	}
}

// TLSSecretRuntimeStatus adapts certificate reload state without exposing
// secret references, filesystem paths, material versions, or error text.
func TLSSecretRuntimeStatus(
	watcher *tlsconfig.SecretWatcher,
) RuntimeStatusProvider {
	if watcher == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := watcher.Description()
		return RuntimeStatus{
			State:    string(description.State),
			Ready:    description.Running && description.Reloads > 0,
			Degraded: description.LastFailed,
			Counters: []RuntimeCounter{
				{Name: "failures", Value: description.Failures},
				{Name: "reconnects", Value: description.Reconnects},
				{Name: "reloads", Value: description.Reloads},
			},
			Capabilities: []string{
				"atomic-reload",
				"last-good",
				"reconnect",
			},
		}, nil
	}
}

// GRPCBearerRuntimeStatus adapts Secret-backed gRPC bearer state without
// exposing token material, references, provider revisions, request targets, or
// error text.
func GRPCBearerRuntimeStatus(
	bearer *grpcauth.Bearer,
) RuntimeStatusProvider {
	if bearer == nil {
		return nil
	}
	return func(ctx context.Context) (RuntimeStatus, error) {
		if ctx == nil {
			return RuntimeStatus{}, ErrInvalidOption
		}
		if cause := context.Cause(ctx); cause != nil {
			return RuntimeStatus{}, cause
		}
		description := bearer.Description()
		return RuntimeStatus{
			State:    string(description.State),
			Ready:    description.Ready,
			Degraded: description.Degraded,
			Counters: []RuntimeCounter{
				{Name: "failures", Value: description.Failures},
				{Name: "reconnects", Value: description.Reconnects},
				{Name: "reloads", Value: description.Reloads},
			},
			Capabilities: []string{
				"bearer",
				"hot-reload",
				"last-good",
				"secret-reference",
				"transport-security-required",
			},
		}, nil
	}
}
