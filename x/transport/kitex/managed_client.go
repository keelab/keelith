package kitex

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	kclient "github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/utils"
)

var (
	// ErrClientSuiteNotApplied means a ManagedClientFactory ignored the
	// supplied guarded Suite.
	ErrClientSuiteNotApplied = errors.New(
		"kitex profile: managed client suite was not applied",
	)
	// ErrClientSuiteAppliedMoreThanOnce means a factory installed one Suite
	// repeatedly and would duplicate metadata and middleware semantics.
	ErrClientSuiteAppliedMoreThanOnce = errors.New(
		"kitex profile: managed client suite was applied more than once",
	)
	// ErrNativeGovernanceConflict means native Kitex governance would execute
	// beside Keelith's single outbound policy authority.
	ErrNativeGovernanceConflict = errors.New(
		"kitex profile: native governance conflicts with Keelith",
	)
)

// ManagedClientFactory constructs a generated Kitex client with the supplied
// guarded Suite. The factory must install it exactly once with
// kitex/client.WithSuite.
type ManagedClientFactory[T any] func(kclient.Suite) (T, error)

// NewManagedClient constructs a generated Kitex client and audits the final
// Kitex option state during that same construction.
//
// The guarded Suite must be installed exactly once. Native retry, fallback,
// circuit breaker, RPC timeout and xDS are rejected. When a Keelith Picker is
// configured, native load balancing and forward proxying are rejected too.
// Static WithHostPorts and Resolver options remain valid bootstrap fallbacks
// because a picked invocation writes its final Instance directly to RPCInfo.
func NewManagedClient[T any](
	suite *ClientSuite,
	factory ManagedClientFactory[T],
) (T, error) {
	var zero T
	if suite == nil {
		return zero, fmt.Errorf("%w: client suite is nil", ErrInvalidOption)
	}
	if factory == nil {
		return zero, fmt.Errorf("%w: client factory is nil", ErrInvalidOption)
	}

	audit := &clientOptionAudit{}
	guarded := managedClientSuite{
		base:  suite,
		audit: audit,
	}
	client, err := factory(guarded)
	if err != nil {
		return zero, err
	}
	if auditErr := audit.result(); auditErr != nil {
		if closer, ok := any(client).(interface{ Close() error }); ok {
			auditErr = errors.Join(auditErr, closer.Close())
		}
		return zero, auditErr
	}
	return client, nil
}

type managedClientSuite struct {
	base  *ClientSuite
	audit *clientOptionAudit
}

func (suite managedClientSuite) Options() []kclient.Option {
	base := suite.base.Options()
	result := make([]kclient.Option, 0, len(base)+1)
	result = append(result, base...)
	result = append(
		result,
		kclient.TailOption(kclient.Option{
			F: func(options *kclient.Options, _ *utils.Slice) {
				suite.audit.inspect(options, suite.base.picker != nil)
			},
		}),
	)
	return result
}

type clientOptionAudit struct {
	mu         sync.Mutex
	applyCount int
	conflicts  []string
}

func (audit *clientOptionAudit) inspect(
	options *kclient.Options,
	pickerEnabled bool,
) {
	conflicts := nativeGovernanceConflicts(options, pickerEnabled)
	audit.mu.Lock()
	audit.applyCount++
	audit.conflicts = append(audit.conflicts, conflicts...)
	audit.mu.Unlock()
}

func (audit *clientOptionAudit) result() error {
	audit.mu.Lock()
	defer audit.mu.Unlock()
	switch {
	case audit.applyCount == 0:
		return ErrClientSuiteNotApplied
	case audit.applyCount > 1:
		return ErrClientSuiteAppliedMoreThanOnce
	}
	if len(audit.conflicts) == 0 {
		return nil
	}
	conflicts := append([]string(nil), audit.conflicts...)
	sort.Strings(conflicts)
	conflicts = compactStrings(conflicts)
	return fmt.Errorf(
		"%w: %s",
		ErrNativeGovernanceConflict,
		strings.Join(conflicts, ", "),
	)
}

func nativeGovernanceConflicts(
	options *kclient.Options,
	pickerEnabled bool,
) []string {
	if options == nil {
		return []string{"missing-options"}
	}
	var conflicts []string
	for _, policy := range options.UnaryOptions.RetryMethodPolicies {
		if policy.Enable {
			conflicts = append(conflicts, "retry-policy")
			break
		}
	}
	if options.UnaryOptions.RetryContainer != nil {
		conflicts = append(conflicts, "retry-container")
	}
	if options.UnaryOptions.RetryWithResult != nil {
		conflicts = append(conflicts, "result-retry")
	}
	if options.UnaryOptions.Fallback != nil {
		conflicts = append(conflicts, "fallback")
	}
	if options.CBSuite != nil {
		conflicts = append(conflicts, "circuit-breaker")
	}
	if options.Timeouts != nil ||
		options.Locks.Bits&rpcinfo.BitRPCTimeout != 0 {
		conflicts = append(conflicts, "rpc-timeout")
	}
	if options.XDSEnabled {
		conflicts = append(conflicts, "xds")
	}
	if pickerEnabled {
		if options.Balancer != nil {
			conflicts = append(conflicts, "load-balancer")
		}
		if options.Proxy != nil {
			conflicts = append(conflicts, "forward-proxy")
		}
	}
	return conflicts
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	write := 1
	for read := 1; read < len(values); read++ {
		if values[read] == values[write-1] {
			continue
		}
		values[write] = values[read]
		write++
	}
	return values[:write]
}
