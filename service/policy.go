package service

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/keelab/keelith/middleware"
)

const (
	maxCapabilities       = 32
	maxCapabilityNameSize = 128
)

// Capability is a stable, low-cardinality policy capability used for startup
// auditing. It never enables behavior by its name alone.
type Capability string

const (
	// CapabilityAuthentication declares identity authentication middleware.
	CapabilityAuthentication Capability = "authentication"
	// CapabilityAuthorization declares authorization middleware.
	CapabilityAuthorization Capability = "authorization"
	// CapabilityRequestID declares request correlation middleware.
	CapabilityRequestID Capability = "request-id"
)

// Policy associates one real middleware Bundle with the capabilities that it
// provides. The Bundle remains the only executable behavior.
type Policy struct {
	bundle       *middleware.Bundle
	capabilities []Capability
	err          error
}

// NewPolicy validates and snapshots a Bundle capability declaration.
func NewPolicy(bundle *middleware.Bundle, capabilities ...Capability) Policy {
	policy := Policy{bundle: bundle}
	if bundle == nil {
		policy.err = errors.New("middleware bundle is nil")
		return policy
	}
	policy.capabilities, policy.err = normalizeCapabilities(capabilities)
	return policy
}

func normalizeCapabilities(values []Capability) ([]Capability, error) {
	if len(values) == 0 || len(values) > maxCapabilities {
		return nil, fmt.Errorf("capability count must be between 1 and %d", maxCapabilities)
	}
	result := append([]Capability(nil), values...)
	for _, value := range result {
		if !validCapability(value) {
			return nil, fmt.Errorf("capability %q is malformed", value)
		}
	}
	sort.Slice(result, func(first, second int) bool { return result[first] < result[second] })
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return nil, fmt.Errorf("capability %q is duplicated", result[index])
		}
	}
	return result, nil
}

func validCapability(value Capability) bool {
	text := string(value)
	if text == "" || len(text) > maxCapabilityNameSize || strings.TrimSpace(text) != text {
		return false
	}
	for _, r := range text {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
