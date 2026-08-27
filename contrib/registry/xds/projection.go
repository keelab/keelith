package xds

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/keelab/keelith/governance/admission"
	"github.com/keelab/keelith/registry"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type projectedResponse struct {
	snapshot   registry.Snapshot
	dropPolicy admission.Policy
	staleAfter time.Duration
}

const (
	metadataRegion         = "cloud.region"
	metadataZone           = "cloud.availability_zone"
	metadataSubZone        = "xds.sub_zone"
	metadataPriority       = "xds.priority"
	metadataEndpointWeight = "xds.endpoint_weight"
	metadataLocalityWeight = "xds.locality_weight"
	// MetadataEffectiveCapacity is the normalized eds traffic share consumed by
	// Keelith capacity-aware selectors.
	MetadataEffectiveCapacity = "xds.effective_capacity"
	maxPriority               = 7
	maxLocalityLength         = 256
	maxXDSWeightSum           = uint64(1<<32 - 1)
	minimumEndpointStaleAfter = 100 * time.Millisecond
	maximumEndpointStaleAfter = 30 * 24 * time.Hour
)

func (client *Client) projectResponse(
	response *discoveryv3.DiscoveryResponse,
	resource Resource,
) (projectedResponse, error) {
	if response == nil {
		return projectedResponse{}, fmt.Errorf(
			"%w: response is nil",
			ErrInvalidResponse,
		)
	}
	if proto.Size(response) > client.maxResponseBytes {
		return projectedResponse{}, fmt.Errorf(
			"%w: response exceeds configured budget",
			ErrInvalidResponse,
		)
	}
	if response.GetTypeUrl() != EndpointTypeurl ||
		response.GetCanary() ||
		len(response.GetResourceErrors()) != 0 {
		return projectedResponse{}, fmt.Errorf(
			"%w: unsupported response envelope",
			ErrInvalidResponse,
		)
	}
	if len(response.GetResources()) > 1 {
		return projectedResponse{}, fmt.Errorf(
			"%w: response must contain at most one resource",
			ErrInvalidResponse,
		)
	}
	if len(response.GetResources()) == 0 {
		snapshot, err := client.newSnapshot(
			resource,
			response.GetVersionInfo(),
			nil,
		)
		return projectedResponse{snapshot: snapshot}, err
	}

	encoded := response.GetResources()[0]
	if encoded == nil || encoded.GetTypeUrl() != EndpointTypeurl {
		return projectedResponse{}, fmt.Errorf(
			"%w: resource type is invalid",
			ErrInvalidResponse,
		)
	}
	assignment := new(endpointv3.ClusterLoadAssignment)
	if err := encoded.UnmarshalTo(assignment); err != nil {
		return projectedResponse{}, fmt.Errorf(
			"%w: decode assignment: %w",
			ErrInvalidResponse,
			err,
		)
	}
	if assignment.GetClusterName() != resource.Cluster {
		return projectedResponse{}, fmt.Errorf(
			"%w: assignment cluster does not match subscription",
			ErrInvalidResponse,
		)
	}
	dropPolicy, err := client.projectDropPolicy(assignment.GetPolicy())
	if err != nil {
		return projectedResponse{}, err
	}
	staleAfter, err := projectEndpointStaleAfter(assignment.GetPolicy())
	if err != nil {
		return projectedResponse{}, err
	}
	instances, err := client.projectAssignment(assignment, resource)
	if err != nil {
		return projectedResponse{}, err
	}
	snapshot, err := client.newSnapshot(
		resource,
		response.GetVersionInfo(),
		instances,
	)
	if err != nil {
		return projectedResponse{}, err
	}
	return projectedResponse{
		snapshot:   snapshot,
		dropPolicy: dropPolicy,
		staleAfter: staleAfter,
	}, nil
}

func (client *Client) projectAssignment(
	assignment *endpointv3.ClusterLoadAssignment,
	resource Resource,
) ([]registry.Instance, error) {
	if assignment == nil {
		return nil, fmt.Errorf("%w: assignment is nil", ErrInvalidResponse)
	}
	if len(assignment.GetNamedEndpoints()) != 0 {
		return nil, fmt.Errorf(
			"%w: named endpoints are unsupported",
			ErrInvalidResponse,
		)
	}
	if unsupportedPolicy(assignment.GetPolicy()) {
		return nil, fmt.Errorf(
			"%w: assignment policy is unsupported",
			ErrInvalidResponse,
		)
	}
	localityWeightSums, err := assignmentLocalityWeightSums(
		assignment.GetEndpoints(),
	)
	if err != nil {
		return nil, err
	}

	byPriority := make(map[uint32][]registry.Instance)
	total := 0
	for _, localityEndpoints := range assignment.GetEndpoints() {
		if localityEndpoints == nil {
			return nil, fmt.Errorf(
				"%w: locality is nil",
				ErrInvalidResponse,
			)
		}
		priority := localityEndpoints.GetPriority()
		if priority > maxPriority {
			return nil, fmt.Errorf(
				"%w: priority is outside 0..%d",
				ErrInvalidResponse,
				maxPriority,
			)
		}
		if localityEndpoints.GetMetadata() != nil ||
			localityEndpoints.GetLbConfig() != nil ||
			localityEndpoints.GetProximity() != nil {
			return nil, fmt.Errorf(
				"%w: locality extension is unsupported",
				ErrInvalidResponse,
			)
		}
		locality, err := localityMetadata(localityEndpoints.GetLocality())
		if err != nil {
			return nil, err
		}
		localityWeight := weight(localityEndpoints.GetLoadBalancingWeight())
		if localityWeight == 0 {
			continue
		}
		endpointWeightSum, err := localityEndpointWeightSum(
			localityEndpoints.GetLbEndpoints(),
		)
		if err != nil {
			return nil, err
		}
		for _, endpoint := range localityEndpoints.GetLbEndpoints() {
			total++
			if total > client.maxEndpoints {
				return nil, fmt.Errorf(
					"%w: endpoint budget exceeded",
					ErrInvalidResponse,
				)
			}
			instance, included, err := projectEndpoint(
				assignment.GetClusterName(),
				resource,
				priority,
				locality,
				localityWeight,
				localityWeightSums[priority],
				endpointWeightSum,
				endpoint,
			)
			if err != nil {
				return nil, err
			}
			if included {
				byPriority[priority] = append(byPriority[priority], instance)
			}
		}
	}

	selected := uint32(maxPriority + 1)
	for priority, instances := range byPriority {
		if len(instances) != 0 && priority < selected {
			selected = priority
		}
	}
	if selected > maxPriority {
		return nil, nil
	}
	return byPriority[selected], nil
}

func projectEndpoint(
	cluster string,
	resource Resource,
	priority uint32,
	locality map[string]string,
	localityWeight uint32,
	localityWeightSum uint64,
	endpointWeightSum uint64,
	lbEndpoint *endpointv3.LbEndpoint,
) (registry.Instance, bool, error) {
	if lbEndpoint == nil {
		return registry.Instance{}, false, fmt.Errorf(
			"%w: endpoint is nil",
			ErrInvalidResponse,
		)
	}
	health := lbEndpoint.GetHealthStatus()
	if health != corev3.HealthStatus_UNKNOWN &&
		health != corev3.HealthStatus_HEALTHY {
		return registry.Instance{}, false, nil
	}
	endpointWeight := weight(lbEndpoint.GetLoadBalancingWeight())
	if endpointWeight == 0 {
		return registry.Instance{}, false, nil
	}
	if lbEndpoint.GetMetadata() != nil {
		return registry.Instance{}, false, fmt.Errorf(
			"%w: endpoint metadata is unsupported",
			ErrInvalidResponse,
		)
	}
	endpoint := lbEndpoint.GetEndpoint()
	if endpoint == nil || lbEndpoint.GetEndpointName() != "" {
		return registry.Instance{}, false, fmt.Errorf(
			"%w: named or missing endpoint",
			ErrInvalidResponse,
		)
	}
	if endpoint.GetHealthCheckConfig() != nil ||
		endpoint.GetHostname() != "" ||
		len(endpoint.GetAdditionalAddresses()) != 0 {
		return registry.Instance{}, false, fmt.Errorf(
			"%w: endpoint extension is unsupported",
			ErrInvalidResponse,
		)
	}
	address := endpoint.GetAddress()
	socket := address.GetSocketAddress()
	if address == nil || socket == nil {
		return registry.Instance{}, false, fmt.Errorf(
			"%w: only socket addresses are supported",
			ErrInvalidResponse,
		)
	}
	if socket.GetProtocol() != corev3.SocketAddress_TCP ||
		socket.GetResolverName() != "" ||
		socket.GetNetworkNamespaceFilepath() != "" ||
		socket.GetIpv4Compat() {
		return registry.Instance{}, false, fmt.Errorf(
			"%w: socket extension is unsupported",
			ErrInvalidResponse,
		)
	}
	if _, ok := socket.GetPortSpecifier().(*corev3.SocketAddress_PortValue); !ok {
		return registry.Instance{}, false, fmt.Errorf(
			"%w: numeric port is required",
			ErrInvalidResponse,
		)
	}
	port := socket.GetPortValue()
	if port == 0 || port > 65_535 || !validHost(socket.GetAddress()) {
		return registry.Instance{}, false, fmt.Errorf(
			"%w: socket address is invalid",
			ErrInvalidResponse,
		)
	}
	endpointurl := resource.Scheme + "://" + net.JoinHostPort(
		socket.GetAddress(),
		strconv.FormatUint(uint64(port), 10),
	)
	metadata := make(map[string]string, len(locality)+4)
	for key, value := range locality {
		metadata[key] = value
	}
	metadata[metadataPriority] = strconv.FormatUint(uint64(priority), 10)
	metadata[metadataEndpointWeight] = strconv.FormatUint(
		uint64(endpointWeight),
		10,
	)
	metadata[metadataLocalityWeight] = strconv.FormatUint(
		uint64(localityWeight),
		10,
	)
	metadata[MetadataEffectiveCapacity] = effectiveCapacity(
		localityWeight,
		localityWeightSum,
		endpointWeight,
		endpointWeightSum,
	)
	instance, err := registry.NewInstance(
		instanceid(cluster, priority, endpointurl),
		resource.Service,
		[]string{endpointurl},
		metadata,
	)
	if err != nil {
		return registry.Instance{}, false, fmt.Errorf(
			"%w: project endpoint: %w",
			ErrInvalidResponse,
			err,
		)
	}
	return instance, true, nil
}

func assignmentLocalityWeightSums(
	localities []*endpointv3.LocalityLbEndpoints,
) (map[uint32]uint64, error) {
	result := make(map[uint32]uint64)
	for _, locality := range localities {
		if locality == nil {
			return nil, fmt.Errorf(
				"%w: locality is nil",
				ErrInvalidResponse,
			)
		}
		priority := locality.GetPriority()
		if priority > maxPriority {
			return nil, fmt.Errorf(
				"%w: priority is outside 0..%d",
				ErrInvalidResponse,
				maxPriority,
			)
		}
		localityWeight := weight(locality.GetLoadBalancingWeight())
		if localityWeight == 0 {
			continue
		}
		result[priority] += uint64(localityWeight)
		if result[priority] > maxXDSWeightSum {
			return nil, fmt.Errorf(
				"%w: locality weight sum exceeds uint32",
				ErrInvalidResponse,
			)
		}
	}
	return result, nil
}

func localityEndpointWeightSum(
	endpoints []*endpointv3.LbEndpoint,
) (uint64, error) {
	var result uint64
	for _, endpoint := range endpoints {
		if endpoint == nil {
			return 0, fmt.Errorf(
				"%w: endpoint is nil",
				ErrInvalidResponse,
			)
		}
		endpointWeight := weight(endpoint.GetLoadBalancingWeight())
		if endpointWeight == 0 {
			continue
		}
		result += uint64(endpointWeight)
		if result > maxXDSWeightSum {
			return 0, fmt.Errorf(
				"%w: endpoint weight sum exceeds uint32",
				ErrInvalidResponse,
			)
		}
	}
	return result, nil
}

func effectiveCapacity(
	localityWeight uint32,
	localityWeightSum uint64,
	endpointWeight uint32,
	endpointWeightSum uint64,
) string {
	capacity := (float64(localityWeight) / float64(localityWeightSum)) *
		(float64(endpointWeight) / float64(endpointWeightSum))
	return strconv.FormatFloat(capacity, 'g', -1, 64)
}

func localityMetadata(locality *corev3.Locality) (map[string]string, error) {
	if locality == nil {
		return nil, nil
	}
	values := []struct {
		key   string
		value string
	}{
		{key: metadataRegion, value: locality.GetRegion()},
		{key: metadataZone, value: locality.GetZone()},
		{key: metadataSubZone, value: locality.GetSubZone()},
	}
	metadata := make(map[string]string, len(values))
	for _, item := range values {
		if item.value == "" {
			continue
		}
		if !validLocality(item.value) {
			return nil, fmt.Errorf(
				"%w: locality value is invalid",
				ErrInvalidResponse,
			)
		}
		metadata[item.key] = item.value
	}
	return metadata, nil
}

func unsupportedPolicy(policy *endpointv3.ClusterLoadAssignment_Policy) bool {
	return policy != nil &&
		(policy.GetOverprovisioningFactor() != nil ||
			policy.GetWeightedPriorityHealth())
}

func projectEndpointStaleAfter(
	policy *endpointv3.ClusterLoadAssignment_Policy,
) (time.Duration, error) {
	if policy == nil || policy.GetEndpointStaleAfter() == nil {
		return 0, nil
	}
	value := policy.GetEndpointStaleAfter()
	if err := value.CheckValid(); err != nil {
		return 0, fmt.Errorf(
			"%w: endpoint stale duration is invalid",
			ErrInvalidResponse,
		)
	}
	duration := value.AsDuration()
	if duration == 0 {
		return 0, nil
	}
	if duration < minimumEndpointStaleAfter ||
		duration > maximumEndpointStaleAfter {
		return 0, fmt.Errorf(
			"%w: endpoint stale duration is outside %s..%s",
			ErrInvalidResponse,
			minimumEndpointStaleAfter,
			maximumEndpointStaleAfter,
		)
	}
	return duration, nil
}

func (client *Client) projectDropPolicy(
	policy *endpointv3.ClusterLoadAssignment_Policy,
) (admission.Policy, error) {
	if policy == nil || len(policy.GetDropOverloads()) == 0 {
		return admission.Policy{}, nil
	}
	if client.admission == nil {
		return admission.Policy{}, fmt.Errorf(
			"%w: drop overload requires an admission sink",
			ErrInvalidResponse,
		)
	}
	projected := admission.Policy{
		Categories: make(
			[]admission.Category,
			0,
			len(policy.GetDropOverloads()),
		),
	}
	for _, overload := range policy.GetDropOverloads() {
		if overload == nil || overload.GetDropPercentage() == nil {
			return admission.Policy{}, fmt.Errorf(
				"%w: drop overload is incomplete",
				ErrInvalidResponse,
			)
		}
		percentage := overload.GetDropPercentage()
		denominator, ok := fractionalDenominator(
			percentage.GetDenominator(),
		)
		if !ok {
			return admission.Policy{}, fmt.Errorf(
				"%w: drop percentage denominator is invalid",
				ErrInvalidResponse,
			)
		}
		numerator := percentage.GetNumerator()
		if numerator > denominator {
			numerator = denominator
		}
		projected.Categories = append(
			projected.Categories,
			admission.Category{
				Name:        overload.GetCategory(),
				Numerator:   numerator,
				Denominator: denominator,
			},
		)
	}
	if err := admission.Validate(projected); err != nil {
		return admission.Policy{}, fmt.Errorf(
			"%w: drop overload: %w",
			ErrInvalidResponse,
			err,
		)
	}
	return projected, nil
}

func fractionalDenominator(
	value typev3.FractionalPercent_DenominatorType,
) (uint32, bool) {
	switch value {
	case typev3.FractionalPercent_HUNDRED:
		return 100, true
	case typev3.FractionalPercent_TEN_THOUSAND:
		return 10_000, true
	case typev3.FractionalPercent_MILLION:
		return 1_000_000, true
	default:
		return 0, false
	}
}

func weight(value *wrapperspb.UInt32Value) uint32 {
	if value == nil {
		return 1
	}
	return value.GetValue()
}

func validLocality(value string) bool {
	if strings.TrimSpace(value) != value ||
		len(value) > maxLocalityLength ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validHost(value string) bool {
	if value == "" ||
		strings.TrimSpace(value) != value ||
		len(value) > maxIdentityLength ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) ||
			character == '/' ||
			character == '?' ||
			character == '#' {
			return false
		}
	}
	return true
}

func instanceid(cluster string, priority uint32, endpoint string) string {
	digest := sha256.New()
	writeDigestString(digest, cluster)
	writeDigestString(digest, strconv.FormatUint(uint64(priority), 10))
	writeDigestString(digest, endpoint)
	return "xds-" + hex.EncodeToString(digest.Sum(nil)[:16])
}

func (client *Client) newSnapshot(
	resource Resource,
	version string,
	instances []registry.Instance,
) (registry.Snapshot, error) {
	revision := snapshotRevision(
		version,
		resource.Service,
		resource.Cluster,
		instances,
	)
	snapshot, err := registry.NewSnapshot(
		resource.Service,
		revision,
		instances,
	)
	if err != nil {
		return registry.Snapshot{}, fmt.Errorf(
			"%w: build snapshot: %w",
			ErrInvalidResponse,
			err,
		)
	}
	return snapshot, nil
}

func snapshotRevision(
	version string,
	service string,
	cluster string,
	instances []registry.Instance,
) string {
	digest := sha256.New()
	writeDigestString(digest, version)
	writeDigestString(digest, service)
	writeDigestString(digest, cluster)
	ordered := append([]registry.Instance(nil), instances...)
	sort.Slice(ordered, func(first, second int) bool {
		return ordered[first].ID() < ordered[second].ID()
	})
	for _, instance := range ordered {
		writeDigestString(digest, instance.ID())
		for _, endpoint := range instance.Endpoints() {
			writeDigestString(digest, endpoint)
		}
		metadata := instance.Metadata()
		keys := make([]string, 0, len(metadata))
		for key := range metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			writeDigestString(digest, key)
			writeDigestString(digest, metadata[key])
		}
	}
	return "xds:" + hex.EncodeToString(digest.Sum(nil)[:16])
}

func writeDigestString(digest hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write([]byte(value))
}
