// Package kubernetes renders one immutable topology epoch as Kubernetes
// ConfigMap, Deployment, and optional Service resources.
package kubernetes

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/programmable/topology"
	topologycontrol "github.com/keelab/keelith/programmable/topology/control"
	"github.com/keelab/keelith/programmable/topology/planfile"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"
)

const (
	// PlanFileName is the ConfigMap key containing the canonical plan.
	PlanFileName = "plan.json"
	// PlanMountPath is the in-container canonical plan path.
	PlanMountPath = "/etc/keelith/topology/plan.json"
	// ControlFileName is the ConfigMap key containing a complete candidate.
	ControlFileName = "candidate.json"
	// ControlMountPath is the in-container revisioned candidate path.
	ControlMountPath = "/etc/keelith/topology/candidate.json"
	// PlanHashAnnotation triggers a rollout when plan placement changes.
	PlanHashAnnotation = "keelith.dev/topology-plan-hash"
	// PlanEpochAnnotation exposes the immutable process epoch.
	PlanEpochAnnotation = "keelith.dev/topology-epoch"
	// PlanEpochLabel identifies the immutable epoch carried by one Pod without
	// making Deployment or Service selectors change across rollouts.
	PlanEpochLabel = "keelith.dev/topology-epoch"
	// PlacementLabel identifies one process placement.
	PlacementLabel = "keelith.dev/placement"
	// ClusterLabel is the platform-provided node cluster identity.
	ClusterLabel = "topology.keelith.dev/cluster"
	// RegionLabel is the Kubernetes standard node region identity.
	RegionLabel = "topology.kubernetes.io/region"
	// ZoneLabel is the Kubernetes standard node zone identity.
	ZoneLabel = "topology.kubernetes.io/zone"
	// StartupPath is the Keelith Ops startup probe contract.
	StartupPath = "/health/startup"
	// ReadinessPath is the Keelith Ops readiness probe contract.
	ReadinessPath = "/health/ready"
	// LivenessPath is the Keelith Ops liveness probe contract.
	LivenessPath = "/health/live"

	defaultNamespace            = "default"
	defaultPlanName             = "keelith-topology"
	defaultTerminationSeconds   = int64(30)
	defaultEndpointDrainSeconds = int64(5)
	defaultMinimumReadySeconds  = int32(5)
	maximumWorkloads            = 1024
	maximumReachabilityRoutes   = 4096
	maximumControlDocumentBytes = 2 * 1024 * 1024
)

var (
	// ErrInvalidConfig reports an invalid plan or incomplete workload mapping.
	ErrInvalidConfig = errors.New(
		"topology kubernetes: invalid render config",
	)
	// ErrUnreachableRoute reports a cross-cluster dependency without a
	// platform-declared directed route.
	ErrUnreachableRoute = errors.New(
		"topology kubernetes: unreachable remote route",
	)
)

// PlacementConstraint pins one placement to platform node locality labels.
// Region requires Cluster, and Zone requires both Region and Cluster.
type PlacementConstraint struct {
	Cluster string
	Region  string
	Zone    string
}

// ReachabilityRoute declares platform-provided, directed cross-cluster
// connectivity. Keelith validates it but never proxies the business traffic.
type ReachabilityRoute struct {
	FromCluster string
	ToCluster   string
}

// ControlConfig embeds one complete revisioned candidate and its immutable
// process-side verification policy.
type ControlConfig struct {
	Document      []byte
	PublicKey     string
	AllowUnsigned bool
}

// Workload maps exactly one topology placement to one process Deployment.
type Workload struct {
	Placement     topology.PlacementID
	Name          string
	Image         string
	Replicas      int32
	ContainerPort int32
	// HealthPort serves the Keelith Ops health contract. Zero reuses
	// ContainerPort.
	HealthPort int32
	Expose     bool
	Constraint PlacementConstraint
}

// Config controls deterministic resources for one complete topology epoch.
type Config struct {
	Namespace               string
	PlanName                string
	Plan                    topology.Plan
	Workloads               []Workload
	Routes                  []ReachabilityRoute
	Control                 *ControlConfig
	TerminationGraceSeconds int64
	// EndpointDrainSeconds is the preStop propagation window after Kubernetes
	// removes a terminating Pod from Ready endpoints and before SIGTERM.
	EndpointDrainSeconds int64
	// DrainTimeoutSeconds is exported to the process as its maximum graceful
	// shutdown budget. It plus EndpointDrainSeconds cannot exceed termination
	// grace.
	DrainTimeoutSeconds int64
	// MinimumReadySeconds requires a successor to remain Ready before the
	// Deployment controller starts terminating the previous epoch.
	MinimumReadySeconds int32
}

// Render validates the complete placement mapping and emits deterministic YAML.
func Render(config Config) ([]byte, error) {
	snapshot, err := topology.Activate(config.Plan)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	planPayload, err := planfile.Marshal(config.Plan)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	controlPolicy, err := validateControl(config.Control, snapshot)
	if err != nil {
		return nil, err
	}
	namespace := strings.TrimSpace(config.Namespace)
	if namespace == "" {
		namespace = defaultNamespace
	}
	planName := strings.TrimSpace(config.PlanName)
	if planName == "" {
		planName = defaultPlanName
	}
	grace, endpointDrain, drainTimeout, minimumReady, validLifecycle :=
		resolveLifecycle(config)
	if len(validation.IsDNS1123Label(namespace)) > 0 ||
		len(validation.IsDNS1123Subdomain(planName)) > 0 ||
		!validLifecycle ||
		len(config.Workloads) == 0 ||
		len(config.Workloads) > maximumWorkloads ||
		len(config.Workloads) != len(config.Plan.Placements) {
		return nil, ErrInvalidConfig
	}
	workloads := append([]Workload(nil), config.Workloads...)
	sort.Slice(workloads, func(left, right int) bool {
		return workloads[left].Placement < workloads[right].Placement
	})
	seenPlacements := make(map[topology.PlacementID]struct{}, len(workloads))
	seenNames := make(map[string]struct{}, len(workloads))
	for _, workload := range workloads {
		if _, exists := config.Plan.Placements[workload.Placement]; !exists ||
			!validWorkload(workload) {
			return nil, ErrInvalidConfig
		}
		if _, duplicate := seenPlacements[workload.Placement]; duplicate {
			return nil, ErrInvalidConfig
		}
		if _, duplicate := seenNames[workload.Name]; duplicate {
			return nil, ErrInvalidConfig
		}
		seenPlacements[workload.Placement] = struct{}{}
		seenNames[workload.Name] = struct{}{}
	}
	for placement := range config.Plan.Placements {
		if _, exists := seenPlacements[placement]; !exists {
			return nil, ErrInvalidConfig
		}
	}
	if err := validateReachability(config.Plan, workloads, config.Routes); err != nil {
		return nil, err
	}

	epoch := strconv.FormatUint(snapshot.Epoch(), 10)
	annotations := map[string]string{
		PlanEpochAnnotation: epoch,
		PlanHashAnnotation:  snapshot.Hash(),
	}
	configMap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        planName,
			Namespace:   namespace,
			Annotations: cloneStrings(annotations),
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "keelith",
			},
		},
		Data: map[string]string{
			PlanFileName: string(planPayload),
		},
	}
	if controlPolicy.enabled {
		configMap.Data[ControlFileName] = string(controlPolicy.document)
	}
	resources := []any{configMap}
	for _, workload := range workloads {
		resources = append(
			resources,
			deployment(
				namespace,
				planName,
				workload,
				annotations,
				epoch,
				snapshot.Hash(),
				grace,
				endpointDrain,
				drainTimeout,
				minimumReady,
				controlPolicy,
			),
		)
		if workload.Expose {
			resources = append(
				resources,
				service(namespace, workload, annotations),
			)
		}
	}
	return marshalResources(resources)
}

func deployment(
	namespace string,
	planName string,
	workload Workload,
	annotations map[string]string,
	epoch string,
	planHash string,
	grace int64,
	endpointDrain int64,
	drainTimeout int64,
	minimumReady int32,
	control controlPolicy,
) *appsv1.Deployment {
	labels := map[string]string{
		"app.kubernetes.io/name":       workload.Name,
		"app.kubernetes.io/managed-by": "keelith",
		PlacementLabel:                 string(workload.Placement),
	}
	replicas := workload.Replicas
	maxUnavailable := intstr.FromInt32(0)
	maxSurge := intstr.FromInt32(1)
	defaultMode := int32(0o444)
	podLabels := cloneStrings(labels)
	podLabels[PlanEpochLabel] = epoch
	if workload.Constraint.Cluster != "" {
		podLabels[ClusterLabel] = workload.Constraint.Cluster
	}
	if workload.Constraint.Region != "" {
		podLabels[RegionLabel] = workload.Constraint.Region
	}
	if workload.Constraint.Zone != "" {
		podLabels[ZoneLabel] = workload.Constraint.Zone
	}
	healthPort := workload.HealthPort
	if healthPort == 0 {
		healthPort = workload.ContainerPort
	}
	containerPorts := []corev1.ContainerPort{{
		Name:          "http",
		ContainerPort: workload.ContainerPort,
		Protocol:      corev1.ProtocolTCP,
	}}
	if healthPort != workload.ContainerPort {
		containerPorts = append(containerPorts, corev1.ContainerPort{
			Name:          "ops",
			ContainerPort: healthPort,
			Protocol:      corev1.ProtocolTCP,
		})
	}
	environment := []corev1.EnvVar{
		{Name: "KEELITH_TOPOLOGY_EPOCH", Value: epoch},
		{Name: "KEELITH_TOPOLOGY_PLAN_HASH", Value: planHash},
		{Name: "KEELITH_PLACEMENT", Value: string(workload.Placement)},
		{Name: "KEELITH_TOPOLOGY_PLAN", Value: PlanMountPath},
		{
			Name:  "KEELITH_ENDPOINT_DRAIN_DELAY",
			Value: strconv.FormatInt(endpointDrain, 10) + "s",
		},
		{
			Name:  "KEELITH_DRAIN_TIMEOUT",
			Value: strconv.FormatInt(drainTimeout, 10) + "s",
		},
	}
	if workload.Constraint.Cluster != "" {
		environment = append(environment, corev1.EnvVar{
			Name: "KEELITH_CLUSTER", Value: workload.Constraint.Cluster,
		})
	}
	if workload.Constraint.Region != "" {
		environment = append(environment, corev1.EnvVar{
			Name: "KEELITH_REGION", Value: workload.Constraint.Region,
		})
	}
	if workload.Constraint.Zone != "" {
		environment = append(environment, corev1.EnvVar{
			Name: "KEELITH_ZONE", Value: workload.Constraint.Zone,
		})
	}
	if control.enabled {
		environment = append(environment, corev1.EnvVar{
			Name: "KEELITH_TOPOLOGY_CONTROL_FILE", Value: ControlMountPath,
		})
		if control.allowUnsigned {
			environment = append(environment, corev1.EnvVar{
				Name: "KEELITH_TOPOLOGY_ALLOW_UNSIGNED", Value: "true",
			})
		} else {
			environment = append(environment, corev1.EnvVar{
				Name: "KEELITH_TOPOLOGY_CONTROL_PUBLIC_KEY", Value: control.publicKey,
			})
		}
	}
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        workload.Name,
			Namespace:   namespace,
			Labels:      cloneStrings(labels),
			Annotations: cloneStrings(annotations),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas:        &replicas,
			MinReadySeconds: minimumReady,
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: &maxUnavailable,
					MaxSurge:       &maxSurge,
				},
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: cloneStrings(labels),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: cloneStrings(annotations),
				},
				Spec: corev1.PodSpec{
					Affinity:                      placementAffinity(workload.Constraint),
					TerminationGracePeriodSeconds: &grace,
					Containers: []corev1.Container{{
						Name:            workload.Name,
						Image:           workload.Image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Ports:           containerPorts,
						Env:             environment,
						Lifecycle: &corev1.Lifecycle{
							PreStop: &corev1.LifecycleHandler{
								Sleep: &corev1.SleepAction{
									Seconds: endpointDrain,
								},
							},
						},
						StartupProbe: httpProbe(
							StartupPath,
							healthPort,
							2,
							30,
						),
						ReadinessProbe: httpProbe(
							ReadinessPath,
							healthPort,
							2,
							1,
						),
						LivenessProbe: httpProbe(
							LivenessPath,
							healthPort,
							10,
							3,
						),
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "keelith-topology",
							MountPath: "/etc/keelith/topology",
							ReadOnly:  true,
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "keelith-topology",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: planName,
								},
								DefaultMode: &defaultMode,
							},
						},
					}},
				},
			},
		},
	}
}

func service(
	namespace string,
	workload Workload,
	annotations map[string]string,
) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        workload.Name,
			Namespace:   namespace,
			Annotations: cloneStrings(annotations),
			Labels: map[string]string{
				"app.kubernetes.io/name":       workload.Name,
				"app.kubernetes.io/managed-by": "keelith",
				PlacementLabel:                 string(workload.Placement),
			},
		},
		Spec: corev1.ServiceSpec{
			PublishNotReadyAddresses: false,
			Selector: map[string]string{
				"app.kubernetes.io/name": workload.Name,
				PlacementLabel:           string(workload.Placement),
			},
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       workload.ContainerPort,
				TargetPort: intstr.FromString("http"),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

func httpProbe(
	path string,
	port int32,
	periodSeconds int32,
	failureThreshold int32,
) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: path,
				Port: intstr.FromInt32(port),
			},
		},
		PeriodSeconds:    periodSeconds,
		TimeoutSeconds:   2,
		FailureThreshold: failureThreshold,
	}
}

func validWorkload(workload Workload) bool {
	if workload.Placement == "" ||
		len(validation.IsDNS1123Subdomain(workload.Name)) > 0 ||
		workload.Replicas < 1 ||
		workload.Replicas > 1000 ||
		workload.ContainerPort < 1 ||
		workload.ContainerPort > 65535 ||
		workload.HealthPort < 0 ||
		workload.HealthPort > 65535 ||
		!validConstraint(workload.Constraint) ||
		!validImage(workload.Image) {
		return false
	}
	return len(validation.IsValidLabelValue(
		string(workload.Placement),
	)) == 0
}

type controlPolicy struct {
	document      []byte
	publicKey     string
	allowUnsigned bool
	enabled       bool
}

func validateControl(
	config *ControlConfig,
	snapshot topology.Snapshot,
) (controlPolicy, error) {
	if config == nil {
		return controlPolicy{}, nil
	}
	if len(config.Document) == 0 ||
		len(config.Document) > maximumControlDocumentBytes ||
		strings.TrimSpace(config.PublicKey) != config.PublicKey {
		return controlPolicy{}, ErrInvalidConfig
	}
	candidate, err := topologycontrol.ParseDocument(config.Document)
	if err != nil || candidate.Epoch() != snapshot.Epoch() ||
		candidate.Hash() != snapshot.Hash() {
		return controlPolicy{}, ErrInvalidConfig
	}
	policy := controlPolicy{
		document:  append([]byte(nil), config.Document...),
		publicKey: config.PublicKey, allowUnsigned: config.AllowUnsigned,
		enabled: true,
	}
	if config.AllowUnsigned {
		if config.PublicKey != "" || len(candidate.Signature()) != 0 {
			return controlPolicy{}, ErrInvalidConfig
		}
		return policy, nil
	}
	publicKey, err := base64.StdEncoding.Strict().DecodeString(config.PublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return controlPolicy{}, ErrInvalidConfig
	}
	verifier, err := topologycontrol.NewEd25519Verifier(
		ed25519.PublicKey(publicKey),
	)
	if err != nil || verifier.Verify(context.Background(), candidate) != nil {
		return controlPolicy{}, ErrInvalidConfig
	}
	return policy, nil
}

func validConstraint(constraint PlacementConstraint) bool {
	if constraint.Zone != "" && constraint.Region == "" ||
		constraint.Region != "" && constraint.Cluster == "" {
		return false
	}
	for _, value := range []string{
		constraint.Cluster, constraint.Region, constraint.Zone,
	} {
		if value != "" && len(validation.IsValidLabelValue(value)) != 0 {
			return false
		}
	}
	return true
}

func placementAffinity(constraint PlacementConstraint) *corev1.Affinity {
	expressions := make([]corev1.NodeSelectorRequirement, 0, 3)
	for _, item := range []struct {
		key   string
		value string
	}{
		{key: ClusterLabel, value: constraint.Cluster},
		{key: RegionLabel, value: constraint.Region},
		{key: ZoneLabel, value: constraint.Zone},
	} {
		if item.value == "" {
			continue
		}
		expressions = append(expressions, corev1.NodeSelectorRequirement{
			Key: item.key, Operator: corev1.NodeSelectorOpIn,
			Values: []string{item.value},
		})
	}
	if len(expressions) == 0 {
		return nil
	}
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: expressions,
				}},
			},
		},
	}
}

func validateReachability(
	plan topology.Plan,
	workloads []Workload,
	routes []ReachabilityRoute,
) error {
	if len(routes) > maximumReachabilityRoutes {
		return ErrInvalidConfig
	}
	constraints := make(
		map[topology.PlacementID]PlacementConstraint,
		len(workloads),
	)
	for _, workload := range workloads {
		constraints[workload.Placement] = workload.Constraint
	}
	type routeKey struct{ from, to string }
	reachable := make(map[routeKey]struct{}, len(routes))
	for _, route := range routes {
		if route.FromCluster == "" || route.ToCluster == "" ||
			route.FromCluster == route.ToCluster ||
			len(validation.IsValidLabelValue(route.FromCluster)) != 0 ||
			len(validation.IsValidLabelValue(route.ToCluster)) != 0 {
			return ErrInvalidConfig
		}
		key := routeKey{from: route.FromCluster, to: route.ToCluster}
		if _, duplicate := reachable[key]; duplicate {
			return ErrInvalidConfig
		}
		reachable[key] = struct{}{}
	}
	for source, targets := range plan.Dependencies {
		sourcePlacement := plan.Components[source]
		for target := range targets {
			targetPlacement := plan.Components[target]
			from := constraints[sourcePlacement].Cluster
			to := constraints[targetPlacement].Cluster
			if from == to {
				continue
			}
			if from == "" || to == "" {
				return ErrUnreachableRoute
			}
			if _, exists := reachable[routeKey{from: from, to: to}]; !exists {
				return ErrUnreachableRoute
			}
		}
	}
	return nil
}

func resolveLifecycle(config Config) (
	grace int64,
	endpointDrain int64,
	drainTimeout int64,
	minimumReady int32,
	valid bool,
) {
	grace = config.TerminationGraceSeconds
	if grace == 0 {
		grace = defaultTerminationSeconds
	}
	endpointDrain = config.EndpointDrainSeconds
	if endpointDrain == 0 {
		endpointDrain = defaultEndpointDrainSeconds
	}
	drainTimeout = config.DrainTimeoutSeconds
	if drainTimeout == 0 {
		drainTimeout = grace - endpointDrain
	}
	minimumReady = config.MinimumReadySeconds
	if minimumReady == 0 {
		minimumReady = defaultMinimumReadySeconds
	}
	valid = grace >= 1 &&
		grace <= 3600 &&
		endpointDrain >= 1 &&
		drainTimeout >= 1 &&
		endpointDrain+drainTimeout <= grace &&
		minimumReady >= 1 &&
		minimumReady <= 3600
	return grace, endpointDrain, drainTimeout, minimumReady, valid
}

func validImage(image string) bool {
	if image == "" ||
		len(image) > 2048 ||
		!utf8.ValidString(image) ||
		strings.TrimSpace(image) != image {
		return false
	}
	for _, character := range image {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return false
		}
	}
	return true
}

func marshalResources(resources []any) ([]byte, error) {
	var builder strings.Builder
	for index, resource := range resources {
		payload, err := yaml.Marshal(resource)
		if err != nil {
			return nil, fmt.Errorf("%w: marshal resource", ErrInvalidConfig)
		}
		if index > 0 {
			builder.WriteString("---\n")
		}
		builder.Write(payload)
	}
	return []byte(builder.String()), nil
}

func cloneStrings(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
