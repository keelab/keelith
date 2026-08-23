// Package v1alpha1 defines the namespaced TopologyRevision Kubernetes API.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// GroupName is the Kubernetes API group for topology control resources.
	GroupName = "topology.keelith.dev"
	// Version is the served TopologyRevision API version.
	Version = "v1alpha1"
	// Kind is the singular TopologyRevision kind.
	Kind = "TopologyRevision"
	// Resource is the plural TopologyRevision resource.
	Resource = "topologyrevisions"
)

var (
	// GroupVersion identifies this API package.
	GroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}
	// GroupVersionResource identifies the namespaced dynamic-client resource.
	GroupVersionResource = GroupVersion.WithResource(Resource)
	// SchemeBuilder registers TopologyRevision runtime objects.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme adds this API to a Kubernetes runtime Scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// TopologyRevision declares one immutable, signed topology rollout revision.
type TopologyRevision struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              TopologyRevisionSpec   `json:"spec"`
	Status            TopologyRevisionStatus `json:"status,omitempty"`
}

// TopologyRevisionList is a namespaced collection.
type TopologyRevisionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TopologyRevision `json:"items"`
}

// TopologyRevisionSpec is the desired signed plan and workload projection.
type TopologyRevisionSpec struct {
	Revision     int64                   `json:"revision"`
	PlanName     string                  `json:"planName,omitempty"`
	ExpectedHash string                  `json:"expectedHash,omitempty"`
	Signature    string                  `json:"signature,omitempty"`
	Plan         PlanSpec                `json:"plan"`
	Workloads    []WorkloadSpec          `json:"workloads"`
	Routes       []ReachabilityRouteSpec `json:"routes,omitempty"`
	Lifecycle    LifecycleSpec           `json:"lifecycle,omitempty"`
}

// PlanSpec is the strict Kubernetes form of a canonical topology plan.
type PlanSpec struct {
	APIVersion   string            `json:"apiVersion"`
	Epoch        int64             `json:"epoch"`
	Placements   []string          `json:"placements"`
	Components   []ComponentSpec   `json:"components"`
	Dependencies []DependencySpec  `json:"dependencies"`
	Traffic      []EpochWeightSpec `json:"traffic,omitempty"`
}

// ComponentSpec places one stable component identity.
type ComponentSpec struct {
	ID        string `json:"id"`
	Placement string `json:"placement"`
}

// DependencySpec declares one directed component call.
type DependencySpec struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Mode   string `json:"mode"`
}

// EpochWeightSpec assigns basis points to one Ready epoch.
type EpochWeightSpec struct {
	Epoch       int64 `json:"epoch"`
	BasisPoints int32 `json:"basisPoints"`
}

// WorkloadSpec maps one placement to one managed Deployment and Service.
type WorkloadSpec struct {
	Placement     string                  `json:"placement"`
	Name          string                  `json:"name"`
	Image         string                  `json:"image"`
	Replicas      int32                   `json:"replicas"`
	ContainerPort int32                   `json:"containerPort"`
	HealthPort    int32                   `json:"healthPort,omitempty"`
	Expose        bool                    `json:"expose,omitempty"`
	Constraint    PlacementConstraintSpec `json:"constraint,omitempty"`
}

// PlacementConstraintSpec pins one workload to cluster/region/zone labels.
type PlacementConstraintSpec struct {
	Cluster string `json:"cluster,omitempty"`
	Region  string `json:"region,omitempty"`
	Zone    string `json:"zone,omitempty"`
}

// ReachabilityRouteSpec declares directed platform cross-cluster reachability.
type ReachabilityRouteSpec struct {
	FromCluster string `json:"fromCluster"`
	ToCluster   string `json:"toCluster"`
}

// LifecycleSpec configures bounded rolling drain timings in seconds.
type LifecycleSpec struct {
	TerminationGraceSeconds int64 `json:"terminationGraceSeconds,omitempty"`
	EndpointDrainSeconds    int64 `json:"endpointDrainSeconds,omitempty"`
	DrainTimeoutSeconds     int64 `json:"drainTimeoutSeconds,omitempty"`
	MinimumReadySeconds     int32 `json:"minimumReadySeconds,omitempty"`
}

// TopologyRevisionPhase is a fixed low-cardinality reconciliation phase.
type TopologyRevisionPhase string

const (
	// PhasePending reports a valid object not yet fully applied.
	PhasePending TopologyRevisionPhase = "Pending"
	// PhaseReady reports all desired resources and status applied.
	PhaseReady TopologyRevisionPhase = "Ready"
	// PhaseRejected reports validation, identity, or reachability rejection.
	PhaseRejected TopologyRevisionPhase = "Rejected"
)

// TopologyRevisionStatus contains only bounded identities and conditions.
type TopologyRevisionStatus struct {
	ObservedGeneration int64                 `json:"observedGeneration,omitempty"`
	AppliedRevision    int64                 `json:"appliedRevision,omitempty"`
	AppliedEpoch       int64                 `json:"appliedEpoch,omitempty"`
	AppliedHash        string                `json:"appliedHash,omitempty"`
	ManagedResources   int32                 `json:"managedResources,omitempty"`
	Phase              TopologyRevisionPhase `json:"phase,omitempty"`
	Conditions         []metav1.Condition    `json:"conditions,omitempty"`
}

// DeepCopyObject implements runtime.Object.
func (revision *TopologyRevision) DeepCopyObject() runtime.Object {
	if revision == nil {
		return nil
	}
	return revision.DeepCopy()
}

// DeepCopy returns a fully independent resource.
func (revision *TopologyRevision) DeepCopy() *TopologyRevision {
	if revision == nil {
		return nil
	}
	result := new(TopologyRevision)
	revision.DeepCopyInto(result)
	return result
}

// DeepCopyInto copies this resource and every mutable child.
func (revision *TopologyRevision) DeepCopyInto(result *TopologyRevision) {
	*result = *revision
	revision.ObjectMeta.DeepCopyInto(&result.ObjectMeta)
	result.Spec.Plan.Placements = append([]string(nil), revision.Spec.Plan.Placements...)
	result.Spec.Plan.Components = append([]ComponentSpec(nil), revision.Spec.Plan.Components...)
	result.Spec.Plan.Dependencies = append([]DependencySpec(nil), revision.Spec.Plan.Dependencies...)
	result.Spec.Plan.Traffic = append([]EpochWeightSpec(nil), revision.Spec.Plan.Traffic...)
	result.Spec.Workloads = append([]WorkloadSpec(nil), revision.Spec.Workloads...)
	result.Spec.Routes = append([]ReachabilityRouteSpec(nil), revision.Spec.Routes...)
	result.Status.Conditions = append([]metav1.Condition(nil), revision.Status.Conditions...)
}

// DeepCopyObject implements runtime.Object.
func (list *TopologyRevisionList) DeepCopyObject() runtime.Object {
	if list == nil {
		return nil
	}
	result := new(TopologyRevisionList)
	*result = *list
	result.ListMeta = *list.DeepCopy()
	if list.Items != nil {
		result.Items = make([]TopologyRevision, len(list.Items))
		for index := range list.Items {
			list.Items[index].DeepCopyInto(&result.Items[index])
		}
	}
	return result
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(
		GroupVersion,
		&TopologyRevision{},
		&TopologyRevisionList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
