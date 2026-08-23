// Package operator reconciles TopologyRevision resources into ordinary
// namespace-scoped Kubernetes workloads. It never proxies business traffic.
package operator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/keelab/keelith/programmable/topology"
	"github.com/keelab/keelith/programmable/topology/control"
	"github.com/keelab/keelith/programmable/topology/planfile"
	topologyv1alpha1 "github.com/keelab/operator/api/v1alpha1"
	topologykubernetes "github.com/keelab/operator/kubernetes"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"
)

const (
	// Finalizer guarantees owned resources are removed before the revision.
	Finalizer = "topology.keelith.dev/finalizer"
	// ManagedRevisionLabel scopes cleanup to one TopologyRevision name.
	ManagedRevisionLabel = "topology.keelith.dev/revision"

	maximumRevisionDocumentBytes = 3 * 1024 * 1024
	maximumWorkloads             = 1024
	maximumRoutes                = 4096
)

var (
	// ErrInvalidConfig reports incomplete Kubernetes clients or trust policy.
	ErrInvalidConfig = errors.New("topology operator: invalid config")
	// ErrInvalidRevision reports a strict schema, budget, signature, monotonic,
	// plan, or reachability rejection. Existing resources remain last-good.
	ErrInvalidRevision = errors.New("topology operator: invalid revision")
	// ErrResourceConflict prevents adopting or overwriting another owner's
	// namespace-scoped object.
	ErrResourceConflict = errors.New("topology operator: resource conflict")
)

// Config wires one namespace-scoped reconciler to official Kubernetes clients.
type Config struct {
	Kubernetes    kubernetes.Interface
	Dynamic       dynamic.Interface
	PublicKey     ed25519.PublicKey
	AllowUnsigned bool
}

// Result is a bounded reconcile disposition.
type Result struct {
	Rejected  bool
	Finalized bool
	Resources int32
}

// Reconciler projects a complete signed revision through create-or-update and
// retains last-good resources after any candidate rejection.
type Reconciler struct {
	kubernetes    kubernetes.Interface
	dynamic       dynamic.Interface
	verifier      control.Verifier
	publicKeyText string
	allowUnsigned bool
	mu            sync.Mutex
}

// NewReconciler validates and freezes one trust and client configuration.
func NewReconciler(config Config) (*Reconciler, error) {
	if config.Kubernetes == nil || config.Dynamic == nil ||
		config.AllowUnsigned == (len(config.PublicKey) != 0) ||
		len(config.PublicKey) != 0 && len(config.PublicKey) != ed25519.PublicKeySize {
		return nil, ErrInvalidConfig
	}
	var verifier control.Verifier
	var err error
	publicKeyText := ""
	if !config.AllowUnsigned {
		verifier, err = control.NewEd25519Verifier(config.PublicKey)
		if err != nil {
			return nil, errors.Join(ErrInvalidConfig, err)
		}
		publicKeyText = base64.StdEncoding.EncodeToString(config.PublicKey)
	}
	return &Reconciler{
		kubernetes: config.Kubernetes, dynamic: config.Dynamic,
		verifier: verifier, publicKeyText: publicKeyText,
		allowUnsigned: config.AllowUnsigned,
	}, nil
}

// Reconcile fetches, strictly validates, and applies one namespaced resource.
func (reconciler *Reconciler) Reconcile(
	ctx context.Context,
	namespace string,
	name string,
) (Result, error) {
	if reconciler == nil || ctx == nil ||
		len(validation.IsDNS1123Label(namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(name)) != 0 {
		return Result{}, ErrInvalidConfig
	}
	if cause := context.Cause(ctx); cause != nil {
		return Result{}, cause
	}
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()
	resource := reconciler.dynamic.Resource(
		topologyv1alpha1.GroupVersionResource,
	).Namespace(namespace)
	object, err := resource.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return Result{}, nil
	}
	if err != nil {
		return Result{}, err
	}
	if object.GetDeletionTimestamp() != nil {
		return reconciler.finalize(ctx, resource, object)
	}
	revision, err := decodeRevision(object)
	if err != nil {
		statusErr := reconciler.reject(
			ctx, resource, object, "InvalidSchema", nil,
		)
		return Result{Rejected: true}, errors.Join(ErrInvalidRevision, err, statusErr)
	}
	candidate, renderConfig, err := reconciler.compile(ctx, revision)
	if err != nil {
		statusErr := reconciler.reject(
			ctx, resource, object, rejectionReason(err), revision,
		)
		return Result{Rejected: true}, errors.Join(ErrInvalidRevision, err, statusErr)
	}
	if revision.Status.AppliedRevision != 0 &&
		(candidate.Revision() < uint64(revision.Status.AppliedRevision) ||
			candidate.Revision() == uint64(revision.Status.AppliedRevision) &&
				candidate.Hash() != revision.Status.AppliedHash) {
		statusErr := reconciler.reject(
			ctx, resource, object, "NonMonotonic", revision,
		)
		return Result{Rejected: true}, errors.Join(ErrInvalidRevision, statusErr)
	}
	object, err = ensureFinalizer(ctx, resource, object)
	if err != nil {
		return Result{}, err
	}
	rendered, err := topologykubernetes.Render(renderConfig)
	if err != nil {
		statusErr := reconciler.reject(
			ctx, resource, object, rejectionReason(err), revision,
		)
		return Result{Rejected: true}, errors.Join(ErrInvalidRevision, err, statusErr)
	}
	resources, err := decodeResources(rendered)
	if err != nil {
		return Result{}, err
	}
	owner := ownerReference(object)
	desired, err := reconciler.applyResources(
		ctx,
		namespace,
		object.GetName(),
		owner,
		resources,
	)
	if err != nil {
		statusErr := reconciler.reject(
			ctx, resource, object, "ApplyFailed", revision,
		)
		return Result{Rejected: true}, errors.Join(err, statusErr)
	}
	if err := reconciler.deleteStale(
		ctx,
		namespace,
		object.GetName(),
		object.GetUID(),
		desired,
	); err != nil {
		statusErr := reconciler.reject(
			ctx, resource, object, "CleanupFailed", revision,
		)
		return Result{Rejected: true}, errors.Join(err, statusErr)
	}
	status := revision.Status
	status.ObservedGeneration = object.GetGeneration()
	status.AppliedRevision = int64(candidate.Revision())
	status.AppliedEpoch = int64(candidate.Epoch())
	status.AppliedHash = candidate.Hash()
	status.ManagedResources = int32(resources.length())
	status.Phase = topologyv1alpha1.PhaseReady
	apiMeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type: "Validated", Status: metav1.ConditionTrue,
		Reason: "Accepted", Message: "candidate accepted",
		ObservedGeneration: object.GetGeneration(),
	})
	apiMeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue,
		Reason: "ResourcesApplied", Message: "resources applied",
		ObservedGeneration: object.GetGeneration(),
	})
	if err := updateStatus(ctx, resource, object, status); err != nil {
		return Result{}, err
	}
	return Result{Resources: int32(resources.length())}, nil
}

func (reconciler *Reconciler) compile(
	ctx context.Context,
	revision *topologyv1alpha1.TopologyRevision,
) (control.Candidate, topologykubernetes.Config, error) {
	if revision.Spec.Revision <= 0 || revision.Spec.Plan.Epoch <= 0 ||
		revision.Spec.ExpectedHash == "" ||
		len(revision.Spec.Workloads) == 0 ||
		len(revision.Spec.Workloads) > maximumWorkloads ||
		len(revision.Spec.Routes) > maximumRoutes {
		return control.Candidate{}, topologykubernetes.Config{}, ErrInvalidRevision
	}
	planPayload, err := json.Marshal(revision.Spec.Plan)
	if err != nil {
		return control.Candidate{}, topologykubernetes.Config{}, err
	}
	plan, err := planfile.Parse(planPayload)
	if err != nil {
		return control.Candidate{}, topologykubernetes.Config{}, err
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(
		revision.Spec.Signature,
	)
	if err != nil {
		return control.Candidate{}, topologykubernetes.Config{}, err
	}
	candidate, err := control.NewCandidate(control.CandidateSpec{
		Revision: uint64(revision.Spec.Revision), Plan: plan,
		Hash: revision.Spec.ExpectedHash, Signature: signature,
	})
	if err != nil {
		return control.Candidate{}, topologykubernetes.Config{}, err
	}
	if reconciler.allowUnsigned {
		if len(candidate.Signature()) != 0 {
			return control.Candidate{}, topologykubernetes.Config{}, ErrInvalidRevision
		}
	} else if err := reconciler.verifier.Verify(ctx, candidate); err != nil {
		return control.Candidate{}, topologykubernetes.Config{}, err
	}
	document, err := control.MarshalDocument(candidate)
	if err != nil {
		return control.Candidate{}, topologykubernetes.Config{}, err
	}
	workloads := make([]topologykubernetes.Workload, 0, len(revision.Spec.Workloads))
	for _, workload := range revision.Spec.Workloads {
		workloads = append(workloads, topologykubernetes.Workload{
			Placement: topology.PlacementID(workload.Placement),
			Name:      workload.Name, Image: workload.Image,
			Replicas: workload.Replicas, ContainerPort: workload.ContainerPort,
			HealthPort: workload.HealthPort, Expose: workload.Expose,
			Constraint: topologykubernetes.PlacementConstraint{
				Cluster: workload.Constraint.Cluster,
				Region:  workload.Constraint.Region,
				Zone:    workload.Constraint.Zone,
			},
		})
	}
	routes := make(
		[]topologykubernetes.ReachabilityRoute,
		0,
		len(revision.Spec.Routes),
	)
	for _, route := range revision.Spec.Routes {
		routes = append(routes, topologykubernetes.ReachabilityRoute{
			FromCluster: route.FromCluster, ToCluster: route.ToCluster,
		})
	}
	planName := revision.Spec.PlanName
	if planName == "" {
		planName = revision.Name
	}
	return candidate, topologykubernetes.Config{
		Namespace: revision.Namespace, PlanName: planName,
		Plan: plan, Workloads: workloads, Routes: routes,
		Control: &topologykubernetes.ControlConfig{
			Document: document, PublicKey: reconciler.publicKeyText,
			AllowUnsigned: reconciler.allowUnsigned,
		},
		TerminationGraceSeconds: revision.Spec.Lifecycle.TerminationGraceSeconds,
		EndpointDrainSeconds:    revision.Spec.Lifecycle.EndpointDrainSeconds,
		DrainTimeoutSeconds:     revision.Spec.Lifecycle.DrainTimeoutSeconds,
		MinimumReadySeconds:     revision.Spec.Lifecycle.MinimumReadySeconds,
	}, nil
}

type renderedResources struct {
	configMaps  []*corev1.ConfigMap
	deployments []*appsv1.Deployment
	services    []*corev1.Service
}

type desiredResources struct {
	configMaps  map[string]struct{}
	deployments map[string]struct{}
	services    map[string]struct{}
}

func decodeResources(payload []byte) (renderedResources, error) {
	var result renderedResources
	for _, part := range strings.Split(strings.TrimSpace(string(payload)), "\n---\n") {
		jsonPayload, err := yaml.YAMLToJSON([]byte(part))
		if err != nil {
			return renderedResources{}, err
		}
		var identity metav1.TypeMeta
		if err := json.Unmarshal(jsonPayload, &identity); err != nil {
			return renderedResources{}, err
		}
		switch identity.Kind {
		case "ConfigMap":
			var resource corev1.ConfigMap
			if err := json.Unmarshal(jsonPayload, &resource); err != nil {
				return renderedResources{}, err
			}
			result.configMaps = append(result.configMaps, &resource)
		case "Deployment":
			var resource appsv1.Deployment
			if err := json.Unmarshal(jsonPayload, &resource); err != nil {
				return renderedResources{}, err
			}
			result.deployments = append(result.deployments, &resource)
		case "Service":
			var resource corev1.Service
			if err := json.Unmarshal(jsonPayload, &resource); err != nil {
				return renderedResources{}, err
			}
			result.services = append(result.services, &resource)
		default:
			return renderedResources{}, ErrInvalidRevision
		}
	}
	return result, nil
}

func (resources renderedResources) length() int {
	return len(resources.configMaps) + len(resources.deployments) +
		len(resources.services)
}

func (reconciler *Reconciler) applyResources(
	ctx context.Context,
	namespace string,
	revisionName string,
	owner metav1.OwnerReference,
	resources renderedResources,
) (desiredResources, error) {
	desired := desiredResources{
		configMaps:  make(map[string]struct{}, len(resources.configMaps)),
		deployments: make(map[string]struct{}, len(resources.deployments)),
		services:    make(map[string]struct{}, len(resources.services)),
	}
	for _, resource := range resources.configMaps {
		prepareManaged(resource, revisionName, owner)
		if err := reconciler.applyConfigMap(ctx, namespace, resource); err != nil {
			return desiredResources{}, err
		}
		desired.configMaps[resource.Name] = struct{}{}
	}
	for _, resource := range resources.services {
		prepareManaged(resource, revisionName, owner)
		if err := reconciler.applyService(ctx, namespace, resource); err != nil {
			return desiredResources{}, err
		}
		desired.services[resource.Name] = struct{}{}
	}
	for _, resource := range resources.deployments {
		prepareManaged(resource, revisionName, owner)
		if err := reconciler.applyDeployment(ctx, namespace, resource); err != nil {
			return desiredResources{}, err
		}
		desired.deployments[resource.Name] = struct{}{}
	}
	return desired, nil
}

func prepareManaged(
	resource metav1.Object,
	revisionName string,
	owner metav1.OwnerReference,
) {
	labels := resource.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[ManagedRevisionLabel] = revisionName
	resource.SetLabels(labels)
	resource.SetOwnerReferences([]metav1.OwnerReference{owner})
}

func (reconciler *Reconciler) applyConfigMap(
	ctx context.Context,
	namespace string,
	desired *corev1.ConfigMap,
) error {
	client := reconciler.kubernetes.CoreV1().ConfigMaps(namespace)
	current, err := client.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !ownedBy(current, desired.OwnerReferences[0].UID) {
		return ErrResourceConflict
	}
	desired.ResourceVersion = current.ResourceVersion
	_, err = client.Update(ctx, desired, metav1.UpdateOptions{})
	return err
}

func (reconciler *Reconciler) applyDeployment(
	ctx context.Context,
	namespace string,
	desired *appsv1.Deployment,
) error {
	client := reconciler.kubernetes.AppsV1().Deployments(namespace)
	current, err := client.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !ownedBy(current, desired.OwnerReferences[0].UID) {
		return ErrResourceConflict
	}
	desired.ResourceVersion = current.ResourceVersion
	_, err = client.Update(ctx, desired, metav1.UpdateOptions{})
	return err
}

func (reconciler *Reconciler) applyService(
	ctx context.Context,
	namespace string,
	desired *corev1.Service,
) error {
	client := reconciler.kubernetes.CoreV1().Services(namespace)
	current, err := client.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !ownedBy(current, desired.OwnerReferences[0].UID) {
		return ErrResourceConflict
	}
	desired.ResourceVersion = current.ResourceVersion
	desired.Spec.ClusterIP = current.Spec.ClusterIP
	desired.Spec.ClusterIPs = append([]string(nil), current.Spec.ClusterIPs...)
	desired.Spec.IPFamilies = append([]corev1.IPFamily(nil), current.Spec.IPFamilies...)
	desired.Spec.IPFamilyPolicy = current.Spec.IPFamilyPolicy
	desired.Spec.HealthCheckNodePort = current.Spec.HealthCheckNodePort
	_, err = client.Update(ctx, desired, metav1.UpdateOptions{})
	return err
}

func (reconciler *Reconciler) deleteStale(
	ctx context.Context,
	namespace string,
	revisionName string,
	uid types.UID,
	desired desiredResources,
) error {
	selector := ManagedRevisionLabel + "=" + revisionName
	deployments, err := reconciler.kubernetes.AppsV1().Deployments(namespace).List(
		ctx, metav1.ListOptions{LabelSelector: selector},
	)
	if err != nil {
		return err
	}
	for index := range deployments.Items {
		resource := &deployments.Items[index]
		if _, keep := desired.deployments[resource.Name]; !keep && ownedBy(resource, uid) {
			if err := reconciler.kubernetes.AppsV1().Deployments(namespace).Delete(
				ctx, resource.Name, metav1.DeleteOptions{},
			); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	services, err := reconciler.kubernetes.CoreV1().Services(namespace).List(
		ctx, metav1.ListOptions{LabelSelector: selector},
	)
	if err != nil {
		return err
	}
	for index := range services.Items {
		resource := &services.Items[index]
		if _, keep := desired.services[resource.Name]; !keep && ownedBy(resource, uid) {
			if err := reconciler.kubernetes.CoreV1().Services(namespace).Delete(
				ctx, resource.Name, metav1.DeleteOptions{},
			); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	configMaps, err := reconciler.kubernetes.CoreV1().ConfigMaps(namespace).List(
		ctx, metav1.ListOptions{LabelSelector: selector},
	)
	if err != nil {
		return err
	}
	for index := range configMaps.Items {
		resource := &configMaps.Items[index]
		if _, keep := desired.configMaps[resource.Name]; !keep && ownedBy(resource, uid) {
			if err := reconciler.kubernetes.CoreV1().ConfigMaps(namespace).Delete(
				ctx, resource.Name, metav1.DeleteOptions{},
			); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (reconciler *Reconciler) finalize(
	ctx context.Context,
	resource dynamic.ResourceInterface,
	object *unstructured.Unstructured,
) (Result, error) {
	if !containsString(object.GetFinalizers(), Finalizer) {
		return Result{Finalized: true}, nil
	}
	desired := desiredResources{
		configMaps: map[string]struct{}{}, deployments: map[string]struct{}{},
		services: map[string]struct{}{},
	}
	if err := reconciler.deleteStale(
		ctx,
		object.GetNamespace(),
		object.GetName(),
		object.GetUID(),
		desired,
	); err != nil {
		return Result{}, err
	}
	updated := object.DeepCopy()
	updated.SetFinalizers(removeString(updated.GetFinalizers(), Finalizer))
	if _, err := resource.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return Result{}, err
	}
	return Result{Finalized: true}, nil
}

func ensureFinalizer(
	ctx context.Context,
	resource dynamic.ResourceInterface,
	object *unstructured.Unstructured,
) (*unstructured.Unstructured, error) {
	if containsString(object.GetFinalizers(), Finalizer) {
		return object, nil
	}
	updated := object.DeepCopy()
	updated.SetFinalizers(append(updated.GetFinalizers(), Finalizer))
	return resource.Update(ctx, updated, metav1.UpdateOptions{})
}

func (reconciler *Reconciler) reject(
	ctx context.Context,
	resource dynamic.ResourceInterface,
	object *unstructured.Unstructured,
	reason string,
	revision *topologyv1alpha1.TopologyRevision,
) error {
	status := topologyv1alpha1.TopologyRevisionStatus{}
	if revision != nil {
		status = revision.Status
	} else {
		var loose topologyv1alpha1.TopologyRevision
		_ = runtime.DefaultUnstructuredConverter.FromUnstructured(
			object.Object,
			&loose,
		)
		status = loose.Status
	}
	status.ObservedGeneration = object.GetGeneration()
	status.Phase = topologyv1alpha1.PhaseRejected
	apiMeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type: "Validated", Status: metav1.ConditionFalse,
		Reason: reason, Message: "candidate rejected",
		ObservedGeneration: object.GetGeneration(),
	})
	apiMeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type: "Ready", Status: metav1.ConditionFalse,
		Reason: "LastGoodRetained", Message: "last-good retained",
		ObservedGeneration: object.GetGeneration(),
	})
	return updateStatus(ctx, resource, object, status)
}

func updateStatus(
	ctx context.Context,
	resource dynamic.ResourceInterface,
	object *unstructured.Unstructured,
	status topologyv1alpha1.TopologyRevisionStatus,
) error {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&status)
	if err != nil {
		return err
	}
	updated := object.DeepCopy()
	if err := unstructured.SetNestedMap(updated.Object, content, "status"); err != nil {
		return err
	}
	_, err = resource.UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	return err
}

func decodeRevision(
	object *unstructured.Unstructured,
) (*topologyv1alpha1.TopologyRevision, error) {
	if object == nil {
		return nil, ErrInvalidRevision
	}
	payload, err := json.Marshal(object.Object)
	if err != nil || len(payload) > maximumRevisionDocumentBytes {
		return nil, ErrInvalidRevision
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var revision topologyv1alpha1.TopologyRevision
	if err := decoder.Decode(&revision); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, ErrInvalidRevision
	}
	if revision.APIVersion != topologyv1alpha1.GroupVersion.String() ||
		revision.Kind != topologyv1alpha1.Kind || revision.Name == "" ||
		revision.Namespace == "" {
		return nil, ErrInvalidRevision
	}
	return &revision, nil
}

func ownerReference(object *unstructured.Unstructured) metav1.OwnerReference {
	controller := true
	blockDeletion := true
	return metav1.OwnerReference{
		APIVersion: topologyv1alpha1.GroupVersion.String(),
		Kind:       topologyv1alpha1.Kind, Name: object.GetName(), UID: object.GetUID(),
		Controller: &controller, BlockOwnerDeletion: &blockDeletion,
	}
}

func ownedBy(resource metav1.Object, uid types.UID) bool {
	for _, owner := range resource.GetOwnerReferences() {
		if owner.UID == uid && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func rejectionReason(err error) string {
	switch {
	case errors.Is(err, topologykubernetes.ErrUnreachableRoute):
		return "UnreachableRoute"
	case errors.Is(err, control.ErrInvalidSignature):
		return "InvalidSignature"
	case errors.Is(err, control.ErrInvalidCandidate):
		return "InvalidCandidate"
	default:
		return "InvalidSpec"
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func removeString(values []string, target string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != target {
			result = append(result, value)
		}
	}
	return result
}
