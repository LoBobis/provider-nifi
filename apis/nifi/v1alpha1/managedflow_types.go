package v1alpha1

import (
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
)

// RolloutStrategy defines how updates are applied to a ManagedFlow.
// +kubebuilder:validation:Enum=InPlace;BlueGreen
type RolloutStrategy string

const (
	// RolloutStrategyInPlace updates the flow in place (same as RegistryFlow).
	RolloutStrategyInPlace RolloutStrategy = "InPlace"
	// RolloutStrategyBlueGreen creates a new PG, validates it, then cuts over.
	RolloutStrategyBlueGreen RolloutStrategy = "BlueGreen"
)

// ManagedFlowPhase represents the current lifecycle phase of a ManagedFlow.
type ManagedFlowPhase string

const (
	ManagedFlowPhaseImporting        ManagedFlowPhase = "Importing"
	ManagedFlowPhaseEnablingServices ManagedFlowPhase = "EnablingServices"
	ManagedFlowPhaseStarting         ManagedFlowPhase = "Starting"
	ManagedFlowPhaseHealthChecking   ManagedFlowPhase = "HealthChecking"
	ManagedFlowPhaseDrainingOld      ManagedFlowPhase = "DrainingOld"
	ManagedFlowPhaseActive           ManagedFlowPhase = "Active"
	ManagedFlowPhaseRollingBack      ManagedFlowPhase = "RollingBack"
	ManagedFlowPhaseFailed           ManagedFlowPhase = "Failed"
)

// HealthCheckConfig configures health checking behavior during rollout.
type HealthCheckConfig struct {
	// StabilizationWindow is the duration to wait with no error bulletins
	// before considering the flow healthy. Uses Go duration format (e.g., "30s", "2m").
	// +kubebuilder:default="30s"
	// +optional
	StabilizationWindow string `json:"stabilizationWindow,omitempty"`
}

// RolloutConfig configures the rollout strategy for flow updates.
type RolloutConfig struct {
	// Strategy is the rollout strategy to use when updating the flow.
	// +kubebuilder:validation:Enum=InPlace;BlueGreen
	// +kubebuilder:default=InPlace
	// +optional
	Strategy RolloutStrategy `json:"strategy,omitempty"`

	// HealthCheck configures health checking during rollout.
	// +optional
	HealthCheck *HealthCheckConfig `json:"healthCheck,omitempty"`

	// DrainTimeout is how long to wait for the old PG's queues to drain
	// before forcefully deleting it. Uses Go duration format (e.g., "60s", "5m").
	// +kubebuilder:default="60s"
	// +optional
	DrainTimeout string `json:"drainTimeout,omitempty"`
}

// ParameterContextConfig defines an inline parameter context to be created
// and managed as part of the ManagedFlow lifecycle.
type ParameterContextConfig struct {
	// Name is the display name of the parameter context in NiFi.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Description is a description of the parameter context.
	// +optional
	Description string `json:"description,omitempty"`

	// Parameters is the list of parameters in this context.
	// +optional
	Parameters []Parameter `json:"parameters,omitempty"`

	// InheritedParameterContexts is a list of parameter context IDs to inherit from.
	// +optional
	InheritedParameterContexts []string `json:"inheritedParameterContexts,omitempty"`
}

// ManagedFlowParameters are the configurable fields for a ManagedFlow.
type ManagedFlowParameters struct {
	// ParentGroupID is the ID of the parent process group where the flow will be imported.
	// +kubebuilder:validation:Required
	ParentGroupID string `json:"parentGroupId"`

	// RegistryID is the ID of the NiFi Registry client configured in NiFi.
	// +kubebuilder:validation:Required
	RegistryID string `json:"registryId"`

	// BucketID is the ID of the bucket in the registry.
	// +kubebuilder:validation:Required
	BucketID string `json:"bucketId"`

	// FlowID is the ID of the versioned flow in the registry.
	// +kubebuilder:validation:Required
	FlowID string `json:"flowId"`

	// FlowVersion is the version of the flow to import.
	// Use -1 or 0 for the latest version.
	// +optional
	FlowVersion int32 `json:"flowVersion,omitempty"`

	// ParameterContextID is the ID of an existing parameter context to assign to the
	// process group. Mutually exclusive with parameterContext.
	// +optional
	ParameterContextID string `json:"parameterContextId,omitempty"`

	// ParameterContext defines an inline parameter context to be created and managed
	// alongside the flow. Changes to parameters will trigger a rollout.
	// Mutually exclusive with parameterContextId.
	// +optional
	ParameterContext *ParameterContextConfig `json:"parameterContext,omitempty"`

	// DesiredState is the desired run status of the imported flow's processors.
	// +kubebuilder:validation:Enum=RUNNING;STOPPED
	// +kubebuilder:default=STOPPED
	// +optional
	DesiredState string `json:"desiredState,omitempty"`

	// Position is the position of the imported process group on the canvas.
	// +optional
	Position *Position `json:"position,omitempty"`

	// Rollout configures the rollout strategy for flow updates.
	// +optional
	Rollout *RolloutConfig `json:"rollout,omitempty"`
}

// ManagedFlowObservation are the observable fields of a ManagedFlow.
type ManagedFlowObservation struct {
	// ActiveProcessGroupID is the NiFi-assigned ID of the currently active process group.
	ActiveProcessGroupID string `json:"activeProcessGroupId,omitempty"`

	// PendingProcessGroupID is the NiFi-assigned ID of the new process group
	// being validated during a blue-green rollout. Empty when no rollout is in progress.
	PendingProcessGroupID string `json:"pendingProcessGroupId,omitempty"`

	// ParameterContextID is the NiFi-assigned ID of the managed parameter context.
	// Only set when using inline parameterContext (not parameterContextId).
	ParameterContextID string `json:"parameterContextId,omitempty"`

	// PreviousParameterContextID is the ID of the old parameter context pending deletion
	// after a parameter rotation. Cleared after successful cleanup.
	// +optional
	PreviousParameterContextID string `json:"previousParameterContextId,omitempty"`

	// PreviousProcessGroupID is the ID of an old process group that failed to delete
	// during cutover. The controller will retry cleanup on subsequent reconciles.
	// +optional
	PreviousProcessGroupID string `json:"previousProcessGroupId,omitempty"`

	// CurrentVersion is the currently deployed flow version.
	CurrentVersion int32 `json:"currentVersion,omitempty"`

	// Phase is the current lifecycle phase of the ManagedFlow.
	Phase ManagedFlowPhase `json:"phase,omitempty"`

	// ControllerServicesEnabled is the number of enabled controller services in the active PG.
	ControllerServicesEnabled int32 `json:"controllerServicesEnabled,omitempty"`

	// ControllerServicesTotal is the total number of controller services in the active PG.
	ControllerServicesTotal int32 `json:"controllerServicesTotal,omitempty"`

	// Version is the revision version for optimistic locking.
	Version int64 `json:"version,omitempty"`

	// HealthCheckStartTime is when the health check began during rollout.
	// +optional
	HealthCheckStartTime *metav1.Time `json:"healthCheckStartTime,omitempty"`

	// DrainStartTime is when queue draining began during blue-green cutover.
	// +optional
	DrainStartTime *metav1.Time `json:"drainStartTime,omitempty"`

	// LastRolloutTime is the timestamp of the last successful rollout.
	// +optional
	LastRolloutTime *metav1.Time `json:"lastRolloutTime,omitempty"`

	// LastAppliedGeneration is the metadata.generation that was last successfully applied.
	// Used to detect spec changes (parameters, etc.) that don't change flow version.
	// +optional
	LastAppliedGeneration int64 `json:"lastAppliedGeneration,omitempty"`

	// LastAppliedParameterHash is a hash of the inline parameter context config
	// that was last successfully deployed. Used to detect actual parameter changes
	// without false positives from position/desiredState changes.
	// +optional
	LastAppliedParameterHash string `json:"lastAppliedParameterHash,omitempty"`

	// FailedFlowVersion is the flow version that last failed during rollout.
	// +optional
	FailedFlowVersion int32 `json:"failedFlowVersion,omitempty"`

	// FailedGeneration is the metadata.generation when the last failure occurred.
	// When phase is Failed and metadata.generation == failedGeneration, the controller
	// stops retrying. Any spec change bumps the generation and triggers a new attempt.
	// +optional
	FailedGeneration int64 `json:"failedGeneration,omitempty"`

	// Message is a human-readable status message.
	Message string `json:"message,omitempty"`
}

// A ManagedFlowSpec defines the desired state of a ManagedFlow.
type ManagedFlowSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              ManagedFlowParameters `json:"forProvider"`
}

// A ManagedFlowStatus represents the observed state of a ManagedFlow.
type ManagedFlowStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          ManagedFlowObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// A ManagedFlow is a managed resource that deploys a NiFi flow from a Registry
// with support for blue-green rollout on updates.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="PHASE",type="string",JSONPath=".status.atProvider.phase"
// +kubebuilder:printcolumn:name="VERSION",type="integer",JSONPath=".status.atProvider.currentVersion"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,nifi}
type ManagedFlow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ManagedFlowSpec   `json:"spec"`
	Status ManagedFlowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ManagedFlowList contains a list of ManagedFlow.
type ManagedFlowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ManagedFlow `json:"items"`
}

// ManagedFlow type metadata.
var (
	ManagedFlowKind             = reflect.TypeOf(ManagedFlow{}).Name()
	ManagedFlowGroupKind        = schema.GroupKind{Group: Group, Kind: ManagedFlowKind}.String()
	ManagedFlowKindAPIVersion   = ManagedFlowKind + "." + SchemeGroupVersion.String()
	ManagedFlowGroupVersionKind = SchemeGroupVersion.WithKind(ManagedFlowKind)
)

func init() {
	SchemeBuilder.Register(&ManagedFlow{}, &ManagedFlowList{})
}
