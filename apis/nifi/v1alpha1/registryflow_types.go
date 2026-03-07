package v1alpha1

import (
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
)

// RegistryFlowParameters are the configurable fields for importing a flow from a NiFi Registry.
type RegistryFlowParameters struct {
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

	// Position is the position of the imported process group on the canvas.
	// +optional
	Position *Position `json:"position,omitempty"`

	// DesiredState is the desired run status of the imported flow's processors.
	// +kubebuilder:validation:Enum=RUNNING;STOPPED;DISABLED
	// +kubebuilder:default=STOPPED
	// +optional
	DesiredState string `json:"desiredState,omitempty"`
}

// RegistryFlowObservation are the observable fields of a NiFi Registry Flow.
type RegistryFlowObservation struct {
	// ProcessGroupID is the NiFi-assigned ID of the created process group.
	ProcessGroupID string `json:"processGroupId,omitempty"`

	// CurrentVersion is the currently deployed flow version.
	CurrentVersion int32 `json:"currentVersion,omitempty"`

	// LatestVersion is the latest available version in the registry.
	LatestVersion int32 `json:"latestVersion,omitempty"`

	// Stale indicates whether the deployed version is behind the latest.
	Stale bool `json:"stale,omitempty"`

	// Version is the revision version for optimistic locking.
	Version int64 `json:"version,omitempty"`
}

// A RegistryFlowSpec defines the desired state of a NiFi Registry Flow import.
type RegistryFlowSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              RegistryFlowParameters `json:"forProvider"`
}

// A RegistryFlowStatus represents the observed state of a NiFi Registry Flow import.
type RegistryFlowStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          RegistryFlowObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// A RegistryFlow is a managed resource that represents a NiFi flow imported from a Registry.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,nifi}
type RegistryFlow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RegistryFlowSpec   `json:"spec"`
	Status RegistryFlowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RegistryFlowList contains a list of RegistryFlow
type RegistryFlowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RegistryFlow `json:"items"`
}

// RegistryFlow type metadata.
var (
	RegistryFlowKind             = reflect.TypeOf(RegistryFlow{}).Name()
	RegistryFlowGroupKind        = schema.GroupKind{Group: Group, Kind: RegistryFlowKind}.String()
	RegistryFlowKindAPIVersion   = RegistryFlowKind + "." + SchemeGroupVersion.String()
	RegistryFlowGroupVersionKind = SchemeGroupVersion.WithKind(RegistryFlowKind)
)

func init() {
	SchemeBuilder.Register(&RegistryFlow{}, &RegistryFlowList{})
}
