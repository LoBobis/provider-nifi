package v1alpha1

import (
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
)

// ProcessGroupParameters are the configurable fields of a NiFi ProcessGroup.
type ProcessGroupParameters struct {
	// ParentGroupID is the ID of the parent process group.
	// Use "root" for the root process group.
	// +kubebuilder:validation:Required
	ParentGroupID string `json:"parentGroupId"`

	// Name is the display name of the process group.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Position is the position of the process group on the canvas.
	// +optional
	Position *Position `json:"position,omitempty"`

	// Comments is a description of the process group.
	// +optional
	Comments string `json:"comments,omitempty"`

	// ParameterContextID is the ID of the parameter context to bind to this group.
	// +optional
	ParameterContextID string `json:"parameterContextId,omitempty"`

	// DesiredState is the desired run status of all processors in this group.
	// +kubebuilder:validation:Enum=RUNNING;STOPPED;DISABLED
	// +kubebuilder:default=STOPPED
	// +optional
	DesiredState string `json:"desiredState,omitempty"`
}

// ProcessGroupObservation are the observable fields of a NiFi ProcessGroup.
type ProcessGroupObservation struct {
	// ID is the NiFi-assigned ID of the process group.
	ID string `json:"id,omitempty"`

	// RunningCount is the number of running processors.
	RunningCount int32 `json:"runningCount,omitempty"`

	// StoppedCount is the number of stopped processors.
	StoppedCount int32 `json:"stoppedCount,omitempty"`

	// DisabledCount is the number of disabled processors.
	DisabledCount int32 `json:"disabledCount,omitempty"`

	// InvalidCount is the number of invalid processors.
	InvalidCount int32 `json:"invalidCount,omitempty"`

	// Version is the revision version for optimistic locking.
	Version int64 `json:"version,omitempty"`
}

// A ProcessGroupSpec defines the desired state of a NiFi ProcessGroup.
type ProcessGroupSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              ProcessGroupParameters `json:"forProvider"`
}

// A ProcessGroupStatus represents the observed state of a NiFi ProcessGroup.
type ProcessGroupStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          ProcessGroupObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// A ProcessGroup is a managed resource that represents a NiFi Process Group.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,nifi}
type ProcessGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProcessGroupSpec   `json:"spec"`
	Status ProcessGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ProcessGroupList contains a list of ProcessGroup
type ProcessGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ProcessGroup `json:"items"`
}

// ProcessGroup type metadata.
var (
	ProcessGroupKind             = reflect.TypeOf(ProcessGroup{}).Name()
	ProcessGroupGroupKind        = schema.GroupKind{Group: Group, Kind: ProcessGroupKind}.String()
	ProcessGroupKindAPIVersion   = ProcessGroupKind + "." + SchemeGroupVersion.String()
	ProcessGroupGroupVersionKind = SchemeGroupVersion.WithKind(ProcessGroupKind)
)

func init() {
	SchemeBuilder.Register(&ProcessGroup{}, &ProcessGroupList{})
}
