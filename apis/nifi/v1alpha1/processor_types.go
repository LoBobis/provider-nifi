package v1alpha1

import (
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
)

// ProcessorParameters are the configurable fields of a NiFi Processor.
type ProcessorParameters struct {
	// ParentGroupID is the ID of the process group this processor belongs to.
	// +kubebuilder:validation:Required
	ParentGroupID string `json:"parentGroupId"`

	// Type is the fully qualified Java class name of the processor.
	// For example: org.apache.nifi.processors.standard.GenerateFlowFile
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// Name is the display name of the processor.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Position is the position of the processor on the canvas.
	// +optional
	Position *Position `json:"position,omitempty"`

	// Config contains the processor configuration properties.
	// +optional
	Config *ProcessorConfig `json:"config,omitempty"`

	// DesiredState is the desired run status of the processor.
	// +kubebuilder:validation:Enum=RUNNING;STOPPED;DISABLED
	// +kubebuilder:default=STOPPED
	// +optional
	DesiredState string `json:"desiredState,omitempty"`
}

// ProcessorConfig contains processor-specific configuration.
type ProcessorConfig struct {
	// Properties is a map of processor property names to values.
	// +optional
	Properties map[string]*string `json:"properties,omitempty"`

	// AutoTerminatedRelationships is a list of relationships to auto-terminate.
	// +optional
	AutoTerminatedRelationships []string `json:"autoTerminatedRelationships,omitempty"`

	// SchedulingStrategy is the scheduling strategy (TIMER_DRIVEN, CRON_DRIVEN, EVENT_DRIVEN).
	// +kubebuilder:validation:Enum=TIMER_DRIVEN;CRON_DRIVEN;EVENT_DRIVEN
	// +optional
	SchedulingStrategy string `json:"schedulingStrategy,omitempty"`

	// SchedulingPeriod is the scheduling period (e.g. "0 sec", "1 min", "0 0 * * * ?").
	// +optional
	SchedulingPeriod string `json:"schedulingPeriod,omitempty"`

	// ConcurrentlySchedulableTaskCount is the number of concurrent tasks.
	// +optional
	ConcurrentlySchedulableTaskCount int32 `json:"concurrentlySchedulableTaskCount,omitempty"`

	// PenaltyDuration is the penalty duration (e.g. "30 sec").
	// +optional
	PenaltyDuration string `json:"penaltyDuration,omitempty"`

	// YieldDuration is the yield duration (e.g. "1 sec").
	// +optional
	YieldDuration string `json:"yieldDuration,omitempty"`

	// BulletinLevel is the bulletin level (DEBUG, INFO, WARN, ERROR).
	// +kubebuilder:validation:Enum=DEBUG;INFO;WARN;ERROR
	// +optional
	BulletinLevel string `json:"bulletinLevel,omitempty"`

	// Comments is a description of the processor.
	// +optional
	Comments string `json:"comments,omitempty"`
}

// ProcessorObservation are the observable fields of a NiFi Processor.
type ProcessorObservation struct {
	// ID is the NiFi-assigned ID of the processor.
	ID string `json:"id,omitempty"`

	// RunStatus is the current run status of the processor.
	RunStatus string `json:"runStatus,omitempty"`

	// Version is the revision version for optimistic locking.
	Version int64 `json:"version,omitempty"`

	// ValidationStatus is the validation status of the processor.
	ValidationStatus string `json:"validationStatus,omitempty"`

	// ValidationErrors lists any validation errors.
	// +optional
	ValidationErrors []string `json:"validationErrors,omitempty"`
}

// A ProcessorSpec defines the desired state of a NiFi Processor.
type ProcessorSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              ProcessorParameters `json:"forProvider"`
}

// A ProcessorStatus represents the observed state of a NiFi Processor.
type ProcessorStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          ProcessorObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// A Processor is a managed resource that represents a NiFi Processor.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,nifi}
type Processor struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProcessorSpec   `json:"spec"`
	Status ProcessorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ProcessorList contains a list of Processor
type ProcessorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Processor `json:"items"`
}

// Processor type metadata.
var (
	ProcessorKind             = reflect.TypeOf(Processor{}).Name()
	ProcessorGroupKind        = schema.GroupKind{Group: Group, Kind: ProcessorKind}.String()
	ProcessorKindAPIVersion   = ProcessorKind + "." + SchemeGroupVersion.String()
	ProcessorGroupVersionKind = SchemeGroupVersion.WithKind(ProcessorKind)
)

func init() {
	SchemeBuilder.Register(&Processor{}, &ProcessorList{})
}
