package v1alpha1

import (
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
)

// SecretKeySelector selects a key from a Kubernetes Secret.
type SecretKeySelector struct {
	// Name of the Secret.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace of the Secret. Defaults to the namespace of the resource.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Key within the Secret to select.
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// Parameter represents a single parameter in a parameter context.
type Parameter struct {
	// Name is the parameter name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Value is the parameter value. For sensitive parameters, prefer using
	// valueFromSecret instead to avoid storing secrets in plain text.
	// +optional
	Value *string `json:"value,omitempty"`

	// ValueFromSecret references a Kubernetes Secret key to use as the parameter value.
	// This is the recommended way to set sensitive parameter values.
	// If both value and valueFromSecret are set, valueFromSecret takes precedence.
	// +optional
	ValueFromSecret *SecretKeySelector `json:"valueFromSecret,omitempty"`

	// Description is the parameter description.
	// +optional
	Description string `json:"description,omitempty"`

	// Sensitive marks the parameter as sensitive. When true, the parameter value
	// will be encrypted at rest in NiFi and masked in the NiFi UI.
	// For sensitive parameters, it is strongly recommended to use valueFromSecret
	// instead of value to avoid storing secrets in the Kubernetes resource spec.
	// +optional
	Sensitive bool `json:"sensitive,omitempty"`
}

// ParameterContextParameters are the configurable fields of a NiFi Parameter Context.
type ParameterContextParameters struct {
	// Name is the display name of the parameter context.
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

// ParameterContextObservation are the observable fields of a NiFi Parameter Context.
type ParameterContextObservation struct {
	// ID is the NiFi-assigned ID of the parameter context.
	ID string `json:"id,omitempty"`

	// Version is the revision version for optimistic locking.
	Version int64 `json:"version,omitempty"`

	// BoundProcessGroups lists the IDs of process groups bound to this context.
	// +optional
	BoundProcessGroups []string `json:"boundProcessGroups,omitempty"`
}

// A ParameterContextSpec defines the desired state of a NiFi Parameter Context.
type ParameterContextSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              ParameterContextParameters `json:"forProvider"`
}

// A ParameterContextStatus represents the observed state of a NiFi Parameter Context.
type ParameterContextStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          ParameterContextObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// A ParameterContext is a managed resource that represents a NiFi Parameter Context.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,nifi}
type ParameterContext struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ParameterContextSpec   `json:"spec"`
	Status ParameterContextStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ParameterContextList contains a list of ParameterContext
type ParameterContextList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ParameterContext `json:"items"`
}

// ParameterContext type metadata.
var (
	ParameterContextKind             = reflect.TypeOf(ParameterContext{}).Name()
	ParameterContextGroupKind        = schema.GroupKind{Group: Group, Kind: ParameterContextKind}.String()
	ParameterContextKindAPIVersion   = ParameterContextKind + "." + SchemeGroupVersion.String()
	ParameterContextGroupVersionKind = SchemeGroupVersion.WithKind(ParameterContextKind)
)

func init() {
	SchemeBuilder.Register(&ParameterContext{}, &ParameterContextList{})
}
