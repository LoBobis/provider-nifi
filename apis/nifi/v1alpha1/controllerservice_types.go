package v1alpha1

import (
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
)

// ControllerServiceParameters are the configurable fields of a NiFi Controller Service.
type ControllerServiceParameters struct {
	// ParentGroupID is the ID of the process group this controller service belongs to.
	// +kubebuilder:validation:Required
	ParentGroupID string `json:"parentGroupId"`

	// Type is the fully qualified Java class name of the controller service.
	// For example: org.apache.nifi.dbcp.DBCPConnectionPool
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// Name is the display name of the controller service.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Properties is a map of controller service property names to values.
	// +optional
	Properties map[string]*string `json:"properties,omitempty"`

	// Comments is a description of the controller service.
	// +optional
	Comments string `json:"comments,omitempty"`

	// DesiredState is the desired state of the controller service.
	// +kubebuilder:validation:Enum=ENABLED;DISABLED
	// +kubebuilder:default=DISABLED
	// +optional
	DesiredState string `json:"desiredState,omitempty"`
}

// ControllerServiceObservation are the observable fields of a NiFi Controller Service.
type ControllerServiceObservation struct {
	// ID is the NiFi-assigned ID of the controller service.
	ID string `json:"id,omitempty"`

	// State is the current state of the controller service.
	State string `json:"state,omitempty"`

	// Version is the revision version for optimistic locking.
	Version int64 `json:"version,omitempty"`

	// ValidationStatus is the validation status.
	ValidationStatus string `json:"validationStatus,omitempty"`

	// ValidationErrors lists any validation errors.
	// +optional
	ValidationErrors []string `json:"validationErrors,omitempty"`
}

// A ControllerServiceSpec defines the desired state of a NiFi Controller Service.
type ControllerServiceSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              ControllerServiceParameters `json:"forProvider"`
}

// A ControllerServiceStatus represents the observed state of a NiFi Controller Service.
type ControllerServiceStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          ControllerServiceObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// A ControllerService is a managed resource that represents a NiFi Controller Service.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,nifi}
type ControllerService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ControllerServiceSpec   `json:"spec"`
	Status ControllerServiceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ControllerServiceList contains a list of ControllerService
type ControllerServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ControllerService `json:"items"`
}

// ControllerService type metadata.
var (
	ControllerServiceKind             = reflect.TypeOf(ControllerService{}).Name()
	ControllerServiceGroupKind        = schema.GroupKind{Group: Group, Kind: ControllerServiceKind}.String()
	ControllerServiceKindAPIVersion   = ControllerServiceKind + "." + SchemeGroupVersion.String()
	ControllerServiceGroupVersionKind = SchemeGroupVersion.WithKind(ControllerServiceKind)
)

func init() {
	SchemeBuilder.Register(&ControllerService{}, &ControllerServiceList{})
}
