package v1alpha1

import (
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
)

// ConnectableRef identifies a NiFi component that can be connected.
type ConnectableRef struct {
	// ID is the ID of the connectable component.
	// +kubebuilder:validation:Required
	ID string `json:"id"`

	// Type is the type of the connectable component.
	// +kubebuilder:validation:Enum=PROCESSOR;INPUT_PORT;OUTPUT_PORT;FUNNEL;REMOTE_INPUT_PORT;REMOTE_OUTPUT_PORT
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// GroupID is the ID of the group that the connectable belongs to.
	// +kubebuilder:validation:Required
	GroupID string `json:"groupId"`
}

// ConnectionParameters are the configurable fields of a NiFi Connection.
type ConnectionParameters struct {
	// ParentGroupID is the ID of the process group this connection belongs to.
	// +kubebuilder:validation:Required
	ParentGroupID string `json:"parentGroupId"`

	// Source identifies the source component.
	// +kubebuilder:validation:Required
	Source ConnectableRef `json:"source"`

	// Destination identifies the destination component.
	// +kubebuilder:validation:Required
	Destination ConnectableRef `json:"destination"`

	// SelectedRelationships is the list of relationships from the source to route through this connection.
	// +optional
	SelectedRelationships []string `json:"selectedRelationships,omitempty"`

	// Name is an optional display name for the connection.
	// +optional
	Name string `json:"name,omitempty"`

	// FlowFileExpiration is the maximum time a FlowFile can remain in the connection (e.g. "0 sec", "1 hour").
	// +optional
	FlowFileExpiration string `json:"flowFileExpiration,omitempty"`

	// BackPressureObjectThreshold is the max number of FlowFiles before back pressure is applied.
	// +optional
	BackPressureObjectThreshold int64 `json:"backPressureObjectThreshold,omitempty"`

	// BackPressureDataSizeThreshold is the max data size before back pressure is applied (e.g. "1 GB").
	// +optional
	BackPressureDataSizeThreshold string `json:"backPressureDataSizeThreshold,omitempty"`

	// Bends are bend points on the connection line.
	// +optional
	Bends []Position `json:"bends,omitempty"`

	// LabelIndex is the index of the bend point to display the label at.
	// +optional
	LabelIndex int32 `json:"labelIndex,omitempty"`

	// Prioritizers is a list of prioritizer class names.
	// +optional
	Prioritizers []string `json:"prioritizers,omitempty"`

	// LoadBalanceStrategy is the strategy for load balancing FlowFiles across the cluster.
	// +kubebuilder:validation:Enum=DO_NOT_LOAD_BALANCE;PARTITION_BY_ATTRIBUTE;ROUND_ROBIN;SINGLE_NODE
	// +optional
	LoadBalanceStrategy string `json:"loadBalanceStrategy,omitempty"`

	// LoadBalancePartitionAttribute is the attribute to partition by when using PARTITION_BY_ATTRIBUTE strategy.
	// +optional
	LoadBalancePartitionAttribute string `json:"loadBalancePartitionAttribute,omitempty"`

	// LoadBalanceCompression is the compression to use for load balancing.
	// +kubebuilder:validation:Enum=DO_NOT_COMPRESS;COMPRESS_ATTRIBUTES_ONLY;COMPRESS_ATTRIBUTES_AND_CONTENT
	// +optional
	LoadBalanceCompression string `json:"loadBalanceCompression,omitempty"`
}

// ConnectionObservation are the observable fields of a NiFi Connection.
type ConnectionObservation struct {
	// ID is the NiFi-assigned ID of the connection.
	ID string `json:"id,omitempty"`

	// Version is the revision version for optimistic locking.
	Version int64 `json:"version,omitempty"`
}

// A ConnectionSpec defines the desired state of a NiFi Connection.
type ConnectionSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              ConnectionParameters `json:"forProvider"`
}

// A ConnectionStatus represents the observed state of a NiFi Connection.
type ConnectionStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          ConnectionObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// A Connection is a managed resource that represents a NiFi Connection.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,nifi}
type Connection struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ConnectionSpec   `json:"spec"`
	Status ConnectionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ConnectionList contains a list of Connection
type ConnectionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Connection `json:"items"`
}

// Connection type metadata.
var (
	ConnectionKind             = reflect.TypeOf(Connection{}).Name()
	ConnectionGroupKind        = schema.GroupKind{Group: Group, Kind: ConnectionKind}.String()
	ConnectionKindAPIVersion   = ConnectionKind + "." + SchemeGroupVersion.String()
	ConnectionGroupVersionKind = SchemeGroupVersion.WithKind(ConnectionKind)
)

func init() {
	SchemeBuilder.Register(&Connection{}, &ConnectionList{})
}
