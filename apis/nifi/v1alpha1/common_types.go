package v1alpha1

// Position represents the position of a component on the NiFi canvas.
type Position struct {
	// X is the x coordinate.
	// +kubebuilder:validation:Type=number
	// +optional
	X float64 `json:"x,omitempty"`

	// Y is the y coordinate.
	// +kubebuilder:validation:Type=number
	// +optional
	Y float64 `json:"y,omitempty"`
}
