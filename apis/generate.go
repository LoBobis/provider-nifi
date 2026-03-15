// NOTE: See the below link for details on what is happening here.
// https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module

// Remove existing CRDs
//go:generate rm -rf ../package/crds

// Generate deepcopy methodsets
//go:generate go run -tags generate sigs.k8s.io/controller-tools/cmd/controller-gen object:headerFile=../hack/boilerplate.go.txt paths=./...

// Generate CRD manifests for provider config types
//go:generate go run -tags generate sigs.k8s.io/controller-tools/cmd/controller-gen crd:crdVersions=v1,allowDangerousTypes=true paths=./v1alpha1/... output:crd:artifacts:config=../package/crds

// Generate CRD manifests for NiFi resource types
//go:generate go run -tags generate sigs.k8s.io/controller-tools/cmd/controller-gen crd:crdVersions=v1,allowDangerousTypes=true paths=./nifi/v1alpha1/... output:crd:artifacts:config=../package/crds

// Generate crossplane-runtime methodsets (resource.Claim, etc)
//go:generate go run -tags generate github.com/crossplane/crossplane-tools/cmd/angryjet generate-methodsets --header-file=../hack/boilerplate.go.txt ./...

package apis
