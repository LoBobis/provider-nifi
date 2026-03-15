package controllerservice

import (
	"context"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/feature"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"github.com/crossplane/crossplane-runtime/v2/pkg/statemetrics"
	"github.com/pkg/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"

	"github.com/crossplane-contrib/provider-nifi/apis/nifi/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-nifi/apis/v1alpha1"
	nificlient "github.com/crossplane-contrib/provider-nifi/internal/clients"
)

// SetupGated adds a controller that reconciles ControllerService managed resources with safe-start.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup ControllerService controller"))
		}
	}, v1alpha1.ControllerServiceGroupVersionKind)
	return nil
}

// Setup adds a controller that reconciles ControllerService managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.ControllerServiceGroupKind)

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*v1alpha1.ControllerService](&connector{
			kube:  mgr.GetClient(),
			usage: resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
		}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
	}

	if o.Features.Enabled(feature.EnableBetaManagementPolicies) {
		opts = append(opts, managed.WithManagementPolicies())
	}

	if o.Features.Enabled(feature.EnableAlphaChangeLogs) {
		opts = append(opts, managed.WithChangeLogger(o.ChangeLogOptions.ChangeLogger))
	}

	if o.MetricOptions != nil {
		opts = append(opts, managed.WithMetricRecorder(o.MetricOptions.MRMetrics))
	}

	if o.MetricOptions != nil && o.MetricOptions.MRStateMetrics != nil {
		stateMetricsRecorder := statemetrics.NewMRStateRecorder(
			mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics, &v1alpha1.ControllerServiceList{}, o.MetricOptions.PollStateMetricInterval,
		)
		if err := mgr.Add(stateMetricsRecorder); err != nil {
			return errors.Wrap(err, "cannot register MR state metrics recorder for ControllerService")
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(v1alpha1.ControllerServiceGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha1.ControllerService{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube  client.Client
	usage *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *v1alpha1.ControllerService) (managed.TypedExternalClient[*v1alpha1.ControllerService], error) {
	nifi, err := nificlient.GetNiFiClient(ctx, c.kube, c.usage, cr)
	if err != nil {
		return nil, err
	}
	return &external{nifi: nifi}, nil
}

type external struct {
	nifi *nificlient.NiFiClient
}

func (e *external) Observe(ctx context.Context, cr *v1alpha1.ControllerService) (managed.ExternalObservation, error) {
	externalName := meta.GetExternalName(cr)
	if externalName == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	cs, err := e.nifi.GetControllerService(externalName)
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, "cannot get controller service")
	}

	cr.Status.AtProvider.ID = cs.Id
	if cs.Revision != nil && cs.Revision.Version != nil {
		cr.Status.AtProvider.Version = *cs.Revision.Version
	}
	if cs.Component != nil {
		cr.Status.AtProvider.State = cs.Component.State
		cr.Status.AtProvider.ValidationStatus = cs.Component.ValidationStatus
		cr.Status.AtProvider.ValidationErrors = cs.Component.ValidationErrors
	}

	upToDate := isControllerServiceUpToDate(cr, cs)

	cr.Status.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:    true,
		ResourceUpToDate:  upToDate,
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (e *external) Create(ctx context.Context, cr *v1alpha1.ControllerService) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv1.Creating())

	entity := buildControllerServiceEntity(cr)

	result, err := e.nifi.CreateControllerService(cr.Spec.ForProvider.ParentGroupID, entity)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot create controller service")
	}

	meta.SetExternalName(cr, result.Id)
	cr.Status.AtProvider.ID = result.Id
	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	// Enable if desired
	desiredState := cr.Spec.ForProvider.DesiredState
	if desiredState == "ENABLED" {
		if err := e.nifi.UpdateControllerServiceRunStatus(result.Id, "ENABLED", cr.Status.AtProvider.Version); err != nil {
			return managed.ExternalCreation{}, errors.Wrap(err, "cannot enable controller service after creation")
		}
	}

	return managed.ExternalCreation{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Update(ctx context.Context, cr *v1alpha1.ControllerService) (managed.ExternalUpdate, error) {
	externalName := meta.GetExternalName(cr)

	// If the service is ENABLED, we must disable it before updating
	if cr.Status.AtProvider.State == "ENABLED" {
		if err := e.nifi.UpdateControllerServiceRunStatus(externalName, "DISABLED", cr.Status.AtProvider.Version); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot disable controller service for update")
		}
		cs, err := e.nifi.GetControllerService(externalName)
		if err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot get controller service after disabling")
		}
		if cs.Revision != nil && cs.Revision.Version != nil {
			cr.Status.AtProvider.Version = *cs.Revision.Version
		}
	}

	entity := buildControllerServiceEntity(cr)
	entity.Id = externalName
	entity.Revision = &nigoapi.RevisionDto{
		Version: &cr.Status.AtProvider.Version,
	}

	result, err := e.nifi.UpdateControllerService(entity)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot update controller service")
	}

	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	// Re-enable if desired
	desiredState := cr.Spec.ForProvider.DesiredState
	if desiredState == "" {
		desiredState = "DISABLED"
	}
	if desiredState == "ENABLED" {
		if err := e.nifi.UpdateControllerServiceRunStatus(externalName, "ENABLED", cr.Status.AtProvider.Version); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot re-enable controller service after update")
		}
	}

	return managed.ExternalUpdate{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Delete(ctx context.Context, cr *v1alpha1.ControllerService) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())

	externalName := meta.GetExternalName(cr)

	// Disable before deleting
	if cr.Status.AtProvider.State == "ENABLED" {
		if err := e.nifi.UpdateControllerServiceRunStatus(externalName, "DISABLED", cr.Status.AtProvider.Version); err != nil {
			return managed.ExternalDelete{}, errors.Wrap(err, "cannot disable controller service before deletion")
		}
		cs, err := e.nifi.GetControllerService(externalName)
		if err != nil {
			return managed.ExternalDelete{}, errors.Wrap(err, "cannot get controller service after disabling")
		}
		if cs.Revision != nil && cs.Revision.Version != nil {
			cr.Status.AtProvider.Version = *cs.Revision.Version
		}
	}

	if err := e.nifi.DeleteControllerService(externalName, cr.Status.AtProvider.Version); err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, "cannot delete controller service")
	}

	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(ctx context.Context) error {
	return nil
}

func buildControllerServiceEntity(cr *v1alpha1.ControllerService) nigoapi.ControllerServiceEntity {
	p := cr.Spec.ForProvider

	component := &nigoapi.ControllerServiceDto{
		Type_:         p.Type,
		Name:          p.Name,
		Properties:    ptrMapToStringMap(p.Properties),
		Comments:      p.Comments,
		ParentGroupId: p.ParentGroupID,
	}

	var initialVersion int64
	return nigoapi.ControllerServiceEntity{
		Component: component,
		Revision: &nigoapi.RevisionDto{
			Version: &initialVersion,
		},
	}
}

func isControllerServiceUpToDate(cr *v1alpha1.ControllerService, cs *nigoapi.ControllerServiceEntity) bool {
	if cs.Component == nil {
		return false
	}

	p := cr.Spec.ForProvider

	if cs.Component.Name != p.Name {
		return false
	}

	// Check properties
	if p.Properties != nil {
		for k, v := range p.Properties {
			actual, ok := cs.Component.Properties[k]
			if !ok {
				return false
			}
			if v != nil && *v != actual {
				return false
			}
		}
	}

	// Check state
	desiredState := p.DesiredState
	if desiredState == "" {
		desiredState = "DISABLED"
	}
	if cs.Component.State != desiredState {
		return false
	}

	return true
}

// ptrMapToStringMap converts map[string]*string to map[string]string.
func ptrMapToStringMap(m map[string]*string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string, len(m))
	for k, v := range m {
		if v != nil {
			result[k] = *v
		}
	}
	return result
}
