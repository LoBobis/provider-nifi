package processgroup

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

// SetupGated adds a controller that reconciles ProcessGroup managed resources with safe-start.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup ProcessGroup controller"))
		}
	}, v1alpha1.ProcessGroupGroupVersionKind)
	return nil
}

// Setup adds a controller that reconciles ProcessGroup managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.ProcessGroupGroupKind)

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*v1alpha1.ProcessGroup](&connector{
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
			mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics, &v1alpha1.ProcessGroupList{}, o.MetricOptions.PollStateMetricInterval,
		)
		if err := mgr.Add(stateMetricsRecorder); err != nil {
			return errors.Wrap(err, "cannot register MR state metrics recorder for ProcessGroup")
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(v1alpha1.ProcessGroupGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha1.ProcessGroup{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube  client.Client
	usage *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *v1alpha1.ProcessGroup) (managed.TypedExternalClient[*v1alpha1.ProcessGroup], error) {
	nifi, err := nificlient.GetNiFiClient(ctx, c.kube, c.usage, cr)
	if err != nil {
		return nil, err
	}
	return &external{nifi: nifi}, nil
}

type external struct {
	nifi *nificlient.NiFiClient
}

func (e *external) Observe(ctx context.Context, cr *v1alpha1.ProcessGroup) (managed.ExternalObservation, error) {
	externalName := meta.GetExternalName(cr)
	if externalName == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	pg, err := e.nifi.GetProcessGroup(externalName)
	if err != nil {
		if nificlient.IsNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, "cannot get process group")
	}

	cr.Status.AtProvider.ID = pg.Id
	if pg.Revision != nil && pg.Revision.Version != nil {
		cr.Status.AtProvider.Version = *pg.Revision.Version
	}
	cr.Status.AtProvider.RunningCount = pg.RunningCount
	cr.Status.AtProvider.StoppedCount = pg.StoppedCount
	cr.Status.AtProvider.DisabledCount = pg.DisabledCount
	cr.Status.AtProvider.InvalidCount = pg.InvalidCount

	upToDate := isProcessGroupUpToDate(cr, pg)

	cr.Status.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:    true,
		ResourceUpToDate:  upToDate,
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (e *external) Create(ctx context.Context, cr *v1alpha1.ProcessGroup) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv1.Creating())

	entity := buildProcessGroupEntity(cr)

	result, err := e.nifi.CreateProcessGroup(cr.Spec.ForProvider.ParentGroupID, entity)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot create process group")
	}

	meta.SetExternalName(cr, result.Id)
	cr.Status.AtProvider.ID = result.Id
	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	return managed.ExternalCreation{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Update(ctx context.Context, cr *v1alpha1.ProcessGroup) (managed.ExternalUpdate, error) {
	externalName := meta.GetExternalName(cr)

	entity := buildProcessGroupEntity(cr)
	entity.Id = externalName
	entity.Component.Id = externalName
	entity.Revision = &nigoapi.RevisionDto{
		Version: &cr.Status.AtProvider.Version,
	}

	result, err := e.nifi.UpdateProcessGroup(entity)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot update process group")
	}

	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	// Handle start/stop of all processors in the group
	desiredState := cr.Spec.ForProvider.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}

	if desiredState == "RUNNING" && cr.Status.AtProvider.StoppedCount > 0 {
		if err := e.nifi.ScheduleProcessGroup(externalName, "RUNNING"); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot start process group")
		}
	} else if desiredState == "STOPPED" && cr.Status.AtProvider.RunningCount > 0 {
		if err := e.nifi.ScheduleProcessGroup(externalName, "STOPPED"); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot stop process group")
		}
	}

	return managed.ExternalUpdate{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Delete(ctx context.Context, cr *v1alpha1.ProcessGroup) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())

	externalName := meta.GetExternalName(cr)

	// Stop all processors before deleting the group
	if cr.Status.AtProvider.RunningCount > 0 {
		if err := e.nifi.ScheduleProcessGroup(externalName, "STOPPED"); err != nil {
			return managed.ExternalDelete{}, errors.Wrap(err, "cannot stop process group before deletion")
		}
		pg, err := e.nifi.GetProcessGroup(externalName)
		if err != nil {
			return managed.ExternalDelete{}, errors.Wrap(err, "cannot get process group after stopping")
		}
		if pg.Revision != nil && pg.Revision.Version != nil {
			cr.Status.AtProvider.Version = *pg.Revision.Version
		}
	}

	if err := e.nifi.DeleteProcessGroup(externalName, cr.Status.AtProvider.Version); err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, "cannot delete process group")
	}

	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(ctx context.Context) error {
	return nil
}

func buildProcessGroupEntity(cr *v1alpha1.ProcessGroup) nigoapi.ProcessGroupEntity {
	p := cr.Spec.ForProvider

	component := &nigoapi.ProcessGroupDto{
		Name:     p.Name,
		Comments: p.Comments,
	}

	if p.Position != nil {
		component.Position = &nigoapi.PositionDto{
			X: p.Position.X,
			Y: p.Position.Y,
		}
	}

	if p.ParameterContextID != "" {
		component.ParameterContext = &nigoapi.ParameterContextReferenceEntity{
			Id: p.ParameterContextID,
		}
	}

	var initialVersion int64
	return nigoapi.ProcessGroupEntity{
		Component: component,
		Revision: &nigoapi.RevisionDto{
			Version: &initialVersion,
		},
	}
}

func isProcessGroupUpToDate(cr *v1alpha1.ProcessGroup, pg *nigoapi.ProcessGroupEntity) bool {
	if pg.Component == nil {
		return false
	}

	p := cr.Spec.ForProvider

	if pg.Component.Name != p.Name {
		return false
	}

	if p.Comments != "" && pg.Component.Comments != p.Comments {
		return false
	}

	desiredState := p.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}

	if desiredState == "RUNNING" && cr.Status.AtProvider.StoppedCount > 0 {
		return false
	}
	if desiredState == "STOPPED" && cr.Status.AtProvider.RunningCount > 0 {
		return false
	}

	return true
}
