package registryflow

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

// SetupGated adds a controller that reconciles RegistryFlow managed resources with safe-start.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup RegistryFlow controller"))
		}
	}, v1alpha1.RegistryFlowGroupVersionKind)
	return nil
}

// Setup adds a controller that reconciles RegistryFlow managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.RegistryFlowGroupKind)

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*v1alpha1.RegistryFlow](&connector{
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
			mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics, &v1alpha1.RegistryFlowList{}, o.MetricOptions.PollStateMetricInterval,
		)
		if err := mgr.Add(stateMetricsRecorder); err != nil {
			return errors.Wrap(err, "cannot register MR state metrics recorder for RegistryFlow")
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(v1alpha1.RegistryFlowGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha1.RegistryFlow{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube  client.Client
	usage *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *v1alpha1.RegistryFlow) (managed.TypedExternalClient[*v1alpha1.RegistryFlow], error) {
	nifi, err := nificlient.GetNiFiClient(ctx, c.kube, c.usage, cr)
	if err != nil {
		return nil, err
	}
	return &external{nifi: nifi}, nil
}

type external struct {
	nifi *nificlient.NiFiClient
}

func (e *external) Observe(ctx context.Context, cr *v1alpha1.RegistryFlow) (managed.ExternalObservation, error) {
	externalName := meta.GetExternalName(cr)
	if externalName == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	pg, err := e.nifi.GetProcessGroup(externalName)
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, "cannot get process group for registry flow")
	}

	cr.Status.AtProvider.ProcessGroupID = pg.Id
	if pg.Revision != nil && pg.Revision.Version != nil {
		cr.Status.AtProvider.Version = *pg.Revision.Version
	}

	// Check version control information
	vci, err := e.nifi.GetVersionControlInfo(externalName)
	if err == nil && vci.VersionControlInformation != nil {
		// Version is interface{} in nigoapi - extract as int32
		if v, ok := vci.VersionControlInformation.Version.(float64); ok {
			cr.Status.AtProvider.CurrentVersion = int32(v)
		}
		// Check staleness from state field
		cr.Status.AtProvider.Stale = vci.VersionControlInformation.State == "STALE" ||
			vci.VersionControlInformation.State == "LOCALLY_MODIFIED_AND_STALE"
	}

	upToDate := isRegistryFlowUpToDate(cr)

	cr.Status.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:    true,
		ResourceUpToDate:  upToDate,
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (e *external) Create(ctx context.Context, cr *v1alpha1.RegistryFlow) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv1.Creating())

	p := cr.Spec.ForProvider

	var position *nigoapi.PositionDto
	if p.Position != nil {
		position = &nigoapi.PositionDto{
			X: p.Position.X,
			Y: p.Position.Y,
		}
	} else {
		position = &nigoapi.PositionDto{X: 0, Y: 0}
	}

	result, err := e.nifi.ImportFlowFromRegistry(
		p.ParentGroupID,
		p.RegistryID,
		p.BucketID,
		p.FlowID,
		p.FlowVersion,
		position,
	)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot import flow from registry")
	}

	meta.SetExternalName(cr, result.Id)
	cr.Status.AtProvider.ProcessGroupID = result.Id
	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	// Start the flow if desired
	desiredState := p.DesiredState
	if desiredState == "RUNNING" {
		if err := e.nifi.ScheduleProcessGroup(result.Id, "RUNNING"); err != nil {
			return managed.ExternalCreation{}, errors.Wrap(err, "cannot start imported flow")
		}
	}

	return managed.ExternalCreation{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Update(ctx context.Context, cr *v1alpha1.RegistryFlow) (managed.ExternalUpdate, error) {
	externalName := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider

	// Check if version needs to change
	desiredVersion := p.FlowVersion
	if desiredVersion > 0 && cr.Status.AtProvider.CurrentVersion != desiredVersion {
		vci, err := e.nifi.GetVersionControlInfo(externalName)
		if err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot get version control info for update")
		}

		if vci.VersionControlInformation != nil {
			vci.VersionControlInformation.Version = int32(desiredVersion)
			if err := e.nifi.ChangeFlowVersion(externalName, *vci); err != nil {
				return managed.ExternalUpdate{}, errors.Wrap(err, "cannot change flow version")
			}
		}
	}

	// Handle start/stop
	desiredState := p.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}

	pg, err := e.nifi.GetProcessGroup(externalName)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot get process group for state update")
	}

	if desiredState == "RUNNING" {
		if pg.StoppedCount > 0 {
			if err := e.nifi.ScheduleProcessGroup(externalName, "RUNNING"); err != nil {
				return managed.ExternalUpdate{}, errors.Wrap(err, "cannot start registry flow")
			}
		}
	} else if desiredState == "STOPPED" {
		if pg.RunningCount > 0 {
			if err := e.nifi.ScheduleProcessGroup(externalName, "STOPPED"); err != nil {
				return managed.ExternalUpdate{}, errors.Wrap(err, "cannot stop registry flow")
			}
		}
	}

	return managed.ExternalUpdate{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Delete(ctx context.Context, cr *v1alpha1.RegistryFlow) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())

	externalName := meta.GetExternalName(cr)

	// Stop everything first
	_ = e.nifi.ScheduleProcessGroup(externalName, "STOPPED")

	// Refresh version after stop
	pg, err := e.nifi.GetProcessGroup(externalName)
	if err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, "cannot get process group for deletion")
	}
	var version int64
	if pg.Revision != nil && pg.Revision.Version != nil {
		version = *pg.Revision.Version
	}

	if err := e.nifi.DeleteProcessGroup(externalName, version); err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, "cannot delete process group for registry flow")
	}

	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(ctx context.Context) error {
	return nil
}

func isRegistryFlowUpToDate(cr *v1alpha1.RegistryFlow) bool {
	p := cr.Spec.ForProvider

	if p.FlowVersion > 0 && cr.Status.AtProvider.CurrentVersion != p.FlowVersion {
		return false
	}

	return true
}
