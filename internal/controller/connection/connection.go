package connection

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

// SetupGated adds a controller that reconciles Connection managed resources with safe-start.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup Connection controller"))
		}
	}, v1alpha1.ConnectionGroupVersionKind)
	return nil
}

// Setup adds a controller that reconciles Connection managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.ConnectionGroupKind)

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*v1alpha1.Connection](&connector{
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
			mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics, &v1alpha1.ConnectionList{}, o.MetricOptions.PollStateMetricInterval,
		)
		if err := mgr.Add(stateMetricsRecorder); err != nil {
			return errors.Wrap(err, "cannot register MR state metrics recorder for Connection")
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(v1alpha1.ConnectionGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha1.Connection{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube  client.Client
	usage *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *v1alpha1.Connection) (managed.TypedExternalClient[*v1alpha1.Connection], error) {
	nifi, err := nificlient.GetNiFiClient(ctx, c.kube, c.usage, cr)
	if err != nil {
		return nil, err
	}
	return &external{nifi: nifi}, nil
}

type external struct {
	nifi *nificlient.NiFiClient
}

func (e *external) Observe(ctx context.Context, cr *v1alpha1.Connection) (managed.ExternalObservation, error) {
	externalName := meta.GetExternalName(cr)
	if externalName == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	conn, err := e.nifi.GetConnection(externalName)
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, "cannot get connection")
	}

	cr.Status.AtProvider.ID = conn.Id
	if conn.Revision != nil && conn.Revision.Version != nil {
		cr.Status.AtProvider.Version = *conn.Revision.Version
	}

	upToDate := isConnectionUpToDate(cr, conn)

	cr.Status.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:    true,
		ResourceUpToDate:  upToDate,
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (e *external) Create(ctx context.Context, cr *v1alpha1.Connection) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv1.Creating())

	entity := buildConnectionEntity(cr)

	result, err := e.nifi.CreateConnection(cr.Spec.ForProvider.ParentGroupID, entity)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot create connection")
	}

	meta.SetExternalName(cr, result.Id)
	cr.Status.AtProvider.ID = result.Id
	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	return managed.ExternalCreation{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Update(ctx context.Context, cr *v1alpha1.Connection) (managed.ExternalUpdate, error) {
	externalName := meta.GetExternalName(cr)

	entity := buildConnectionEntity(cr)
	entity.Id = externalName
	entity.Revision = &nigoapi.RevisionDto{
		Version: &cr.Status.AtProvider.Version,
	}

	result, err := e.nifi.UpdateConnection(entity)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot update connection")
	}

	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	return managed.ExternalUpdate{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Delete(ctx context.Context, cr *v1alpha1.Connection) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())

	externalName := meta.GetExternalName(cr)

	if err := e.nifi.DeleteConnection(externalName, cr.Status.AtProvider.Version); err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, "cannot delete connection")
	}

	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(ctx context.Context) error {
	return nil
}

func buildConnectionEntity(cr *v1alpha1.Connection) nigoapi.ConnectionEntity {
	p := cr.Spec.ForProvider

	component := &nigoapi.ConnectionDto{
		ParentGroupId:                 p.ParentGroupID,
		SelectedRelationships:         p.SelectedRelationships,
		Name:                          p.Name,
		FlowFileExpiration:            p.FlowFileExpiration,
		BackPressureObjectThreshold:   p.BackPressureObjectThreshold,
		BackPressureDataSizeThreshold: p.BackPressureDataSizeThreshold,
		LoadBalanceStrategy:           p.LoadBalanceStrategy,
		LoadBalancePartitionAttribute: p.LoadBalancePartitionAttribute,
		LoadBalanceCompression:        p.LoadBalanceCompression,
		LabelIndex:                    p.LabelIndex,
		Prioritizers:                  p.Prioritizers,
		Source: &nigoapi.ConnectableDto{
			Id:      p.Source.ID,
			Type_:   p.Source.Type,
			GroupId: p.Source.GroupID,
		},
		Destination: &nigoapi.ConnectableDto{
			Id:      p.Destination.ID,
			Type_:   p.Destination.Type,
			GroupId: p.Destination.GroupID,
		},
	}

	if len(p.Bends) > 0 {
		bends := make([]nigoapi.PositionDto, len(p.Bends))
		for i, b := range p.Bends {
			bends[i] = nigoapi.PositionDto{X: b.X, Y: b.Y}
		}
		component.Bends = bends
	}

	var initialVersion int64
	return nigoapi.ConnectionEntity{
		Component: component,
		Revision: &nigoapi.RevisionDto{
			Version: &initialVersion,
		},
	}
}

func isConnectionUpToDate(cr *v1alpha1.Connection, conn *nigoapi.ConnectionEntity) bool {
	if conn.Component == nil {
		return false
	}

	p := cr.Spec.ForProvider

	if p.Name != "" && conn.Component.Name != p.Name {
		return false
	}

	if p.FlowFileExpiration != "" && conn.Component.FlowFileExpiration != p.FlowFileExpiration {
		return false
	}

	if p.BackPressureObjectThreshold != 0 && conn.Component.BackPressureObjectThreshold != p.BackPressureObjectThreshold {
		return false
	}

	if p.BackPressureDataSizeThreshold != "" && conn.Component.BackPressureDataSizeThreshold != p.BackPressureDataSizeThreshold {
		return false
	}

	return true
}
