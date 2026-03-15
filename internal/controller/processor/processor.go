package processor

import (
	"context"
	"strings"

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

// SetupGated adds a controller that reconciles Processor managed resources with safe-start.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup Processor controller"))
		}
	}, v1alpha1.ProcessorGroupVersionKind)
	return nil
}

// Setup adds a controller that reconciles Processor managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.ProcessorGroupKind)

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*v1alpha1.Processor](&connector{
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
			mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics, &v1alpha1.ProcessorList{}, o.MetricOptions.PollStateMetricInterval,
		)
		if err := mgr.Add(stateMetricsRecorder); err != nil {
			return errors.Wrap(err, "cannot register MR state metrics recorder for Processor")
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(v1alpha1.ProcessorGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha1.Processor{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube  client.Client
	usage *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *v1alpha1.Processor) (managed.TypedExternalClient[*v1alpha1.Processor], error) {
	nifi, err := nificlient.GetNiFiClient(ctx, c.kube, c.usage, cr)
	if err != nil {
		return nil, err
	}
	return &external{nifi: nifi}, nil
}

type external struct {
	nifi *nificlient.NiFiClient
}

func (e *external) Observe(ctx context.Context, cr *v1alpha1.Processor) (managed.ExternalObservation, error) {
	externalName := meta.GetExternalName(cr)
	if externalName == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	processor, err := e.nifi.GetProcessor(externalName)
	if err != nil {
		if nificlient.IsNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, "cannot get processor")
	}

	// Update observed state
	cr.Status.AtProvider.ID = processor.Id
	cr.Status.AtProvider.RunStatus = processor.Status.RunStatus
	if processor.Revision != nil && processor.Revision.Version != nil {
		cr.Status.AtProvider.Version = *processor.Revision.Version
	}
	if processor.Component != nil {
		cr.Status.AtProvider.ValidationStatus = processor.Component.ValidationStatus
		cr.Status.AtProvider.ValidationErrors = processor.Component.ValidationErrors
	}

	upToDate := isProcessorUpToDate(cr, processor)

	cr.Status.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:    true,
		ResourceUpToDate:  upToDate,
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (e *external) Create(ctx context.Context, cr *v1alpha1.Processor) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv1.Creating())

	entity := buildProcessorEntity(cr)

	result, err := e.nifi.CreateProcessor(cr.Spec.ForProvider.ParentGroupID, entity)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot create processor")
	}

	meta.SetExternalName(cr, result.Id)
	cr.Status.AtProvider.ID = result.Id
	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	return managed.ExternalCreation{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Update(ctx context.Context, cr *v1alpha1.Processor) (managed.ExternalUpdate, error) {
	externalName := meta.GetExternalName(cr)

	// NiFi requires processors to be stopped before configuration changes.
	// Always stop first — NiFi returns run status in mixed case ("Running")
	// which may not match our uppercase constants, and stopping an already-
	// stopped processor is a safe no-op.
	if err := e.nifi.UpdateProcessorRunStatus(externalName, "STOPPED", cr.Status.AtProvider.Version); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot stop processor before update")
	}
	// Refresh version after stopping (NiFi increments revision on state changes)
	processor, err := e.nifi.GetProcessor(externalName)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot get processor after stopping")
	}
	if processor.Revision != nil && processor.Revision.Version != nil {
		cr.Status.AtProvider.Version = *processor.Revision.Version
	}

	entity := buildProcessorEntity(cr)
	entity.Id = externalName
	entity.Component.Id = externalName
	entity.Revision = &nigoapi.RevisionDto{
		Version: &cr.Status.AtProvider.Version,
	}

	result, err := e.nifi.UpdateProcessor(entity)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot update processor")
	}

	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	// Restart the processor if desired state is RUNNING
	desiredState := cr.Spec.ForProvider.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}
	if desiredState == "RUNNING" {
		if err := e.nifi.UpdateProcessorRunStatus(externalName, "RUNNING", cr.Status.AtProvider.Version); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot start processor after update")
		}
	}

	return managed.ExternalUpdate{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Delete(ctx context.Context, cr *v1alpha1.Processor) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())

	externalName := meta.GetExternalName(cr)

	// Stop the processor before deleting
	if cr.Status.AtProvider.RunStatus == "RUNNING" {
		if err := e.nifi.UpdateProcessorRunStatus(externalName, "STOPPED", cr.Status.AtProvider.Version); err != nil {
			return managed.ExternalDelete{}, errors.Wrap(err, "cannot stop processor before deletion")
		}
		// Get updated version after stopping
		processor, err := e.nifi.GetProcessor(externalName)
		if err != nil {
			return managed.ExternalDelete{}, errors.Wrap(err, "cannot get processor after stopping")
		}
		if processor.Revision != nil && processor.Revision.Version != nil {
			cr.Status.AtProvider.Version = *processor.Revision.Version
		}
	}

	if err := e.nifi.DeleteProcessor(externalName, cr.Status.AtProvider.Version); err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, "cannot delete processor")
	}

	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(ctx context.Context) error {
	return nil
}

// buildProcessorEntity constructs a NiFi API ProcessorEntity from the CR spec.
func buildProcessorEntity(cr *v1alpha1.Processor) nigoapi.ProcessorEntity {
	p := cr.Spec.ForProvider

	component := &nigoapi.ProcessorDto{
		Type_:         p.Type,
		Name:          p.Name,
		ParentGroupId: p.ParentGroupID,
	}

	if p.Position != nil {
		component.Position = &nigoapi.PositionDto{
			X: p.Position.X,
			Y: p.Position.Y,
		}
	}

	if p.Config != nil {
		config := &nigoapi.ProcessorConfigDto{
			Properties:                       ptrMapToStringMap(p.Config.Properties),
			AutoTerminatedRelationships:      p.Config.AutoTerminatedRelationships,
			SchedulingStrategy:               p.Config.SchedulingStrategy,
			SchedulingPeriod:                 p.Config.SchedulingPeriod,
			ConcurrentlySchedulableTaskCount: p.Config.ConcurrentlySchedulableTaskCount,
			PenaltyDuration:                  p.Config.PenaltyDuration,
			YieldDuration:                    p.Config.YieldDuration,
			BulletinLevel:                    p.Config.BulletinLevel,
			Comments:                         p.Config.Comments,
		}
		component.Config = config
	}

	var initialVersion int64
	return nigoapi.ProcessorEntity{
		Component: component,
		Revision: &nigoapi.RevisionDto{
			Version: &initialVersion,
		},
	}
}

// isProcessorUpToDate checks if the NiFi processor matches the desired spec.
func isProcessorUpToDate(cr *v1alpha1.Processor, processor *nigoapi.ProcessorEntity) bool {
	if processor.Component == nil {
		return false
	}

	p := cr.Spec.ForProvider

	if processor.Component.Name != p.Name {
		return false
	}

	// Check config properties
	if p.Config != nil {
		if processor.Component.Config == nil {
			return false
		}
		for k, v := range p.Config.Properties {
			actual, ok := processor.Component.Config.Properties[k]
			if !ok {
				return false
			}
			if v != nil && *v != actual {
				return false
			}
		}
		if p.Config.SchedulingStrategy != "" && processor.Component.Config.SchedulingStrategy != p.Config.SchedulingStrategy {
			return false
		}
		if p.Config.SchedulingPeriod != "" && processor.Component.Config.SchedulingPeriod != p.Config.SchedulingPeriod {
			return false
		}
	}

	// Check run status (NiFi returns mixed case like "Running", we use uppercase "RUNNING")
	desiredState := p.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}
	if processor.Status != nil && !strings.EqualFold(processor.Status.RunStatus, desiredState) {
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
