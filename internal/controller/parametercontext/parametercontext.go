package parametercontext

import (
	"context"
	"fmt"

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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"

	"github.com/crossplane-contrib/provider-nifi/apis/nifi/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-nifi/apis/v1alpha1"
	nificlient "github.com/crossplane-contrib/provider-nifi/internal/clients"
)

// SetupGated adds a controller that reconciles ParameterContext managed resources with safe-start.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup ParameterContext controller"))
		}
	}, v1alpha1.ParameterContextGroupVersionKind)
	return nil
}

// Setup adds a controller that reconciles ParameterContext managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.ParameterContextGroupKind)

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*v1alpha1.ParameterContext](&connector{
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
			mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics, &v1alpha1.ParameterContextList{}, o.MetricOptions.PollStateMetricInterval,
		)
		if err := mgr.Add(stateMetricsRecorder); err != nil {
			return errors.Wrap(err, "cannot register MR state metrics recorder for ParameterContext")
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(v1alpha1.ParameterContextGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha1.ParameterContext{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube  client.Client
	usage *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *v1alpha1.ParameterContext) (managed.TypedExternalClient[*v1alpha1.ParameterContext], error) {
	nifi, err := nificlient.GetNiFiClient(ctx, c.kube, c.usage, cr)
	if err != nil {
		return nil, err
	}
	return &external{nifi: nifi, kube: c.kube}, nil
}

type external struct {
	nifi *nificlient.NiFiClient
	kube client.Client
}

func (e *external) Observe(ctx context.Context, cr *v1alpha1.ParameterContext) (managed.ExternalObservation, error) {
	externalName := meta.GetExternalName(cr)
	if externalName == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	pc, err := e.nifi.GetParameterContext(externalName)
	if err != nil {
		if nificlient.IsNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, "cannot get parameter context")
	}

	cr.Status.AtProvider.ID = pc.Id
	if pc.Revision != nil && pc.Revision.Version != nil {
		cr.Status.AtProvider.Version = *pc.Revision.Version
	}

	// Extract bound process groups
	if pc.Component != nil && pc.Component.BoundProcessGroups != nil {
		groups := make([]string, 0, len(pc.Component.BoundProcessGroups))
		for _, pg := range pc.Component.BoundProcessGroups {
			groups = append(groups, pg.Id)
		}
		cr.Status.AtProvider.BoundProcessGroups = groups
	}

	upToDate := isParameterContextUpToDate(cr, pc)

	cr.Status.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:    true,
		ResourceUpToDate:  upToDate,
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (e *external) Create(ctx context.Context, cr *v1alpha1.ParameterContext) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv1.Creating())

	entity, err := e.buildParameterContextEntity(ctx, cr)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot build parameter context entity")
	}

	result, err := e.nifi.CreateParameterContext(entity)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot create parameter context")
	}

	meta.SetExternalName(cr, result.Id)
	cr.Status.AtProvider.ID = result.Id
	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	return managed.ExternalCreation{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Update(ctx context.Context, cr *v1alpha1.ParameterContext) (managed.ExternalUpdate, error) {
	externalName := meta.GetExternalName(cr)

	entity, err := e.buildParameterContextEntity(ctx, cr)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot build parameter context entity")
	}
	entity.Id = externalName
	entity.Component.Id = externalName
	entity.Revision = &nigoapi.RevisionDto{
		Version: &cr.Status.AtProvider.Version,
	}

	result, err := e.nifi.UpdateParameterContext(entity)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot update parameter context")
	}

	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	return managed.ExternalUpdate{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Delete(ctx context.Context, cr *v1alpha1.ParameterContext) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())

	externalName := meta.GetExternalName(cr)

	if err := e.nifi.DeleteParameterContext(externalName, cr.Status.AtProvider.Version); err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, "cannot delete parameter context")
	}

	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(ctx context.Context) error {
	return nil
}

func (e *external) buildParameterContextEntity(ctx context.Context, cr *v1alpha1.ParameterContext) (nigoapi.ParameterContextEntity, error) {
	p := cr.Spec.ForProvider

	component := &nigoapi.ParameterContextDto{
		Name:        p.Name,
		Description: p.Description,
	}

	// Build parameters
	if len(p.Parameters) > 0 {
		params := make([]nigoapi.ParameterEntity, len(p.Parameters))
		for i, param := range p.Parameters {
			desc := param.Description
			paramDto := nigoapi.ParameterDto{
				Name:        param.Name,
				Description: &desc,
				Sensitive:   param.Sensitive,
			}
			// Resolve value: valueFromSecret takes precedence over inline value
			val, err := resolveParameterValue(ctx, e.kube, param, cr.Namespace)
			if err != nil {
				return nigoapi.ParameterContextEntity{}, errors.Wrapf(err, "cannot resolve value for parameter %q", param.Name)
			}
			if val != nil {
				paramDto.Value = val
			}
			params[i] = nigoapi.ParameterEntity{
				Parameter: &paramDto,
			}
		}
		component.Parameters = params
	}

	// Build inherited parameter contexts
	if len(p.InheritedParameterContexts) > 0 {
		inherited := make([]nigoapi.ParameterContextReferenceEntity, len(p.InheritedParameterContexts))
		for i, id := range p.InheritedParameterContexts {
			inherited[i] = nigoapi.ParameterContextReferenceEntity{
				Id: id,
			}
		}
		component.InheritedParameterContexts = inherited
	}

	var initialVersion int64
	return nigoapi.ParameterContextEntity{
		Component: component,
		Revision: &nigoapi.RevisionDto{
			Version: &initialVersion,
		},
	}, nil
}

// resolveParameterValue resolves the parameter value from either inline value or a Kubernetes Secret.
// If valueFromSecret is set, it takes precedence over the inline value.
func resolveParameterValue(ctx context.Context, kube client.Client, param v1alpha1.Parameter, defaultNamespace string) (*string, error) {
	if param.ValueFromSecret != nil {
		ref := param.ValueFromSecret
		ns := ref.Namespace
		if ns == "" {
			ns = defaultNamespace
		}

		secret := &corev1.Secret{}
		if err := kube.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ns}, secret); err != nil {
			return nil, fmt.Errorf("cannot get secret %s/%s: %w", ns, ref.Name, err)
		}

		data, ok := secret.Data[ref.Key]
		if !ok {
			return nil, fmt.Errorf("key %q not found in secret %s/%s", ref.Key, ns, ref.Name)
		}

		val := string(data)
		return &val, nil
	}

	return param.Value, nil
}

func isParameterContextUpToDate(cr *v1alpha1.ParameterContext, pc *nigoapi.ParameterContextEntity) bool {
	if pc.Component == nil {
		return false
	}

	p := cr.Spec.ForProvider

	if pc.Component.Name != p.Name {
		return false
	}

	if p.Description != "" && pc.Component.Description != p.Description {
		return false
	}

	// Compare parameter count
	if len(p.Parameters) != len(pc.Component.Parameters) {
		return false
	}

	// Build map of existing parameters for comparison
	existing := make(map[string]nigoapi.ParameterDto)
	for _, pe := range pc.Component.Parameters {
		if pe.Parameter != nil {
			existing[pe.Parameter.Name] = *pe.Parameter
		}
	}

	for _, desired := range p.Parameters {
		actual, ok := existing[desired.Name]
		if !ok {
			return false
		}
		if desired.Value != nil && actual.Value != nil && *actual.Value != *desired.Value {
			return false
		}
		if desired.Description != "" && actual.Description != nil && *actual.Description != desired.Description {
			return false
		}
	}

	return true
}
