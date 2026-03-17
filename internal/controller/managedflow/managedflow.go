package managedflow

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane-contrib/provider-nifi/apis/nifi/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-nifi/apis/v1alpha1"
	nificlient "github.com/crossplane-contrib/provider-nifi/internal/clients"
)

const (
	defaultStabilizationWindow = 30 * time.Second
	defaultDrainTimeout        = 60 * time.Second
)

// SetupGated adds a controller that reconciles ManagedFlow managed resources with safe-start.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup ManagedFlow controller"))
		}
	}, v1alpha1.ManagedFlowGroupVersionKind)
	return nil
}

// Setup adds a controller that reconciles ManagedFlow managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.ManagedFlowGroupKind)

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*v1alpha1.ManagedFlow](&connector{
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
			mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics, &v1alpha1.ManagedFlowList{}, o.MetricOptions.PollStateMetricInterval,
		)
		if err := mgr.Add(stateMetricsRecorder); err != nil {
			return errors.Wrap(err, "cannot register MR state metrics recorder for ManagedFlow")
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(v1alpha1.ManagedFlowGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha1.ManagedFlow{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube  client.Client
	usage *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.TypedExternalClient[*v1alpha1.ManagedFlow], error) {
	nifi, err := nificlient.GetNiFiClient(ctx, c.kube, c.usage, cr)
	if err != nil {
		return nil, err
	}
	return &external{nifi: nifi}, nil
}

type external struct {
	nifi *nificlient.NiFiClient
}

func (e *external) Observe(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalObservation, error) {
	externalName := meta.GetExternalName(cr)
	if externalName == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	pg, err := e.nifi.GetProcessGroup(externalName)
	if err != nil {
		if nificlient.IsNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, "cannot get process group for managed flow")
	}

	cr.Status.AtProvider.ActiveProcessGroupID = pg.Id
	if pg.Revision != nil && pg.Revision.Version != nil {
		cr.Status.AtProvider.Version = *pg.Revision.Version
	}

	// Get version control info
	vci, err := e.nifi.GetVersionControlInfo(externalName)
	if err == nil && vci.VersionControlInformation != nil {
		if v, ok := vci.VersionControlInformation.Version.(float64); ok {
			cr.Status.AtProvider.CurrentVersion = int32(v)
		}
	}

	// Get controller service counts
	services, err := e.nifi.ListControllerServicesInGroup(externalName)
	if err == nil {
		var enabled int32
		for _, svc := range services {
			if svc.Component != nil && strings.EqualFold(svc.Component.State, "ENABLED") {
				enabled++
			}
		}
		cr.Status.AtProvider.ControllerServicesEnabled = enabled
		cr.Status.AtProvider.ControllerServicesTotal = int32(len(services))
	}

	// If phase is transitional, signal that Update needs to run
	phase := cr.Status.AtProvider.Phase
	if isTransitionalPhase(phase) {
		cr.Status.SetConditions(xpv1.Available())
		return managed.ExternalObservation{
			ResourceExists:   true,
			ResourceUpToDate: false,
		}, nil
	}

	upToDate := isManagedFlowUpToDate(cr, pg)

	cr.Status.SetConditions(xpv1.Available())
	return managed.ExternalObservation{
		ResourceExists:    true,
		ResourceUpToDate:  upToDate,
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (e *external) Create(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv1.Creating())

	p := cr.Spec.ForProvider

	var position *nigoapi.PositionDto
	if p.Position != nil {
		position = &nigoapi.PositionDto{X: p.Position.X, Y: p.Position.Y}
	} else {
		position = &nigoapi.PositionDto{X: 0, Y: 0}
	}

	// 1. Import flow from registry
	result, err := e.nifi.ImportFlowFromRegistry(
		p.ParentGroupID, p.RegistryID, p.BucketID, p.FlowID, p.FlowVersion, position,
	)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot import flow from registry")
	}

	meta.SetExternalName(cr, result.Id)
	cr.Status.AtProvider.ActiveProcessGroupID = result.Id
	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}

	// 2. Create inline parameter context if specified, or assign existing one
	paramCtxID, err := e.resolveParameterContext(cr)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot resolve parameter context")
	}
	if paramCtxID != "" {
		if err := e.assignParameterContext(result.Id, paramCtxID, cr.Status.AtProvider.Version); err != nil {
			return managed.ExternalCreation{}, errors.Wrap(err, "cannot assign parameter context")
		}
	}

	// 3. Enable all controller services (batch API)
	if err := e.nifi.ActivateControllerServicesInGroup(result.Id, "ENABLED"); err != nil {
		// Non-fatal: there may be no controller services. Log and continue.
		cr.Status.AtProvider.Message = fmt.Sprintf("Warning: could not enable controller services: %v", err)
	}

	// Set phase to drive the state machine through Update cycles
	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseEnablingServices

	return managed.ExternalCreation{ConnectionDetails: managed.ConnectionDetails{}}, nil
}

func (e *external) Update(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalUpdate, error) {
	phase := cr.Status.AtProvider.Phase
	strategy := getStrategy(cr)

	switch phase {
	case "", v1alpha1.ManagedFlowPhaseActive, v1alpha1.ManagedFlowPhaseFailed:
		// Spec changed — start a rollout
		if strategy == v1alpha1.RolloutStrategyBlueGreen {
			return e.startBlueGreenRollout(ctx, cr)
		}
		return e.doInPlaceUpdate(ctx, cr)

	case v1alpha1.ManagedFlowPhaseImporting:
		// Blue-green: new PG was just imported, enable services
		return e.enableServicesOnTarget(ctx, cr)

	case v1alpha1.ManagedFlowPhaseEnablingServices:
		// Check if services are enabled, then start PG
		return e.checkServicesAndStart(ctx, cr)

	case v1alpha1.ManagedFlowPhaseStarting:
		// PG was started, begin health check
		return e.beginHealthCheck(ctx, cr)

	case v1alpha1.ManagedFlowPhaseHealthChecking:
		// Check bulletins for errors
		return e.checkHealth(ctx, cr)

	case v1alpha1.ManagedFlowPhaseDrainingOld:
		// Check if old PG is drained
		return e.checkDrain(ctx, cr)

	case v1alpha1.ManagedFlowPhaseRollingBack:
		cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseFailed
		cr.Status.AtProvider.Message = "Rollback completed: pending PG had errors, old PG is still active"
		cr.Status.AtProvider.PendingProcessGroupID = ""
		cr.Status.AtProvider.HealthCheckStartTime = nil
		return managed.ExternalUpdate{}, nil
	}

	return managed.ExternalUpdate{}, nil
}

func (e *external) Delete(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())

	// Delete active PG
	if err := e.stopAndDeletePG(cr.Status.AtProvider.ActiveProcessGroupID); err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, "cannot delete active process group")
	}

	// Delete pending PG if mid-rollout
	if cr.Status.AtProvider.PendingProcessGroupID != "" {
		if err := e.stopAndDeletePG(cr.Status.AtProvider.PendingProcessGroupID); err != nil {
			return managed.ExternalDelete{}, errors.Wrap(err, "cannot delete pending process group")
		}
	}

	// Delete managed parameter context if we created one
	if cr.Status.AtProvider.ParameterContextID != "" {
		pc, err := e.nifi.GetParameterContext(cr.Status.AtProvider.ParameterContextID)
		if err == nil && pc.Revision != nil && pc.Revision.Version != nil {
			_ = e.nifi.DeleteParameterContext(cr.Status.AtProvider.ParameterContextID, *pc.Revision.Version)
		}
	}

	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(ctx context.Context) error {
	return nil
}

// --- State machine steps ---

func (e *external) startBlueGreenRollout(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider

	var position *nigoapi.PositionDto
	if p.Position != nil {
		position = &nigoapi.PositionDto{X: p.Position.X, Y: p.Position.Y}
	} else {
		position = &nigoapi.PositionDto{X: 0, Y: 0}
	}

	// Import new version as a separate PG
	newPG, err := e.nifi.ImportFlowFromRegistry(
		p.ParentGroupID, p.RegistryID, p.BucketID, p.FlowID, p.FlowVersion, position,
	)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot import new flow version for blue-green rollout")
	}

	cr.Status.AtProvider.PendingProcessGroupID = newPG.Id
	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseImporting
	cr.Status.AtProvider.Message = fmt.Sprintf("Blue-green rollout: imported new PG %s", newPG.Id)

	// Assign parameter context to new PG (inline or by ID)
	paramCtxID, err := e.resolveParameterContext(cr)
	if err != nil {
		_ = e.stopAndDeletePG(newPG.Id)
		cr.Status.AtProvider.PendingProcessGroupID = ""
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot resolve parameter context for new PG")
	}
	if paramCtxID != "" {
		var version int64
		if newPG.Revision != nil && newPG.Revision.Version != nil {
			version = *newPG.Revision.Version
		}
		if err := e.assignParameterContext(newPG.Id, paramCtxID, version); err != nil {
			_ = e.stopAndDeletePG(newPG.Id)
			cr.Status.AtProvider.PendingProcessGroupID = ""
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot assign parameter context to new PG")
		}
	}

	return managed.ExternalUpdate{}, nil
}

func (e *external) enableServicesOnTarget(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalUpdate, error) {
	targetID := e.getTargetPGID(cr)

	if err := e.nifi.ActivateControllerServicesInGroup(targetID, "ENABLED"); err != nil {
		cr.Status.AtProvider.Message = fmt.Sprintf("Enabling controller services: %v", err)
	}

	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseEnablingServices
	cr.Status.AtProvider.Message = "Enabling controller services"
	return managed.ExternalUpdate{}, nil
}

func (e *external) checkServicesAndStart(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalUpdate, error) {
	targetID := e.getTargetPGID(cr)

	// Check if all services are enabled
	services, err := e.nifi.ListControllerServicesInGroup(targetID)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot list controller services")
	}

	allEnabled := true
	for _, svc := range services {
		if svc.Component != nil && !strings.EqualFold(svc.Component.State, "ENABLED") {
			allEnabled = false
			break
		}
	}

	if !allEnabled {
		// Retry enabling — services may still be transitioning
		_ = e.nifi.ActivateControllerServicesInGroup(targetID, "ENABLED")
		cr.Status.AtProvider.Message = "Waiting for controller services to enable"
		return managed.ExternalUpdate{}, nil
	}

	// Start the PG if desired
	desiredState := cr.Spec.ForProvider.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}

	if desiredState == "RUNNING" {
		if err := e.nifi.ScheduleProcessGroup(targetID, "RUNNING"); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot start process group")
		}
	}

	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseStarting
	cr.Status.AtProvider.Message = fmt.Sprintf("Process group %s scheduled to %s", targetID, desiredState)
	return managed.ExternalUpdate{}, nil
}

func (e *external) beginHealthCheck(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalUpdate, error) {
	targetID := e.getTargetPGID(cr)

	// Verify PG is in the expected state
	pg, err := e.nifi.GetProcessGroup(targetID)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot get process group for health check")
	}

	desiredState := cr.Spec.ForProvider.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}

	if desiredState == "RUNNING" && pg.RunningCount == 0 {
		// Not yet running, wait
		cr.Status.AtProvider.Message = "Waiting for processors to start"
		return managed.ExternalUpdate{}, nil
	}

	now := metav1.Now()
	cr.Status.AtProvider.HealthCheckStartTime = &now
	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseHealthChecking
	cr.Status.AtProvider.Message = "Health check started"
	return managed.ExternalUpdate{}, nil
}

func (e *external) checkHealth(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalUpdate, error) {
	targetID := e.getTargetPGID(cr)
	isBlueGreen := cr.Status.AtProvider.PendingProcessGroupID != ""

	stabilizationWindow := getStabilizationWindow(cr)

	// Check for error bulletins
	board, err := e.nifi.GetBulletinBoard(targetID)
	if err != nil {
		cr.Status.AtProvider.Message = fmt.Sprintf("Cannot check bulletins: %v", err)
		return managed.ExternalUpdate{}, nil
	}

	hasErrors := false
	if board.BulletinBoard != nil {
		for _, b := range board.BulletinBoard.Bulletins {
			if b.Bulletin != nil && strings.EqualFold(b.Bulletin.Level, "ERROR") {
				hasErrors = true
				cr.Status.AtProvider.Message = fmt.Sprintf("Error bulletin: %s", b.Bulletin.Message)
				break
			}
		}
	}

	if hasErrors && isBlueGreen {
		// Rollback: delete pending PG
		_ = e.stopAndDeletePG(cr.Status.AtProvider.PendingProcessGroupID)
		cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseRollingBack
		cr.Status.AtProvider.Message = "Error detected during health check, rolling back"
		return managed.ExternalUpdate{}, nil
	}

	if hasErrors {
		// Not blue-green (initial create) — just report and stay in health checking
		cr.Status.AtProvider.Message = "Error bulletins detected, waiting for resolution"
		// Reset health check timer
		now := metav1.Now()
		cr.Status.AtProvider.HealthCheckStartTime = &now
		return managed.ExternalUpdate{}, nil
	}

	// No errors — check if stabilization window has passed
	if cr.Status.AtProvider.HealthCheckStartTime == nil {
		now := metav1.Now()
		cr.Status.AtProvider.HealthCheckStartTime = &now
		return managed.ExternalUpdate{}, nil
	}

	elapsed := time.Since(cr.Status.AtProvider.HealthCheckStartTime.Time)
	if elapsed < stabilizationWindow {
		cr.Status.AtProvider.Message = fmt.Sprintf("Health check: %s / %s elapsed, no errors", elapsed.Round(time.Second), stabilizationWindow)
		return managed.ExternalUpdate{}, nil
	}

	// Health check passed
	if isBlueGreen {
		// Advance to draining old PG
		now := metav1.Now()
		cr.Status.AtProvider.DrainStartTime = &now
		cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseDrainingOld
		cr.Status.AtProvider.Message = "Health check passed, draining old process group"

		// Stop old PG to begin draining
		_ = e.nifi.ScheduleProcessGroup(cr.Status.AtProvider.ActiveProcessGroupID, "STOPPED")
	} else {
		// Initial create — we're done
		cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive
		cr.Status.AtProvider.Message = "Flow is active and healthy"
		cr.Status.AtProvider.HealthCheckStartTime = nil
		now := metav1.Now()
		cr.Status.AtProvider.LastRolloutTime = &now
	}

	return managed.ExternalUpdate{}, nil
}

func (e *external) checkDrain(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalUpdate, error) {
	oldPGID := cr.Status.AtProvider.ActiveProcessGroupID
	newPGID := cr.Status.AtProvider.PendingProcessGroupID
	drainTimeout := getDrainTimeout(cr)

	// Check if queues are drained
	drained := false
	status, err := e.nifi.GetProcessGroupStatus(oldPGID)
	if err != nil {
		// If old PG is already gone, consider it drained
		if nificlient.IsNotFound(err) {
			drained = true
		} else {
			cr.Status.AtProvider.Message = fmt.Sprintf("Cannot get old PG status: %v", err)
		}
	} else if status.ProcessGroupStatus != nil && status.ProcessGroupStatus.AggregateSnapshot != nil {
		drained = status.ProcessGroupStatus.AggregateSnapshot.FlowFilesQueued == 0
	}

	// Check timeout
	timedOut := false
	if cr.Status.AtProvider.DrainStartTime != nil {
		timedOut = time.Since(cr.Status.AtProvider.DrainStartTime.Time) >= drainTimeout
	}

	if !drained && !timedOut {
		cr.Status.AtProvider.Message = "Waiting for old process group queues to drain"
		return managed.ExternalUpdate{}, nil
	}

	if timedOut && !drained {
		cr.Status.AtProvider.Message = "Drain timeout exceeded, force-deleting old process group"
	}

	// Delete old PG
	if err := e.stopAndDeletePG(oldPGID); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot delete old process group during cutover")
	}

	// Swap: new PG becomes active
	meta.SetExternalName(cr, newPGID)
	cr.Status.AtProvider.ActiveProcessGroupID = newPGID
	cr.Status.AtProvider.PendingProcessGroupID = ""
	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive
	cr.Status.AtProvider.Message = "Blue-green rollout completed successfully"
	cr.Status.AtProvider.HealthCheckStartTime = nil
	cr.Status.AtProvider.DrainStartTime = nil
	now := metav1.Now()
	cr.Status.AtProvider.LastRolloutTime = &now

	return managed.ExternalUpdate{}, nil
}

func (e *external) doInPlaceUpdate(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalUpdate, error) {
	externalName := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider

	// Change flow version if needed
	desiredVersion := p.FlowVersion
	if desiredVersion > 0 && cr.Status.AtProvider.CurrentVersion != desiredVersion {
		vci, err := e.nifi.GetVersionControlInfo(externalName)
		if err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot get version control info for in-place update")
		}
		if vci.VersionControlInformation != nil {
			vci.VersionControlInformation.Version = int32(desiredVersion)
			if err := e.nifi.ChangeFlowVersion(externalName, *vci); err != nil {
				return managed.ExternalUpdate{}, errors.Wrap(err, "cannot change flow version in-place")
			}
		}
	}

	// Re-enable controller services
	_ = e.nifi.ActivateControllerServicesInGroup(externalName, "ENABLED")

	// Handle start/stop
	desiredState := p.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}

	pg, err := e.nifi.GetProcessGroup(externalName)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot get process group for state update")
	}

	if desiredState == "RUNNING" && pg.StoppedCount > 0 {
		if err := e.nifi.ScheduleProcessGroup(externalName, "RUNNING"); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot start managed flow")
		}
	} else if desiredState == "STOPPED" && pg.RunningCount > 0 {
		if err := e.nifi.ScheduleProcessGroup(externalName, "STOPPED"); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot stop managed flow")
		}
	}

	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive
	cr.Status.AtProvider.Message = "In-place update completed"
	return managed.ExternalUpdate{}, nil
}

// --- Helpers ---

// resolveParameterContext returns the parameter context ID to assign.
// If inline parameterContext is specified, it creates or updates the managed parameter context.
// If parameterContextId is specified, it returns that ID directly.
func (e *external) resolveParameterContext(cr *v1alpha1.ManagedFlow) (string, error) {
	p := cr.Spec.ForProvider

	// Inline parameter context takes precedence
	if p.ParameterContext != nil {
		if cr.Status.AtProvider.ParameterContextID != "" {
			// Update existing managed parameter context
			existing, err := e.nifi.GetParameterContext(cr.Status.AtProvider.ParameterContextID)
			if err != nil {
				return "", errors.Wrap(err, "cannot get existing managed parameter context")
			}
			entity := buildInlineParameterContextEntity(p.ParameterContext)
			entity.Component.Id = existing.Component.Id
			entity.Id = existing.Id
			entity.Revision = existing.Revision
			updated, err := e.nifi.UpdateParameterContext(entity)
			if err != nil {
				return "", errors.Wrap(err, "cannot update managed parameter context")
			}
			return updated.Id, nil
		}
		// Create new managed parameter context
		entity := buildInlineParameterContextEntity(p.ParameterContext)
		result, err := e.nifi.CreateParameterContext(entity)
		if err != nil {
			return "", errors.Wrap(err, "cannot create managed parameter context")
		}
		cr.Status.AtProvider.ParameterContextID = result.Id
		return result.Id, nil
	}

	// Direct ID reference
	if p.ParameterContextID != "" {
		return p.ParameterContextID, nil
	}

	return "", nil
}

func buildInlineParameterContextEntity(cfg *v1alpha1.ParameterContextConfig) nigoapi.ParameterContextEntity {
	component := &nigoapi.ParameterContextDto{
		Name:        cfg.Name,
		Description: cfg.Description,
	}

	if len(cfg.Parameters) > 0 {
		params := make([]nigoapi.ParameterEntity, len(cfg.Parameters))
		for i, param := range cfg.Parameters {
			desc := param.Description
			paramDto := nigoapi.ParameterDto{
				Name:        param.Name,
				Description: &desc,
				Sensitive:   param.Sensitive,
			}
			if param.Value != nil {
				paramDto.Value = param.Value
			}
			params[i] = nigoapi.ParameterEntity{
				Parameter: &paramDto,
			}
		}
		component.Parameters = params
	}

	if len(cfg.InheritedParameterContexts) > 0 {
		inherited := make([]nigoapi.ParameterContextReferenceEntity, len(cfg.InheritedParameterContexts))
		for i, id := range cfg.InheritedParameterContexts {
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
	}
}

func (e *external) assignParameterContext(pgID, paramCtxID string, version int64) error {
	entity := nigoapi.ProcessGroupEntity{
		Id: pgID,
		Revision: &nigoapi.RevisionDto{
			Version: &version,
		},
		Component: &nigoapi.ProcessGroupDto{
			Id: pgID,
			ParameterContext: &nigoapi.ParameterContextReferenceEntity{
				Id: paramCtxID,
			},
		},
	}
	_, err := e.nifi.UpdateProcessGroup(entity)
	return err
}

func (e *external) stopAndDeletePG(pgID string) error {
	if pgID == "" {
		return nil
	}

	// Stop all processors first
	_ = e.nifi.ScheduleProcessGroup(pgID, "STOPPED")

	// Disable all controller services before deletion
	_ = e.nifi.ActivateControllerServicesInGroup(pgID, "DISABLED")

	// Refresh version after state changes
	pg, err := e.nifi.GetProcessGroup(pgID)
	if err != nil {
		if nificlient.IsNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot get process group for deletion")
	}

	var version int64
	if pg.Revision != nil && pg.Revision.Version != nil {
		version = *pg.Revision.Version
	}

	return e.nifi.DeleteProcessGroup(pgID, version)
}

func (e *external) getTargetPGID(cr *v1alpha1.ManagedFlow) string {
	if cr.Status.AtProvider.PendingProcessGroupID != "" {
		return cr.Status.AtProvider.PendingProcessGroupID
	}
	return cr.Status.AtProvider.ActiveProcessGroupID
}

func isTransitionalPhase(phase v1alpha1.ManagedFlowPhase) bool {
	switch phase {
	case v1alpha1.ManagedFlowPhaseImporting,
		v1alpha1.ManagedFlowPhaseEnablingServices,
		v1alpha1.ManagedFlowPhaseStarting,
		v1alpha1.ManagedFlowPhaseHealthChecking,
		v1alpha1.ManagedFlowPhaseDrainingOld,
		v1alpha1.ManagedFlowPhaseRollingBack:
		return true
	}
	return false
}

func isManagedFlowUpToDate(cr *v1alpha1.ManagedFlow, pg *nigoapi.ProcessGroupEntity) bool {
	p := cr.Spec.ForProvider

	if cr.Status.AtProvider.Phase != v1alpha1.ManagedFlowPhaseActive {
		return false
	}

	if p.FlowVersion > 0 && cr.Status.AtProvider.CurrentVersion != p.FlowVersion {
		return false
	}

	desiredState := p.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}
	if desiredState == "RUNNING" && pg.StoppedCount > 0 {
		return false
	}
	if desiredState == "STOPPED" && pg.RunningCount > 0 {
		return false
	}

	return true
}

func getStrategy(cr *v1alpha1.ManagedFlow) v1alpha1.RolloutStrategy {
	if cr.Spec.ForProvider.Rollout != nil && cr.Spec.ForProvider.Rollout.Strategy != "" {
		return cr.Spec.ForProvider.Rollout.Strategy
	}
	return v1alpha1.RolloutStrategyInPlace
}

func getStabilizationWindow(cr *v1alpha1.ManagedFlow) time.Duration {
	if cr.Spec.ForProvider.Rollout != nil && cr.Spec.ForProvider.Rollout.HealthCheck != nil {
		if d, err := time.ParseDuration(cr.Spec.ForProvider.Rollout.HealthCheck.StabilizationWindow); err == nil {
			return d
		}
	}
	return defaultStabilizationWindow
}

func getDrainTimeout(cr *v1alpha1.ManagedFlow) time.Duration {
	if cr.Spec.ForProvider.Rollout != nil {
		if d, err := time.ParseDuration(cr.Spec.ForProvider.Rollout.DrainTimeout); err == nil {
			return d
		}
	}
	return defaultDrainTimeout
}
