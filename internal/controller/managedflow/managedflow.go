package managedflow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane-contrib/provider-nifi/apis/nifi/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-nifi/apis/v1alpha1"
	nificlient "github.com/crossplane-contrib/provider-nifi/internal/clients"
)

const (
	defaultStabilizationWindow = 30 * time.Second
	defaultDrainTimeout        = 60 * time.Second
	maxHealthCheckDuration     = 5 * time.Minute // Max time before health check gives up
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
	return &external{nifi: nifi, kube: c.kube}, nil
}

type external struct {
	nifi *nificlient.NiFiClient
	kube client.Client
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

	// Recover parameter context ID from PG if not set in status
	if cr.Status.AtProvider.ParameterContextID == "" && pg.Component != nil && pg.Component.ParameterContext != nil && pg.Component.ParameterContext.Id != "" {
		cr.Status.AtProvider.ParameterContextID = pg.Component.ParameterContext.Id
	}

	p := cr.Spec.ForProvider

	// Get version control info for CurrentVersion tracking.
	// IMPORTANT: For Git-based registries (branch is set), SKIP VCI version parsing entirely.
	// Git registries return garbage in VCI's Version field (e.g., float64(7484) — some Git
	// internal number, not a sequential version). This corrupts CurrentVersion and breaks
	// auto-update detection. For Git registries, CurrentVersion is set authoritatively
	// during Create and checkDrain (from the version count, not VCI).
	if p.Branch == "" {
		// Traditional NiFi Registry: VCI returns reliable integer versions
		vci, err := e.nifi.GetVersionControlInfo(externalName)
		if err == nil && vci.VersionControlInformation != nil {
			switch v := vci.VersionControlInformation.Version.(type) {
			case float64:
				cr.Status.AtProvider.CurrentVersion = int32(v)
			case string:
				if parsed, parseErr := nificlient.ParseVersion(v); parseErr == nil {
					cr.Status.AtProvider.CurrentVersion = parsed
				}
			}
		}
	}

	// Auto-update: poll registry for the latest version and expose it in status.
	// If autoUpdate is enabled and a new version is detected, signal not-up-to-date
	// so the Update path triggers a blue-green rollout.
	if p.AutoUpdate {
		latestVer, latestErr := e.nifi.GetLatestFlowVersion(p.RegistryID, p.BucketID, p.FlowID, p.Branch)
		if latestErr == nil && latestVer > 0 {
			cr.Status.AtProvider.LatestRegistryVersion = latestVer
		}
	}

	// Recover CurrentVersion if it's 0 (e.g., status was lost after Create, or deployed
	// with an older version of the controller). Without this, auto-update can never
	// trigger because the guard `CurrentVersion > 0` always fails.
	if cr.Status.AtProvider.CurrentVersion == 0 && cr.Status.AtProvider.Phase == v1alpha1.ManagedFlowPhaseActive {
		if cr.Status.AtProvider.LatestRegistryVersion > 0 {
			// Assume we're on the latest version (we imported latest during Create).
			// This establishes a baseline so future versions can be detected.
			cr.Status.AtProvider.CurrentVersion = cr.Status.AtProvider.LatestRegistryVersion
		} else {
			// No LatestRegistryVersion either — try resolving from registry directly
			latestVer, latestErr := e.nifi.GetLatestFlowVersion(p.RegistryID, p.BucketID, p.FlowID, p.Branch)
			if latestErr == nil && latestVer > 0 {
				cr.Status.AtProvider.CurrentVersion = latestVer
			}
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

	// Best-effort cleanup of orphaned old PG from a previous cutover that failed to delete.
	// This runs every Observe cycle until the old PG is successfully removed.
	if cr.Status.AtProvider.PreviousProcessGroupID != "" {
		if err := e.stopAndDeletePG(cr.Status.AtProvider.PreviousProcessGroupID); err == nil {
			cr.Status.AtProvider.PreviousProcessGroupID = ""
		}
		// If it fails again, we'll retry on the next Observe — no error returned.
	}

	phase := cr.Status.AtProvider.Phase

	// If spec changed (e.g. version bump) while in a transitional phase from the initial create
	// (no pending PG = not mid blue-green rollout), reset to Active so Update starts a proper rollout.
	if isTransitionalPhase(phase) && cr.Status.AtProvider.PendingProcessGroupID == "" {
		if specChanged(cr) {
			cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive
			cr.Status.AtProvider.Message = "Spec changed during initial rollout, restarting"
			cr.Status.AtProvider.HealthCheckStartTime = nil
			phase = v1alpha1.ManagedFlowPhaseActive
		}
	}

	// If phase is transitional, signal that Update needs to run
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

	// 1. Resolve the flow version to import.
	// We need both:
	//   - importVersion (int32): for internal version tracking / auto-update comparison
	//   - versionRaw (string): the actual value to pass to NiFi (commit SHA for Git, integer string for traditional)
	importVersion := p.FlowVersion
	var versionRaw string
	if importVersion <= 0 {
		latestNum, latestRaw, latestErr := e.nifi.GetLatestFlowVersionInfo(p.RegistryID, p.BucketID, p.FlowID, p.Branch)
		if latestErr == nil && latestNum > 0 {
			importVersion = latestNum
			versionRaw = latestRaw // commit SHA for Git registries, integer string for traditional
		}
	}

	result, err := e.nifi.ImportFlowFromRegistry(
		p.ParentGroupID, p.RegistryID, p.BucketID, p.FlowID, importVersion, p.Branch, versionRaw, position,
	)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot import flow from registry")
	}

	meta.SetExternalName(cr, result.Id)
	cr.Status.AtProvider.ActiveProcessGroupID = result.Id
	if result.Revision != nil && result.Revision.Version != nil {
		cr.Status.AtProvider.Version = *result.Revision.Version
	}
	// Track the imported flow version directly (don't rely on VCI which may return
	// commit SHAs for Git registries instead of integer versions).
	if importVersion > 0 {
		cr.Status.AtProvider.CurrentVersion = importVersion
	}

	// 2. Rename PG to match the ManagedFlow resource name
	if err := e.renamePG(result.Id, cr.Name, cr.Status.AtProvider.Version); err != nil {
		// Non-fatal: PG works fine with default name
		cr.Status.AtProvider.Message = fmt.Sprintf("Warning: could not rename PG: %v", err)
	} else {
		// Version increments after rename
		pg, err := e.nifi.GetProcessGroup(result.Id)
		if err == nil && pg.Revision != nil && pg.Revision.Version != nil {
			cr.Status.AtProvider.Version = *pg.Revision.Version
		}
	}

	// 3. Create inline parameter context if specified, or assign existing one.
	// IMPORTANT: After PG is created and external name is set, ALL errors are non-fatal.
	// If Create returns an error after import, Crossplane may not persist the external name,
	// causing Create to run again on the next reconcile and creating duplicate PGs.
	// The state machine will retry these steps on subsequent Update cycles.
	paramCtxID, err := e.resolveParameterContext(ctx, cr)
	if err != nil {
		cr.Status.AtProvider.Message = fmt.Sprintf("Warning: could not resolve parameter context (will retry): %v", err)
	}
	if paramCtxID != "" && err == nil {
		if assignErr := e.assignParameterContext(result.Id, paramCtxID, cr.Status.AtProvider.Version); assignErr != nil {
			cr.Status.AtProvider.Message = fmt.Sprintf("Warning: could not assign parameter context (will retry): %v", assignErr)
		}
	}

	// 4. Enable all controller services (batch API)
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
	case "":
		// Phase is empty — this means the resource exists but has never completed the initial
		// state machine (e.g., Create's status write was lost). Resume the initial setup
		// instead of triggering a blue-green rollout (which would create a DUPLICATE PG).
		return e.enableServicesOnTarget(ctx, cr)

	case v1alpha1.ManagedFlowPhaseActive, v1alpha1.ManagedFlowPhaseFailed:
		// Check if this is only a desiredState change (start/stop) — handle without rollout
		if isOnlyStateChange(cr) {
			return e.doStateChange(ctx, cr)
		}

		// All other spec changes (flow version, parameters, etc.) trigger blue-green rollout
		if strategy == v1alpha1.RolloutStrategyBlueGreen {
			return e.startBlueGreenRollout(ctx, cr)
		}
		// InPlace strategy — change flow version directly (no parameter rotation needed)
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
		// Rollback parameter context: delete new one, restore old one
		e.rollbackParameterContext(cr)
		cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseFailed
		cr.Status.AtProvider.FailedFlowVersion = cr.Spec.ForProvider.FlowVersion
		cr.Status.AtProvider.FailedGeneration = cr.Generation
		cr.Status.AtProvider.Message = fmt.Sprintf("Rollback completed: version %d had errors, old PG is still active. Change spec to retry.", cr.Spec.ForProvider.FlowVersion)
		cr.Status.AtProvider.PendingProcessGroupID = ""
		cr.Status.AtProvider.PendingFlowVersion = 0
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

	// Delete orphaned old PG from failed cutover cleanup
	if cr.Status.AtProvider.PreviousProcessGroupID != "" {
		_ = e.stopAndDeletePG(cr.Status.AtProvider.PreviousProcessGroupID)
	}

	// Delete managed parameter contexts (current and previous if mid-rotation)
	for _, pcID := range []string{cr.Status.AtProvider.ParameterContextID, cr.Status.AtProvider.PreviousParameterContextID} {
		if pcID == "" {
			continue
		}
		pc, err := e.nifi.GetParameterContext(pcID)
		if err == nil && pc.Revision != nil && pc.Revision.Version != nil {
			_ = e.nifi.DeleteParameterContext(pcID, *pc.Revision.Version)
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

	// Determine which version to import.
	// We need both importVersion (int32 for tracking) and versionRaw (string for NiFi API).
	importVersion := p.FlowVersion
	var versionRaw string
	if importVersion <= 0 && cr.Status.AtProvider.LatestRegistryVersion > 0 {
		importVersion = cr.Status.AtProvider.LatestRegistryVersion
	}
	// Always resolve the raw version string for Git registries (need commit SHA for import)
	latestNum, latestRaw, latestErr := e.nifi.GetLatestFlowVersionInfo(p.RegistryID, p.BucketID, p.FlowID, p.Branch)
	if latestErr == nil {
		versionRaw = latestRaw
		if importVersion <= 0 && latestNum > 0 {
			importVersion = latestNum
		}
	}

	// SAFEGUARD: Don't create a duplicate PG if we're already on the target version
	// and parameters haven't changed. This prevents spurious rollouts triggered by
	// non-rollout spec changes (position, desiredState) or transient state mismatches.
	if importVersion > 0 && importVersion == cr.Status.AtProvider.CurrentVersion {
		currentHash := computeParameterHash(p.ParameterContext)
		if cr.Status.AtProvider.LastAppliedParameterHash == "" || currentHash == cr.Status.AtProvider.LastAppliedParameterHash {
			// Same version AND same parameters — nothing to roll out
			cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive
			cr.Status.AtProvider.LastAppliedGeneration = cr.Generation
			cr.Status.AtProvider.LastAppliedParameterHash = currentHash
			cr.Status.AtProvider.Message = fmt.Sprintf("Already on version %d with matching parameters, skipping rollout", importVersion)
			return managed.ExternalUpdate{}, nil
		}
	}

	// Import new version as a separate PG
	newPG, err := e.nifi.ImportFlowFromRegistry(
		p.ParentGroupID, p.RegistryID, p.BucketID, p.FlowID, importVersion, p.Branch, versionRaw, position,
	)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot import new flow version for blue-green rollout")
	}

	cr.Status.AtProvider.PendingProcessGroupID = newPG.Id
	cr.Status.AtProvider.PendingFlowVersion = importVersion // Track the integer version we imported (VCI may return commit SHA for Git registries)
	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseImporting
	cr.Status.AtProvider.FailedFlowVersion = 0 // Clear any previous failure
	cr.Status.AtProvider.FailedGeneration = 0
	cr.Status.AtProvider.Message = fmt.Sprintf("Blue-green rollout: imported new PG %s (version %d)", newPG.Id, importVersion)

	// Rename new PG to match ManagedFlow name with version suffix
	var newPGVersion int64
	if newPG.Revision != nil && newPG.Revision.Version != nil {
		newPGVersion = *newPG.Revision.Version
	}
	pendingName := fmt.Sprintf("%s-v%d", cr.Name, importVersion)
	if err := e.renamePG(newPG.Id, pendingName, newPGVersion); err != nil {
		cr.Status.AtProvider.Message = fmt.Sprintf("Blue-green rollout: imported new PG %s (rename failed: %v)", newPG.Id, err)
	}

	// Assign parameter context to new PG (inline or by ID)
	paramCtxID, err := e.resolveParameterContext(ctx, cr)
	if err != nil {
		_ = e.stopAndDeletePG(newPG.Id)
		cr.Status.AtProvider.PendingProcessGroupID = ""
		cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive // Reset phase so next reconcile can retry
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot resolve parameter context for new PG")
	}
	if paramCtxID != "" {
		// Refresh version — rename may have incremented it
		refreshedPG, err := e.nifi.GetProcessGroup(newPG.Id)
		if err != nil {
			_ = e.stopAndDeletePG(newPG.Id)
			cr.Status.AtProvider.PendingProcessGroupID = ""
			cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot refresh PG version after rename")
		}
		var version int64
		if refreshedPG.Revision != nil && refreshedPG.Revision.Version != nil {
			version = *refreshedPG.Revision.Version
		}
		if err := e.assignParameterContext(newPG.Id, paramCtxID, version); err != nil {
			_ = e.stopAndDeletePG(newPG.Id)
			cr.Status.AtProvider.PendingProcessGroupID = ""
			cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive // Reset phase so next reconcile can retry
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

	hasErrors := false
	var errorMsg string

	// Check 1: Verify all processors are running and valid (not just bulletins)
	desiredState := cr.Spec.ForProvider.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}
	if desiredState == "RUNNING" {
		processors, err := e.nifi.GetProcessors(targetID)
		if err != nil {
			cr.Status.AtProvider.Message = fmt.Sprintf("Cannot list processors for health check: %v", err)
			return managed.ExternalUpdate{}, nil
		}
		for _, proc := range processors {
			if proc.Component == nil {
				continue
			}
			// Check for INVALID validation status
			if strings.EqualFold(proc.Component.ValidationStatus, "INVALID") {
				hasErrors = true
				validationErrs := strings.Join(proc.Component.ValidationErrors, "; ")
				errorMsg = fmt.Sprintf("Processor %s is INVALID: %s", proc.Component.Name, validationErrs)
				break
			}
			// Check that processors are actually running (not stopped/disabled)
			if !strings.EqualFold(proc.Component.State, "RUNNING") {
				hasErrors = true
				errorMsg = fmt.Sprintf("Processor %s is not running (state: %s)", proc.Component.Name, proc.Component.State)
				break
			}
		}
	}

	// Check 2: Error bulletins
	if !hasErrors {
		board, err := e.nifi.GetBulletinBoard(targetID)
		if err != nil {
			cr.Status.AtProvider.Message = fmt.Sprintf("Cannot check bulletins: %v", err)
			return managed.ExternalUpdate{}, nil
		}
		if board.BulletinBoard != nil {
			for _, b := range board.BulletinBoard.Bulletins {
				if b.Bulletin != nil && strings.EqualFold(b.Bulletin.Level, "ERROR") {
					hasErrors = true
					errorMsg = fmt.Sprintf("Error bulletin: %s", b.Bulletin.Message)
					break
				}
			}
		}
	}

	if hasErrors {
		cr.Status.AtProvider.Message = errorMsg
	}

	// Check if we've exceeded the max health check duration
	healthCheckElapsed := time.Duration(0)
	if cr.Status.AtProvider.HealthCheckStartTime != nil {
		healthCheckElapsed = time.Since(cr.Status.AtProvider.HealthCheckStartTime.Time)
	}

	if hasErrors && isBlueGreen {
		// Rollback: delete pending PG
		_ = e.stopAndDeletePG(cr.Status.AtProvider.PendingProcessGroupID)
		cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseRollingBack
		cr.Status.AtProvider.Message = "Error detected during health check, rolling back"
		return managed.ExternalUpdate{}, nil
	}

	if hasErrors {
		// Not blue-green (initial create) — check if we've been trying too long
		if healthCheckElapsed >= maxHealthCheckDuration {
			cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseFailed
			cr.Status.AtProvider.FailedFlowVersion = cr.Spec.ForProvider.FlowVersion
			cr.Status.AtProvider.FailedGeneration = cr.Generation
			cr.Status.AtProvider.Message = fmt.Sprintf("Health check failed: errors persisted for %s. Change spec to retry.", healthCheckElapsed.Round(time.Second))
			cr.Status.AtProvider.HealthCheckStartTime = nil
			return managed.ExternalUpdate{}, nil
		}
		cr.Status.AtProvider.Message = fmt.Sprintf("Error bulletins detected (%s elapsed), waiting for resolution", healthCheckElapsed.Round(time.Second))
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

		// Stop only input processors (no incoming connections) to allow data to drain through
		_ = e.stopInputProcessors(cr.Status.AtProvider.ActiveProcessGroupID)
	} else {
		// Initial create — we're done
		cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive
		cr.Status.AtProvider.LastAppliedGeneration = cr.Generation
		cr.Status.AtProvider.LastAppliedParameterHash = computeParameterHash(cr.Spec.ForProvider.ParameterContext)
		cr.Status.AtProvider.Message = "Flow is active and healthy"
		cr.Status.AtProvider.HealthCheckStartTime = nil
		now := metav1.Now()
		cr.Status.AtProvider.LastRolloutTime = &now
		// Ensure CurrentVersion is set — it may have been lost if Create's status write failed.
		// This is the last chance to set it before the flow goes Active.
		if cr.Status.AtProvider.CurrentVersion == 0 {
			p := cr.Spec.ForProvider
			latestVer, latestErr := e.nifi.GetLatestFlowVersion(p.RegistryID, p.BucketID, p.FlowID, p.Branch)
			if latestErr == nil && latestVer > 0 {
				cr.Status.AtProvider.CurrentVersion = latestVer
			}
		}
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

	// Swap external name to the new PG BEFORE deleting the old one.
	// This ensures that even if the subsequent delete or status write fails,
	// the next reconcile's Observe will find the new PG (not the deleted old one).
	meta.SetExternalName(cr, newPGID)
	cr.Status.AtProvider.ActiveProcessGroupID = newPGID
	cr.Status.AtProvider.PendingProcessGroupID = ""

	// Delete old PG — best-effort since external name already points to new PG.
	// If this fails (e.g., services still disabling), we still complete the cutover.
	// The old PG becomes orphaned but the new PG is safely active.
	if err := e.stopAndDeletePG(oldPGID); err != nil {
		cr.Status.AtProvider.Message = fmt.Sprintf("Cutover complete but old PG %s cleanup failed (will retry): %v", oldPGID, err)
		// Store old PG ID so we can retry cleanup on next reconcile
		cr.Status.AtProvider.PreviousProcessGroupID = oldPGID
	}

	// Set CurrentVersion from PendingFlowVersion — this is the integer version we tracked
	// at import time. We do NOT rely on VCI here because Git-based registries return commit
	// SHAs (not integers) in VCI's Version field, which breaks int32 version tracking.
	if cr.Status.AtProvider.PendingFlowVersion > 0 {
		cr.Status.AtProvider.CurrentVersion = cr.Status.AtProvider.PendingFlowVersion
		cr.Status.AtProvider.PendingFlowVersion = 0 // Clear after cutover
	} else if cr.Spec.ForProvider.FlowVersion > 0 {
		// Fallback: trust the spec value — we imported exactly this version
		cr.Status.AtProvider.CurrentVersion = cr.Spec.ForProvider.FlowVersion
	}

	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseActive
	cr.Status.AtProvider.LastAppliedGeneration = cr.Generation
	cr.Status.AtProvider.LastAppliedParameterHash = computeParameterHash(cr.Spec.ForProvider.ParameterContext)
	cr.Status.AtProvider.Message = "Blue-green rollout completed successfully"

	// Clean up old parameter context after successful cutover
	e.cleanupOldParameterContext(cr)

	// Rename the new active PG to the ManagedFlow name (remove version suffix)
	newPG, renameErr := e.nifi.GetProcessGroup(newPGID)
	if renameErr == nil && newPG.Revision != nil && newPG.Revision.Version != nil {
		_ = e.renamePG(newPGID, cr.Name, *newPG.Revision.Version)
	}
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

	cr.Status.AtProvider.Phase = v1alpha1.ManagedFlowPhaseEnablingServices
	cr.Status.AtProvider.Message = "In-place version update, re-enabling services"
	return managed.ExternalUpdate{}, nil
}

// doStateChange handles only desiredState changes (start/stop) without triggering a rollout.
func (e *external) doStateChange(ctx context.Context, cr *v1alpha1.ManagedFlow) (managed.ExternalUpdate, error) {
	externalName := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider

	desiredState := p.DesiredState
	if desiredState == "" {
		desiredState = "STOPPED"
	}

	pg, err := e.nifi.GetProcessGroup(externalName)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot get process group for state change")
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
	cr.Status.AtProvider.LastAppliedGeneration = cr.Generation
	cr.Status.AtProvider.LastAppliedParameterHash = computeParameterHash(cr.Spec.ForProvider.ParameterContext)
	cr.Status.AtProvider.Message = "State change completed"
	return managed.ExternalUpdate{}, nil
}

// isOnlyStateChange returns true when the only meaningful change is desiredState
// (RUNNING/STOPPED) or non-rollout fields (position, rollout config).
// Only flow version changes and parameter changes should trigger a rollout.
func isOnlyStateChange(cr *v1alpha1.ManagedFlow) bool {
	p := cr.Spec.ForProvider

	// If flow version changed → rollout needed
	if p.FlowVersion > 0 && cr.Status.AtProvider.CurrentVersion != p.FlowVersion {
		return false
	}

	// If auto-update detected a newer version in registry → rollout needed
	if p.AutoUpdate && cr.Status.AtProvider.LatestRegistryVersion > 0 &&
		cr.Status.AtProvider.CurrentVersion > 0 &&
		cr.Status.AtProvider.LatestRegistryVersion > cr.Status.AtProvider.CurrentVersion {
		return false
	}

	// If inline parameters changed → rollout needed
	// Compare a hash of the current parameters with the last applied hash.
	// This avoids false positives from position/desiredState/config changes
	// (which also bump metadata.generation).
	if p.ParameterContext != nil {
		currentHash := computeParameterHash(p.ParameterContext)
		if cr.Status.AtProvider.LastAppliedParameterHash != "" && currentHash != cr.Status.AtProvider.LastAppliedParameterHash {
			return false
		}
		// If LastAppliedParameterHash is empty (first deploy), let the state machine handle it
		if cr.Status.AtProvider.LastAppliedParameterHash == "" && cr.Status.AtProvider.LastAppliedGeneration == 0 {
			return false
		}
	}

	// If parameterContextId (external reference) changed → rollout needed
	if p.ParameterContextID != "" && cr.Status.AtProvider.ParameterContextID != "" && p.ParameterContextID != cr.Status.AtProvider.ParameterContextID {
		return false
	}

	return true
}

// computeParameterHash returns a stable hash of the inline parameter context config.
// Used to detect actual parameter changes without relying on metadata.generation
// (which also increments for position, desiredState, and other non-rollout fields).
func computeParameterHash(pc *v1alpha1.ParameterContextConfig) string {
	if pc == nil {
		return ""
	}
	data, err := json.Marshal(pc)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:8])
}

// --- Helpers ---

// resolveParameterContext returns the parameter context ID to assign to a PG.
// For inline parameter contexts, it creates a new one with a generation-suffixed name
// to avoid conflicts with any existing context (blue-green rollout runs both PGs in parallel).
// After successful cutover, the old parameter context is cleaned up.
// If parameterContextId is specified, it returns that ID directly.
func (e *external) resolveParameterContext(ctx context.Context, cr *v1alpha1.ManagedFlow) (string, error) {
	p := cr.Spec.ForProvider

	// Inline parameter context takes precedence
	if p.ParameterContext != nil {
		return e.createParameterContextForGeneration(ctx, cr)
	}

	// Direct ID reference
	if p.ParameterContextID != "" {
		return p.ParameterContextID, nil
	}

	return "", nil
}

// createParameterContextForGeneration creates a new parameter context with a generation-suffixed
// name (e.g., "my-params-gen5"). If one with that name already exists AND is not the current
// active context, it reuses it (idempotency for retries). If the existing one IS the active
// context (same generation used for both Create and rollout), it creates a separate one with
// a "-bg" suffix so the old and new PGs don't share the same parameter context.
func (e *external) createParameterContextForGeneration(ctx context.Context, cr *v1alpha1.ManagedFlow) (string, error) {
	cfg := cr.Spec.ForProvider.ParameterContext
	oldParamCtxID := cr.Status.AtProvider.ParameterContextID

	// Use generation-suffixed name to avoid conflicts during blue-green
	newName := fmt.Sprintf("%s-gen%d", cfg.Name, cr.Generation)

	// Check if a parameter context with this name already exists (idempotency)
	existingID, err := e.findParameterContextByName(newName)
	if err == nil && existingID != "" {
		if existingID == oldParamCtxID {
			// The existing context is the one already assigned to the active PG.
			// We need a separate one for the new PG during blue-green rollout.
			newName = fmt.Sprintf("%s-gen%d-bg", cfg.Name, cr.Generation)
			// Check if the -bg variant already exists (retry idempotency)
			bgID, bgErr := e.findParameterContextByName(newName)
			if bgErr == nil && bgID != "" {
				cr.Status.AtProvider.ParameterContextID = bgID
				cr.Status.AtProvider.PreviousParameterContextID = oldParamCtxID
				return bgID, nil
			}
		} else {
			// Different from active — safe to reuse (previous failed rollout attempt)
			cr.Status.AtProvider.ParameterContextID = existingID
			if oldParamCtxID != "" {
				cr.Status.AtProvider.PreviousParameterContextID = oldParamCtxID
			}
			return existingID, nil
		}
	}

	entity, buildErr := e.buildInlineParameterContextEntity(ctx, cfg, cr.Namespace)
	if buildErr != nil {
		return "", errors.Wrap(buildErr, "cannot build parameter context entity")
	}
	entity.Component.Name = newName

	newPC, err := e.nifi.CreateParameterContext(entity)
	if err != nil {
		return "", errors.Wrap(err, "cannot create parameter context")
	}

	cr.Status.AtProvider.ParameterContextID = newPC.Id
	if oldParamCtxID != "" && oldParamCtxID != newPC.Id {
		cr.Status.AtProvider.PreviousParameterContextID = oldParamCtxID
	}

	return newPC.Id, nil
}

// cleanupOldParameterContext deletes the previous parameter context after a successful rotation.
func (e *external) cleanupOldParameterContext(cr *v1alpha1.ManagedFlow) {
	oldID := cr.Status.AtProvider.PreviousParameterContextID
	if oldID == "" {
		return
	}
	pc, err := e.nifi.GetParameterContext(oldID)
	if err != nil {
		// Already gone or inaccessible
		cr.Status.AtProvider.PreviousParameterContextID = ""
		return
	}
	var version int64
	if pc.Revision != nil && pc.Revision.Version != nil {
		version = *pc.Revision.Version
	}
	_ = e.nifi.DeleteParameterContext(oldID, version)
	cr.Status.AtProvider.PreviousParameterContextID = ""
}

// rollbackParameterContext deletes the new parameter context and restores the old one.
func (e *external) rollbackParameterContext(cr *v1alpha1.ManagedFlow) {
	newID := cr.Status.AtProvider.ParameterContextID
	oldID := cr.Status.AtProvider.PreviousParameterContextID
	if oldID == "" || newID == "" {
		return
	}
	// Delete the new (failed) parameter context
	pc, err := e.nifi.GetParameterContext(newID)
	if err == nil {
		var version int64
		if pc.Revision != nil && pc.Revision.Version != nil {
			version = *pc.Revision.Version
		}
		_ = e.nifi.DeleteParameterContext(newID, version)
	}
	// Restore old ID
	cr.Status.AtProvider.ParameterContextID = oldID
	cr.Status.AtProvider.PreviousParameterContextID = ""
}

func (e *external) findParameterContextByName(name string) (string, error) {
	contexts, err := e.nifi.GetParameterContexts()
	if err != nil {
		return "", err
	}
	if contexts.ParameterContexts != nil {
		for _, pc := range contexts.ParameterContexts {
			if pc.Component != nil && pc.Component.Name == name {
				return pc.Id, nil
			}
		}
	}
	return "", nil
}

func (e *external) buildInlineParameterContextEntity(ctx context.Context, cfg *v1alpha1.ParameterContextConfig, namespace string) (nigoapi.ParameterContextEntity, error) {
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
			// Resolve value: valueFromSecret takes precedence over inline value
			val, err := resolveParameterValue(ctx, e.kube, param, namespace)
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

// renamePG renames a process group to the given name.
func (e *external) renamePG(pgID, name string, version int64) error {
	entity := nigoapi.ProcessGroupEntity{
		Id: pgID,
		Revision: &nigoapi.RevisionDto{
			Version: &version,
		},
		Component: &nigoapi.ProcessGroupDto{
			Id:   pgID,
			Name: name,
		},
	}
	_, err := e.nifi.UpdateProcessGroup(entity)
	return err
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

	// Check if PG exists first
	_, err := e.nifi.GetProcessGroup(pgID)
	if err != nil {
		if nificlient.IsNotFound(err) {
			return nil // Already gone
		}
		return errors.Wrap(err, "cannot get process group for deletion")
	}

	// Stop all processors first (ignore errors — some may already be stopped or invalid)
	_ = e.nifi.ScheduleProcessGroup(pgID, "STOPPED")

	// Disable all controller services before deletion.
	// NiFi disables services asynchronously, so we must wait for them to actually become DISABLED.
	_ = e.nifi.ActivateControllerServicesInGroup(pgID, "DISABLED")

	// Wait for all services to be fully disabled (up to 30 seconds)
	for attempt := 0; attempt < 15; attempt++ {
		allDisabled := true
		services, svcErr := e.nifi.ListControllerServicesInGroup(pgID)
		if svcErr != nil {
			break // Can't check — proceed with delete attempt anyway
		}
		for _, svc := range services {
			if svc.Component != nil && !strings.EqualFold(svc.Component.State, "DISABLED") {
				allDisabled = false
				break
			}
		}
		if allDisabled {
			break
		}
		time.Sleep(2 * time.Second)
		// Re-request disable in case some services have dependencies that are now disabled
		_ = e.nifi.ActivateControllerServicesInGroup(pgID, "DISABLED")
	}

	// Drop all queued FlowFiles to allow deletion of PG with queued data
	_ = e.nifi.EmptyAllConnectionsInGroup(pgID)

	// Refresh version after state changes (version increments on stop/disable)
	pg, err := e.nifi.GetProcessGroup(pgID)
	if err != nil {
		if nificlient.IsNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "cannot refresh process group version for deletion")
	}

	var version int64
	if pg.Revision != nil && pg.Revision.Version != nil {
		version = *pg.Revision.Version
	}

	err = e.nifi.DeleteProcessGroup(pgID, version)
	if err != nil && nificlient.IsNotFound(err) {
		return nil // Race condition: already deleted
	}
	return err
}

// stopInputProcessors stops only the root/input processors (those with no incoming connections)
// in the given process group. This allows data already in the pipeline to drain through.
func (e *external) stopInputProcessors(pgID string) error {
	// Get all connections to find which processors have incoming connections
	connections, err := e.nifi.GetConnections(pgID)
	if err != nil {
		// Fallback: stop the entire PG if we can't determine input processors
		return e.nifi.ScheduleProcessGroup(pgID, "STOPPED")
	}

	// Build set of processor IDs that have incoming connections (destination IDs)
	hasIncoming := make(map[string]bool)
	for _, conn := range connections {
		if conn.Component != nil && conn.Component.Destination != nil {
			hasIncoming[conn.Component.Destination.Id] = true
		}
	}

	// Get all processors and stop only those with no incoming connections
	processors, err := e.nifi.GetProcessors(pgID)
	if err != nil {
		return e.nifi.ScheduleProcessGroup(pgID, "STOPPED")
	}

	for _, proc := range processors {
		if proc.Component == nil {
			continue
		}
		if !hasIncoming[proc.Id] && strings.EqualFold(proc.Component.State, "RUNNING") {
			var version int64
			if proc.Revision != nil && proc.Revision.Version != nil {
				version = *proc.Revision.Version
			}
			_ = e.nifi.StopProcessor(proc.Id, version)
		}
	}

	return nil
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

// specChanged returns true if a rollout-worthy spec field changed (flow version or parameters).
// Used to detect changes during transitional phases. Does NOT trigger on position/state changes.
func specChanged(cr *v1alpha1.ManagedFlow) bool {
	p := cr.Spec.ForProvider
	if p.FlowVersion > 0 && cr.Status.AtProvider.CurrentVersion != p.FlowVersion {
		return true
	}
	if p.ParameterContext != nil && cr.Status.AtProvider.LastAppliedParameterHash != "" {
		if computeParameterHash(p.ParameterContext) != cr.Status.AtProvider.LastAppliedParameterHash {
			return true
		}
	}
	return false
}

func isManagedFlowUpToDate(cr *v1alpha1.ManagedFlow, pg *nigoapi.ProcessGroupEntity) bool {
	p := cr.Spec.ForProvider

	// Failed phase: stop retrying unless the user changed the spec (any spec change bumps generation)
	if cr.Status.AtProvider.Phase == v1alpha1.ManagedFlowPhaseFailed {
		// If failedGeneration was recorded, use it to detect spec changes
		if cr.Status.AtProvider.FailedGeneration > 0 {
			if cr.Generation != cr.Status.AtProvider.FailedGeneration {
				return false // User changed spec since failure — retry
			}
			return true // Same spec that failed — don't retry
		}
		// Backwards compat: failedGeneration not set (old binary), use observedGeneration from Synced condition
		// If the Synced condition's observedGeneration matches current generation, spec hasn't changed
		for _, c := range cr.Status.Conditions {
			if c.Type == "Synced" && c.ObservedGeneration == cr.Generation {
				return true // Already observed this generation while in Failed — don't retry
			}
		}
		return false // Generation not yet observed — retry
	}

	if cr.Status.AtProvider.Phase != v1alpha1.ManagedFlowPhaseActive {
		return false
	}

	// Check flow version change (explicit version pinning)
	if p.FlowVersion > 0 && cr.Status.AtProvider.CurrentVersion != p.FlowVersion {
		return false
	}

	// Auto-update: if enabled and a newer version exists in the registry, trigger rollout.
	// This allows the controller to automatically deploy new versions pushed to the registry
	// (e.g., when someone pushes a new commit to main in a Git-based registry).
	if p.AutoUpdate && cr.Status.AtProvider.LatestRegistryVersion > 0 &&
		cr.Status.AtProvider.CurrentVersion > 0 &&
		cr.Status.AtProvider.LatestRegistryVersion > cr.Status.AtProvider.CurrentVersion {
		return false
	}

	// Check inline parameter changes via hash (not generation — generation also
	// increments for position, desiredState, rollout config, etc.)
	if p.ParameterContext != nil && cr.Status.AtProvider.LastAppliedParameterHash != "" {
		if computeParameterHash(p.ParameterContext) != cr.Status.AtProvider.LastAppliedParameterHash {
			return false
		}
	}

	// Check external parameter context ID change
	if p.ParameterContextID != "" && cr.Status.AtProvider.ParameterContextID != "" && p.ParameterContextID != cr.Status.AtProvider.ParameterContextID {
		return false
	}

	// Check desiredState
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
