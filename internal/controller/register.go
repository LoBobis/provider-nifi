package controller

import (
	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/crossplane-contrib/provider-nifi/internal/controller/config"
	"github.com/crossplane-contrib/provider-nifi/internal/controller/connection"
	"github.com/crossplane-contrib/provider-nifi/internal/controller/controllerservice"
	"github.com/crossplane-contrib/provider-nifi/internal/controller/parametercontext"
	"github.com/crossplane-contrib/provider-nifi/internal/controller/processgroup"
	"github.com/crossplane-contrib/provider-nifi/internal/controller/processor"
	"github.com/crossplane-contrib/provider-nifi/internal/controller/managedflow"
	"github.com/crossplane-contrib/provider-nifi/internal/controller/registryflow"
)

// SetupGated creates all NiFi controllers with safe-start support and adds them to
// the supplied manager.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	for _, setup := range []func(ctrl.Manager, controller.Options) error{
		config.Setup,
		processor.SetupGated,
		processgroup.SetupGated,
		connection.SetupGated,
		controllerservice.SetupGated,
		parametercontext.SetupGated,
		registryflow.SetupGated,
		managedflow.SetupGated,
	} {
		if err := setup(mgr, o); err != nil {
			return err
		}
	}
	return nil
}
