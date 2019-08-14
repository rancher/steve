package server

import (
	"context"
	"net/http"

	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/client"
	"github.com/rancher/naok/pkg/controllers/schema"
	"github.com/rancher/naok/pkg/resources"
	"github.com/rancher/naok/pkg/server/publicapi"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/apiextensions.k8s.io"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/apiregistration.k8s.io"
	rbaccontroller "github.com/rancher/wrangler-api/pkg/generated/controllers/rbac"
	"github.com/rancher/wrangler/pkg/kubeconfig"
	"github.com/rancher/wrangler/pkg/start"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
)

type Config struct {
	Kubeconfig    string
	Namespace     string
	ListenAddress string
}

func Run(ctx context.Context, cfg Config) error {
	restConfig, err := kubeconfig.GetNonInteractiveClientConfig(cfg.Kubeconfig).ClientConfig()
	if err != nil {
		return err
	}

	rbac, err := rbaccontroller.NewFactoryFromConfig(restConfig)
	if err != nil {
		return err
	}

	k8s, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	api, err := apiregistration.NewFactoryFromConfig(restConfig)
	if err != nil {
		return err
	}

	crd, err := apiextensions.NewFactoryFromConfig(restConfig)
	if err != nil {
		return err
	}

	cf, err := client.NewFactory(restConfig)
	if err != nil {
		return err
	}

	sf := resources.SchemaFactory(cf,
		accesscontrol.NewAccessStore(rbac.Rbac().V1()),
		k8s)

	schema.Register(ctx,
		k8s.Discovery(),
		crd.Apiextensions().V1beta1().CustomResourceDefinition(),
		api.Apiregistration().V1().APIService(),
		sf)

	handler, err := publicapi.NewHandler(restConfig, sf)
	if err != nil {
		return err
	}

	if err := start.All(ctx, 5, api, crd, rbac); err != nil {
		return err
	}

	logrus.Infof("listening on %s", cfg.ListenAddress)
	return http.ListenAndServe(cfg.ListenAddress, handler)
}
