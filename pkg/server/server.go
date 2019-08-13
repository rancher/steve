package server

import (
	"context"
	"net/http"

	"github.com/rancher/naok/pkg/resources"

	"github.com/rancher/naok/pkg/controllers/schema"

	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/client"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/apiextensions.k8s.io"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/apiregistration.k8s.io"
	rbaccontroller "github.com/rancher/wrangler-api/pkg/generated/controllers/rbac"
	"github.com/rancher/wrangler/pkg/kubeconfig"
	"github.com/rancher/wrangler/pkg/start"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

	starter, err := startAPI(ctx, cfg.ListenAddress, restConfig, k8s, crd, api, rbac)
	if err != nil {
		return err
	}

	if err := start.All(ctx, 5, api, crd, rbac); err != nil {
		return err
	}

	if err := starter(); err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}

func startAPI(ctx context.Context, listenAddress string, restConfig *rest.Config, k8s *kubernetes.Clientset, crd *apiextensions.Factory,
	api *apiregistration.Factory, rbac *rbaccontroller.Factory) (func() error, error) {

	cf, err := client.NewFactory(restConfig)
	if err != nil {
		return nil, err
	}

	as := accesscontrol.NewAccessStore(rbac.Rbac().V1())
	sf := resources.SchemaFactory(cf, as)

	schema.Register(ctx,
		k8s.Discovery(),
		crd.Apiextensions().V1beta1().CustomResourceDefinition(),
		api.Apiregistration().V1().APIService(),
		sf)

	return func() error {
		handler, err := newAPIServer(restConfig, sf)
		if err != nil {
			return err
		}
		logrus.Infof("listening on %s", listenAddress)
		return http.ListenAndServe(listenAddress, handler)
	}, nil
}
