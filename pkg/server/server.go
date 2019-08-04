package server

import (
	"context"
	"net/http"

	"github.com/rancher/norman/pkg/store/proxy"

	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/client"
	"github.com/rancher/naok/pkg/schemas"
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
	Kubeconfig string
	Namespace  string
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

	starter, err := startAPI(ctx, restConfig, k8s, crd, api, rbac)
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

func startAPI(ctx context.Context, restConfig *rest.Config, k8s *kubernetes.Clientset, crd *apiextensions.Factory,
	api *apiregistration.Factory, rbac *rbaccontroller.Factory) (func() error, error) {

	cf, err := client.NewFactory(restConfig)
	if err != nil {
		return nil, err
	}

	sf := schemas.Register(ctx,
		cf,
		k8s.Discovery(),
		crd.Apiextensions().V1beta1().CustomResourceDefinition(),
		api.Apiregistration().V1().APIService(),
	)

	accessStore := accesscontrol.NewAccessStore(rbac.Rbac().V1())

	return func() error {
		return serve(cf, accessStore, sf)
	}, nil
}

func serve(cf proxy.ClientGetter, as *accesscontrol.AccessStore, sf schemas.SchemaFactory) error {
	logrus.Infof("listening on :8989")
	return http.ListenAndServe(":8989", newAPIServer(cf, as, sf))
}
