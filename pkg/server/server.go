package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/rancher/dynamiclistener/server"
	"github.com/rancher/dynamiclistener/storage/kubernetes"
	"github.com/rancher/dynamiclistener/storage/memory"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/client"
	"github.com/rancher/steve/pkg/clustercache"
	schemacontroller "github.com/rancher/steve/pkg/controllers/schema"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/steve/pkg/schemaserver/types"
	"github.com/rancher/steve/pkg/server/handler"
	"github.com/rancher/steve/pkg/server/resources"
	v1 "github.com/rancher/wrangler-api/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/pkg/start"
)

var ErrConfigRequired = errors.New("rest config is required")

func setDefaults(server *Server) error {
	if server.RestConfig == nil {
		return ErrConfigRequired
	}

	if server.Namespace == "" {
		server.Namespace = "steve"
	}

	if server.Controllers == nil {
		var err error
		server.Controllers, err = NewController(server.RestConfig)
		if err != nil {
			return err
		}
	}

	if server.Next == nil {
		server.Next = http.NotFoundHandler()
	}

	if server.BaseSchemas == nil {
		server.BaseSchemas = types.EmptyAPISchemas()
	}

	return nil
}

func setup(ctx context.Context, server *Server) (http.Handler, *schema.Collection, error) {
	if err := setDefaults(server); err != nil {
		return nil, nil, err
	}

	cf, err := client.NewFactory(server.RestConfig)
	if err != nil {
		return nil, nil, err
	}

	ccache := clustercache.NewClusterCache(ctx, cf.DynamicClient())

	server.BaseSchemas = resources.DefaultSchemas(server.BaseSchemas, server.K8s.Discovery(), ccache)
	server.SchemaTemplates = append(server.SchemaTemplates, resources.DefaultSchemaTemplates(cf)...)

	sf := schema.NewCollection(server.BaseSchemas, accesscontrol.NewAccessStore(server.RBAC))
	sync := schemacontroller.Register(ctx,
		server.K8s.Discovery(),
		server.CRD.CustomResourceDefinition(),
		server.API.APIService(),
		server.K8s.AuthorizationV1().SelfSubjectAccessReviews(),
		ccache,
		sf)

	handler, err := handler.New(server.RestConfig, sf, server.AuthMiddleware, server.Next, server.Router)
	if err != nil {
		return nil, nil, err
	}

	server.PostStartHooks = append(server.PostStartHooks, func() error {
		return sync()
	})

	return handler, sf, nil
}

func (c *Server) Handler(ctx context.Context) (http.Handler, error) {
	handler, sf, err := setup(ctx, c)
	if err != nil {
		return nil, err
	}

	for _, hook := range c.StartHooks {
		if err := hook(ctx, c); err != nil {
			return nil, err
		}
	}

	for i := range c.SchemaTemplates {
		sf.AddTemplate(&c.SchemaTemplates[i])
	}

	if err := start.All(ctx, 5, c.starters...); err != nil {
		return nil, err
	}

	for _, hook := range c.PostStartHooks {
		if err := hook(); err != nil {
			return nil, err
		}
	}

	return handler, nil
}

func ListenAndServe(ctx context.Context, secrets v1.SecretController, namespace string, handler http.Handler, httpsPort, httpPort int, opts *server.ListenOpts) error {
	var (
		err error
	)

	if opts == nil {
		opts = &server.ListenOpts{}
	}

	if opts.CA == nil || opts.CAKey == nil {
		opts.CA, opts.CAKey, err = kubernetes.LoadOrGenCA(secrets, namespace, "serving-ca")
		if err != nil {
			return err
		}
	}

	if opts.Storage == nil {
		storage := kubernetes.Load(ctx, secrets, namespace, "service-cert", memory.New())
		opts.Storage = storage
	}

	if err := server.ListenAndServe(ctx, httpsPort, httpPort, handler, opts); err != nil {
		return err
	}

	return nil
}

func (c *Server) ListenAndServe(ctx context.Context, httpsPort, httpPort int, opts *server.ListenOpts) error {
	handler, err := c.Handler(ctx)
	if err != nil {
		return err
	}

	return ListenAndServe(ctx, c.Core.Secret(), c.Namespace, handler, httpsPort, httpPort, opts)
}
