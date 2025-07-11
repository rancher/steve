package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	apiserver "github.com/rancher/apiserver/pkg/server"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/dynamiclistener/server"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/aggregation"
	"github.com/rancher/steve/pkg/auth"
	"github.com/rancher/steve/pkg/client"
	"github.com/rancher/steve/pkg/clustercache"
	schemacontroller "github.com/rancher/steve/pkg/controllers/schema"
	"github.com/rancher/steve/pkg/ext"
	"github.com/rancher/steve/pkg/resources"
	"github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/resources/schemas"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/steve/pkg/schema/definitions"
	"github.com/rancher/steve/pkg/server/handler"
	"github.com/rancher/steve/pkg/server/router"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	metricsStore "github.com/rancher/steve/pkg/stores/metrics"
	"github.com/rancher/steve/pkg/stores/proxy"
	"github.com/rancher/steve/pkg/stores/sqlpartition"
	"github.com/rancher/steve/pkg/stores/sqlproxy"
	"github.com/rancher/steve/pkg/summarycache"
	"k8s.io/client-go/rest"
)

var ErrConfigRequired = errors.New("rest config is required")

var _ ExtensionAPIServer = (*ext.ExtensionAPIServer)(nil)

// ExtensionAPIServer will run an extension API server. The extension API server
// will be accessible from Steve at the /ext endpoint and will be compatible with
// the aggregate API server in Kubernetes.
type ExtensionAPIServer interface {
	// The ExtensionAPIServer is served at /ext in Steve's mux
	http.Handler
	// Run configures the API server and make the HTTP handler available
	Run(ctx context.Context) error
	// Registered returns a channel that will be close once the registration requests are received from the kube-apiserver.
	Registered() <-chan struct{}
}

type Server struct {
	http.Handler

	ClientFactory   *client.Factory
	ClusterCache    clustercache.ClusterCache
	SchemaFactory   schema.Factory
	RESTConfig      *rest.Config
	BaseSchemas     *types.APISchemas
	AccessSetLookup accesscontrol.AccessSetLookup
	APIServer       *apiserver.Server
	ClusterRegistry string
	Version         string

	cacheFactory *factory.CacheFactory

	extensionAPIServer            ExtensionAPIServer
	SkipWaitForExtensionAPIServer bool

	authMiddleware      auth.Middleware
	controllers         *Controllers
	needControllerStart bool
	next                http.Handler
	router              router.RouterFunc

	aggregationSecretNamespace string
	aggregationSecretName      string
	SQLCache                   bool
}

type Options struct {
	// Controllers If the controllers are passed in the caller must also start the controllers
	Controllers                *Controllers
	ClientFactory              *client.Factory
	AccessSetLookup            accesscontrol.AccessSetLookup
	AuthMiddleware             auth.Middleware
	Next                       http.Handler
	Router                     router.RouterFunc
	AggregationSecretNamespace string
	AggregationSecretName      string
	ClusterRegistry            string
	ServerVersion              string
	// SQLCache enables the SQLite-based caching mechanism
	SQLCache bool

	SQLCacheFactoryOptions factory.CacheFactoryOptions

	// ExtensionAPIServer enables an extension API server that will be served
	// under /ext
	// If nil, Steve's default http handler for unknown routes will be served.
	//
	// In most cases, you'll want to use [github.com/rancher/steve/pkg/ext.NewExtensionAPIServer]
	// to create an ExtensionAPIServer.
	ExtensionAPIServer ExtensionAPIServer

	// SkipWaitForExtensionAPIServer allows serving requests despite the ExtensionAPIServer may not have been registered yet.
	SkipWaitForExtensionAPIServer bool
}

func New(ctx context.Context, restConfig *rest.Config, opts *Options) (*Server, error) {
	if opts == nil {
		opts = &Options{}
	}

	var cacheFactory *factory.CacheFactory
	if opts.SQLCache {
		var err error
		cacheFactory, err = factory.NewCacheFactory(opts.SQLCacheFactoryOptions)
		if err != nil {
			return nil, fmt.Errorf("creating SQL cache factory: %w", err)
		}
	}

	server := &Server{
		RESTConfig:                 restConfig,
		ClientFactory:              opts.ClientFactory,
		AccessSetLookup:            opts.AccessSetLookup,
		authMiddleware:             opts.AuthMiddleware,
		controllers:                opts.Controllers,
		next:                       opts.Next,
		router:                     opts.Router,
		aggregationSecretNamespace: opts.AggregationSecretNamespace,
		aggregationSecretName:      opts.AggregationSecretName,
		ClusterRegistry:            opts.ClusterRegistry,
		Version:                    opts.ServerVersion,
		// SQLCache enables the SQLite-based lasso caching mechanism
		SQLCache:                      opts.SQLCache,
		cacheFactory:                  cacheFactory,
		extensionAPIServer:            opts.ExtensionAPIServer,
		SkipWaitForExtensionAPIServer: opts.SkipWaitForExtensionAPIServer,
	}

	if err := setup(ctx, server); err != nil {
		return nil, err
	}

	return server, server.start(ctx)
}

func setDefaults(server *Server) error {
	if server.RESTConfig == nil {
		return ErrConfigRequired
	}

	if server.controllers == nil {
		var err error
		server.controllers, err = NewController(server.RESTConfig, nil)
		server.needControllerStart = true
		if err != nil {
			return err
		}
	}

	if server.next == nil {
		server.next = http.NotFoundHandler()
	}

	if server.BaseSchemas == nil {
		server.BaseSchemas = types.EmptyAPISchemas()
	}

	return nil
}

func setup(ctx context.Context, server *Server) error {
	err := setDefaults(server)
	if err != nil {
		return err
	}

	cf := server.ClientFactory
	if cf == nil {
		cf, err = client.NewFactory(server.RESTConfig, server.authMiddleware != nil)
		if err != nil {
			return err
		}
		server.ClientFactory = cf
	}

	asl := server.AccessSetLookup
	if asl == nil {
		asl = accesscontrol.NewAccessStore(ctx, true, server.controllers.RBAC)
	}

	ccache := clustercache.NewClusterCache(ctx, cf.AdminDynamicClient())
	server.ClusterCache = ccache
	sf := schema.NewCollection(ctx, server.BaseSchemas, asl)

	if err = resources.DefaultSchemas(ctx, server.BaseSchemas, ccache, server.ClientFactory, sf, server.Version); err != nil {
		return err
	}
	definitions.Register(ctx, server.BaseSchemas, server.controllers.K8s.Discovery(),
		server.controllers.CRD.CustomResourceDefinition(), server.controllers.API.APIService())

	summaryCache := summarycache.New(sf, ccache)
	summaryCache.Start(ctx)
	cols, err := common.NewDynamicColumns(server.RESTConfig)
	if err != nil {
		return err
	}

	var onSchemasHandler schemacontroller.SchemasHandlerFunc
	if server.SQLCache {
		s, err := sqlproxy.NewProxyStore(ctx, cols, cf, summaryCache, summaryCache, server.cacheFactory)
		if err != nil {
			panic(err)
		}

		errStore := proxy.NewErrorStore(
			proxy.NewUnformatterStore(
				proxy.NewWatchRefresh(
					sqlpartition.NewStore(
						s,
						asl,
					),
					asl,
				),
			),
		)
		store := metricsStore.NewMetricsStore(errStore)
		// end store setup code

		for _, template := range resources.DefaultSchemaTemplatesForStore(store, server.BaseSchemas, summaryCache, asl, server.controllers.K8s.Discovery(), common.TemplateOptions{InSQLMode: true}) {
			sf.AddTemplate(template)
		}

		onSchemasHandler = func(schemas *schema.Collection) error {
			if err := ccache.OnSchemas(schemas); err != nil {
				return err
			}
			if err := s.Reset(); err != nil {
				return err
			}
			return nil
		}
	} else {
		for _, template := range resources.DefaultSchemaTemplates(cf, server.BaseSchemas, summaryCache, asl, server.controllers.K8s.Discovery(), server.controllers.Core.Namespace().Cache(), common.TemplateOptions{InSQLMode: false}) {
			sf.AddTemplate(template)
		}
		onSchemasHandler = ccache.OnSchemas
	}

	schemas.SetupWatcher(ctx, server.BaseSchemas, asl, sf)

	schemacontroller.Register(ctx,
		cols,
		server.controllers.K8s.Discovery(),
		server.controllers.CRD.CustomResourceDefinition(),
		server.controllers.API.APIService(),
		server.controllers.K8s.AuthorizationV1().SelfSubjectAccessReviews(),
		onSchemasHandler,
		sf)

	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if server.extensionAPIServer == nil || server.SkipWaitForExtensionAPIServer {
			server.next.ServeHTTP(rw, req)
			return
		}

		select {
		case <-server.extensionAPIServer.Registered():
			server.next.ServeHTTP(rw, req)
		default:
			http.Error(rw, "API Aggregation not ready", http.StatusServiceUnavailable)
		}
	})

	apiServer, handler, err := handler.New(server.RESTConfig, sf, server.authMiddleware, next, server.router, server.extensionAPIServer)
	if err != nil {
		return err
	}

	server.APIServer = apiServer
	server.Handler = handler
	server.SchemaFactory = sf

	return nil
}

func (c *Server) start(ctx context.Context) error {
	if c.needControllerStart {
		if err := c.controllers.Start(ctx); err != nil {
			return err
		}
	}
	if c.extensionAPIServer != nil {
		if err := c.extensionAPIServer.Run(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *Server) StartAggregation(ctx context.Context) {
	aggregation.Watch(ctx, c.controllers.Core.Secret(), c.aggregationSecretNamespace,
		c.aggregationSecretName, c)
}

func (c *Server) ListenAndServe(ctx context.Context, httpsPort, httpPort int, opts *server.ListenOpts) error {
	if opts == nil {
		opts = &server.ListenOpts{}
	}
	if opts.Storage == nil && opts.Secrets == nil {
		opts.Secrets = c.controllers.Core.Secret()
	}

	c.StartAggregation(ctx)

	if len(opts.TLSListenerConfig.SANs) == 0 {
		opts.TLSListenerConfig.SANs = []string{"127.0.0.1"}
	}
	if err := server.ListenAndServe(ctx, httpsPort, httpPort, c, opts); err != nil {
		return err
	}

	<-ctx.Done()
	return ctx.Err()
}
