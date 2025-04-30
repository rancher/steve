package router

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/apiserver/pkg/urlbuilder"
	"github.com/rancher/steve/pkg/server/router/merge"
)

type RouterFunc func(h Handlers) http.Handler

type Handlers struct {
	K8sResource http.Handler
	APIRoot     http.Handler
	K8sProxy    http.Handler
	Next        http.Handler
	// ExtensionAPIServer serves under /ext. If nil, the default unknown path
	// handler is served.
	ExtensionAPIServer http.Handler
}

func Routes(h Handlers) http.Handler {
	m := mux.NewRouter()
	m.UseEncodedPath()
	m.StrictSlash(true)
	m.Use(urlbuilder.RedirectRewrite)

	m.Path("/").Handler(h.APIRoot).HeadersRegexp("Accept", ".*json.*")
	m.Path("/{name:v1}").Handler(h.APIRoot)

	if h.ExtensionAPIServer != nil {
		m.Path("/ext").Handler(http.StripPrefix("/ext", h.ExtensionAPIServer))
		m.PathPrefix("/ext/").Handler(http.StripPrefix("/ext", h.ExtensionAPIServer))

		m.Path("/v1/ext.cattle.io.{[A-Za-z]+}").Handler(h.ExtensionAPIServer)
		m.Path("/apis/ext.cattle.io.{[A-Za-z]+}").Handler(h.ExtensionAPIServer)
	}

	m.Path("/v1/{type}").Handler(h.K8sResource)
	m.Path("/v1/{type}/{nameorns}").Queries("link", "{link}").Handler(h.K8sResource)
	m.Path("/v1/{type}/{nameorns}").Queries("action", "{action}").Handler(h.K8sResource)
	m.Path("/v1/{type}/{nameorns}").Handler(h.K8sResource)
	m.Path("/v1/{type}/{namespace}/{name}").Queries("action", "{action}").Handler(h.K8sResource)
	m.Path("/v1/{type}/{namespace}/{name}").Queries("link", "{link}").Handler(h.K8sResource)
	m.Path("/v1/{type}/{namespace}/{name}").Handler(h.K8sResource)
	m.Path("/v1/{type}/{namespace}/{name}/{link}").Handler(h.K8sResource)

	m.Path("/api").Handler(h.K8sProxy) // Can't just prefix this as UI needs /apikeys path
	m.PathPrefix("/api/").Handler(h.K8sProxy)

	m.Path("/apis").Handler(merge.Merge(h.K8sProxy, h.ExtensionAPIServer, merge.ApiGropuListMerger))
	m.PathPrefix("/apis").Handler(h.K8sProxy)

	m.PathPrefix("/openapi/v2").Handler(merge.Merge(h.K8sProxy, h.ExtensionAPIServer, merge.OpenAPIV2Merger))
	m.PathPrefix("/openapi/v3").Handler(merge.Merge(h.K8sProxy, h.ExtensionAPIServer, merge.OpenAPIV3Merger))

	m.PathPrefix("/version").Handler(h.K8sProxy)
	m.NotFoundHandler = h.Next

	return m
}
