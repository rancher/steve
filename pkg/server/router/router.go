package router

import (
	"net/http"

	"github.com/gorilla/mux"
)

type Handlers struct {
	K8sResource     http.Handler
	GenericResource http.Handler
	APIRoot         http.Handler
	K8sProxy        http.Handler
}

func Routes(h Handlers) http.Handler {
	m := mux.NewRouter()
	m.UseEncodedPath()
	m.StrictSlash(true)
	m.NotFoundHandler = h.K8sProxy

	m.Path("/").Handler(h.APIRoot)
	m.Path("/{name:v1}").Handler(h.APIRoot)

	m.Path("/v1/{type:schemas}/{name:.*}").Handler(h.GenericResource)
	m.Path("/v1/{group}.{version}.{resource}").Handler(h.K8sResource)
	m.Path("/v1/{group}.{version}.{resource}/{nameorns}").Handler(h.K8sResource)
	m.Path("/v1/{group}.{version}.{resource}/{namespace}/{name}").Handler(h.K8sResource)
	m.Path("/v1/{type}").Handler(h.GenericResource)
	m.Path("/v1/{type}/{name}").Handler(h.GenericResource)

	return m
}
