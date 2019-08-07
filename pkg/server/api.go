package server

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/naok/pkg/schemas"
	"github.com/rancher/norman/pkg/api"
	"github.com/rancher/norman/pkg/store/proxy"
	"github.com/rancher/norman/pkg/subscribe"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/norman/pkg/urlbuilder"
	"k8s.io/apiserver/pkg/authentication/user"
)

func newAPIServer(cf proxy.ClientGetter, as *accesscontrol.AccessStore, sf schemas.SchemaFactory) http.Handler {
	a := &apiServer{
		Router: mux.NewRouter(),
		cf:     cf,
		as:     as,
		sf:     sf,
		server: api.NewAPIServer(),
	}
	a.Router.StrictSlash(true)
	a.server.AccessControl = accesscontrol.NewAccessControl()
	a.routes()
	return a
}

type apiServer struct {
	*mux.Router
	cf     proxy.ClientGetter
	as     *accesscontrol.AccessStore
	sf     schemas.SchemaFactory
	server *api.Server
}

func (a *apiServer) newSchemas() (*types.Schemas, error) {
	schemas, err := schemas.DefaultSchemaFactory()
	if err != nil {
		return nil, err
	}

	schemas.DefaultMapper = newDefaultMapper
	subscribe.Register(schemas)
	return schemas, nil
}

func (a *apiServer) common(rw http.ResponseWriter, req *http.Request) (*types.APIRequest, bool) {
	user := &user.DefaultInfo{
		Name:   "admin",
		Groups: []string{"system:masters"},
	}

	accessSet := a.as.AccessFor(user)
	schemas, err := a.sf.Schemas("", accessSet, a.newSchemas)
	if err != nil {
		rw.Write([]byte(err.Error()))
		rw.WriteHeader(http.StatusInternalServerError)
	}

	urlBuilder, err := urlbuilder.New(req, a, schemas)
	if err != nil {
		rw.Write([]byte(err.Error()))
		rw.WriteHeader(http.StatusInternalServerError)
		return nil, false
	}

	return &types.APIRequest{
		Schemas:    schemas,
		Request:    req,
		Response:   rw,
		URLBuilder: urlBuilder,
	}, true
}

func (a *apiServer) Schema(base string, schema *types.Schema) string {
	gvr := attributes.GVR(schema)

	if gvr.Group == "" && gvr.Version != "" && gvr.Resource != "" {
		return urlbuilder.ConstructBasicURL(base, gvr.Version, gvr.Resource)
	}

	if gvr.Resource != "" {
		return urlbuilder.ConstructBasicURL(base, "v1", "apis", gvr.Group, gvr.Version, gvr.Resource)
	}

	return urlbuilder.ConstructBasicURL(base, "v1", schema.PluralName)
}
