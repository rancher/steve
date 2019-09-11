package publicapi

import (
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/rancher/steve/pkg/accesscontrol"
	k8sproxy "github.com/rancher/steve/pkg/proxy"
	"github.com/rancher/steve/pkg/resources/schema"
	"github.com/rancher/steve/pkg/server/router"
	"github.com/rancher/norman/pkg/api"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/norman/pkg/urlbuilder"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/rest"
)

func NewHandler(cfg *rest.Config, sf schema.Factory) (http.Handler, error) {
	var (
		err error
	)

	a := &apiServer{
		sf:     sf,
		server: api.DefaultAPIServer(),
	}
	a.server.AccessControl = accesscontrol.NewAccessControl()

	proxy, err := k8sproxy.Handler("/", cfg)
	if err != nil {
		return nil, err
	}

	return router.Routes(router.Handlers{
		K8sResource:     a.apiHandler(k8sAPI),
		GenericResource: a.apiHandler(nil),
		K8sProxy:        proxy,
		APIRoot:         a.apiHandler(apiRoot),
	}), nil
}

type apiServer struct {
	sf     schema.Factory
	server *api.Server
}

func (a *apiServer) common(rw http.ResponseWriter, req *http.Request) (*types.APIRequest, bool) {
	user := &user.DefaultInfo{
		Name:   "admin",
		Groups: []string{"system:masters"},
	}

	schemas, err := a.sf.Schemas(user)
	if err != nil {
		logrus.Errorf("HTTP request failed: %v", err)
		rw.Write([]byte(err.Error()))
		rw.WriteHeader(http.StatusInternalServerError)
	}

	urlBuilder, err := urlbuilder.NewPrefixed(req, schemas, "v1")
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

type APIFunc func(schema.Factory, *types.APIRequest)

func (a *apiServer) apiHandler(apiFunc APIFunc) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		a.api(rw, req, apiFunc)
	})
}

func (a *apiServer) api(rw http.ResponseWriter, req *http.Request, apiFunc APIFunc) {
	apiOp, ok := a.common(rw, req)
	if ok {
		if apiFunc != nil {
			apiFunc(a.sf, apiOp)
		}
		a.server.Handle(apiOp)
	}
}
