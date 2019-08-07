package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"golang.org/x/sync/semaphore"

	"github.com/rancher/norman/pkg/types/values"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	sSchema := schemas.Schema("schema")
	sSchema.CollectionFormatter = a.schemaCollectionFormatter(sSchema.CollectionFormatter)

	schemas.DefaultMapper = newDefaultMapper
	subscribe.Register(schemas)
	return schemas, nil
}

func (a *apiServer) schemaCollectionFormatter(next types.CollectionFormatter) types.CollectionFormatter {
	return func(request *types.APIRequest, collection *types.GenericCollection) {
		if next != nil {
			next(request, collection)
		}

		wg := sync.WaitGroup{}
		sem := semaphore.NewWeighted(100)

		for _, item := range collection.Data {
			resource, ok := item.(*types.RawResource)
			if !ok {
				continue
			}

			schema := request.Schemas.Schema(resource.ID)
			if schema == nil {
				continue
			}

			access := accesscontrol.GetAccessListMap(schema)
			if !access.Grants("list", "*", "*") {
				continue
			}

			wg.Add(1)
			if err := sem.Acquire(context.TODO(), 1); err != nil {
				panic(err)
			}
			go func() {
				defer func() {
					sem.Release(1)
					wg.Done()
				}()

				client, err := a.cf.Client(request, schema)
				if err != nil {
					return
				}

				fmt.Println("listing", attributes.GVK(schema))
				resp, err := client.List(v1.ListOptions{})
				if err != nil {
					return
				}
				if len(resp.Items) > 0 {
					values.PutValue(resource.Values, len(resp.Items), "attributes", "count")
				}
			}()
		}

		wg.Wait()
	}
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
