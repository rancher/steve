package clusters

import (
	"net/http"

	"github.com/rancher/steve/pkg/server/store/proxy"

	"github.com/rancher/steve/pkg/schemaserver/store/empty"
	"github.com/rancher/steve/pkg/schemaserver/types"
	"github.com/rancher/wrangler/pkg/schemas/validation"
)

const (
	rancherCluster = "management.cattle.io.cluster"
	localID        = "local"
)

var (
	local = types.APIObject{
		Type:   "cluster",
		ID:     localID,
		Object: &Cluster{},
	}
	localList = types.APIObjectList{
		Objects: []types.APIObject{
			local,
		},
	}
)

type Cluster struct {
}

func Register(schemas *types.APISchemas, cg proxy.ClientGetter) {
	schemas.MustImportAndCustomize(Cluster{}, func(schema *types.APISchema) {
		schema.CollectionMethods = []string{http.MethodGet}
		schema.ResourceMethods = []string{http.MethodGet}
		schema.Store = &Store{}

		shell := &shell{
			cg:        cg,
			namespace: "dashboard-shells",
		}
		schema.LinkHandlers = map[string]http.Handler{
			"shell": shell,
		}

		schema.Formatter = func(request *types.APIRequest, resource *types.RawResource) {
			resource.Links["api"] = request.URLBuilder.RelativeToRoot("/k8s/clusters/" + resource.ID)
		}
	})
}

type Store struct {
	empty.Store
}

func toClusterList(obj types.APIObjectList, err error) (types.APIObjectList, error) {
	for i := range obj.Objects {
		obj.Objects[i], _ = toCluster(obj.Objects[i], err)
	}
	return obj, err
}

func toCluster(obj types.APIObject, err error) (types.APIObject, error) {
	return types.APIObject{
		Type:   "cluster",
		ID:     obj.ID,
		Object: &Cluster{},
	}, err
}

func (s *Store) ByID(apiOp *types.APIRequest, schema *types.APISchema, id string) (types.APIObject, error) {
	clusters := apiOp.Schemas.LookupSchema(rancherCluster)
	if clusters == nil {
		if id == localID {
			return local, nil
		}
		return types.APIObject{}, validation.NotFound
	}
	return toCluster(clusters.Store.ByID(apiOp, clusters, id))
}

func (s *Store) List(apiOp *types.APIRequest, schema *types.APISchema) (types.APIObjectList, error) {
	clusters := apiOp.Schemas.LookupSchema(rancherCluster)
	if clusters == nil {
		return localList, nil
	}
	return toClusterList(clusters.Store.List(apiOp, clusters))
}

func (s *Store) Watch(apiOp *types.APIRequest, schema *types.APISchema, w types.WatchRequest) (chan types.APIEvent, error) {
	clusters := apiOp.Schemas.LookupSchema(rancherCluster)
	if clusters == nil {
		return nil, nil
	}
	target, err := clusters.Store.Watch(apiOp, clusters, w)
	if err != nil {
		return nil, err
	}

	result := make(chan types.APIEvent)
	go func() {
		defer close(result)
		for event := range target {
			event.Object, _ = toCluster(event.Object, nil)
			result <- event
		}
	}()

	return result, nil
}
