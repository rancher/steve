package apigroups

import (
	"net/http"

	"github.com/rancher/norman/v2/pkg/data"
	"github.com/rancher/norman/v2/pkg/store/empty"
	"github.com/rancher/norman/v2/pkg/types"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
)

func Register(schemas *types.Schemas, discovery discovery.DiscoveryInterface) {
	schemas.MustImportAndCustomize(v1.APIGroup{}, func(schema *types.Schema) {
		schema.CollectionMethods = []string{http.MethodGet}
		schema.ResourceMethods = []string{http.MethodGet}
		schema.Store = NewStore(discovery)
		schema.Formatter = func(request *types.APIRequest, resource *types.RawResource) {
			resource.ID = data.Object(resource.Values).String("name")
		}
	})
}

type Store struct {
	empty.Store

	discovery discovery.DiscoveryInterface
}

func NewStore(discovery discovery.DiscoveryInterface) types.Store {
	return &Store{
		Store:     empty.Store{},
		discovery: discovery,
	}
}

func (e *Store) ByID(apiOp *types.APIRequest, schema *types.Schema, id string) (types.APIObject, error) {
	groupList, err := e.discovery.ServerGroups()
	if err != nil {
		return types.APIObject{}, err
	}

	if id == "core" {
		id = ""
	}

	for _, group := range groupList.Groups {
		if group.Name == id {
			return types.ToAPI(group), nil
		}
	}

	return types.APIObject{}, nil
}

func (e *Store) List(apiOp *types.APIRequest, schema *types.Schema, opt *types.QueryOptions) (types.APIObject, error) {
	groupList, err := e.discovery.ServerGroups()
	if err != nil {
		return types.APIObject{}, err
	}

	var result []interface{}
	for _, item := range groupList.Groups {
		if item.Name == "" {
			item.Name = "core"
		}
		result = append(result, item)
	}

	return types.ToAPI(result), nil
}
