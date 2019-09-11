package counts

import (
	"net/http"
	"strconv"
	"sync"

	schema2 "k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/rancher/norman/pkg/store/empty"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/clustercache"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	ignore = map[string]bool{
		"count":   true,
		"schema":  true,
		"apiRoot": true,
	}
)

func Register(schemas *types.Schemas, ccache clustercache.ClusterCache) {
	schemas.MustImportAndCustomize(Count{}, func(schema *types.Schema) {
		schema.CollectionMethods = []string{http.MethodGet}
		schema.ResourceMethods = []string{http.MethodGet}
		schema.Attributes["access"] = accesscontrol.AccessListMap{
			"watch": accesscontrol.AccessList{
				{
					Namespace:    "*",
					ResourceName: "*",
				},
			},
		}
		schema.Store = &Store{
			ccache: ccache,
		}
	})
}

type Count struct {
	ID     string               `json:"id,omitempty"`
	Counts map[string]ItemCount `json:"counts"`
}

type ItemCount struct {
	Count      int            `json:"count,omitempty"`
	Namespaces map[string]int `json:"namespaces,omitempty"`
	Revision   int            `json:"revision,omitempty"`
}

type Store struct {
	empty.Store
	ccache clustercache.ClusterCache
}

func (s *Store) ByID(apiOp *types.APIRequest, schema *types.Schema, id string) (types.APIObject, error) {
	c := s.getCount(apiOp)
	return types.ToAPI(c), nil
}

func (s *Store) List(apiOp *types.APIRequest, schema *types.Schema, opt *types.QueryOptions) (types.APIObject, error) {
	c := s.getCount(apiOp)
	return types.ToAPI([]interface{}{c}), nil
}

func (s *Store) Watch(apiOp *types.APIRequest, schema *types.Schema, w types.WatchRequest) (chan types.APIEvent, error) {
	var (
		result      = make(chan types.APIEvent, 100)
		counts      map[string]ItemCount
		gvrToSchema = map[schema2.GroupVersionResource]*types.Schema{}
		countLock   sync.Mutex
	)

	counts = s.getCount(apiOp).Counts
	for id := range counts {
		schema := apiOp.Schemas.Schema(id)
		if schema == nil {
			continue
		}

		gvrToSchema[attributes.GVR(schema)] = schema
	}

	go func() {
		<-apiOp.Context().Done()
		close(result)
	}()

	onChange := func(add bool, gvr schema2.GroupVersionResource, _ string, obj runtime.Object) error {
		countLock.Lock()
		defer countLock.Unlock()

		schema := gvrToSchema[gvr]
		if schema == nil {
			return nil
		}

		apiObj := apiOp.Filter(nil, schema, types.ToAPI(obj))
		if apiObj.IsNil() {
			return nil
		}

		_, namespace, revision, ok := getInfo(obj)
		if !ok {
			return nil
		}

		itemCount := counts[schema.ID]
		if revision <= itemCount.Revision {
			return nil
		}

		if add {
			itemCount.Count++
			if namespace != "" {
				itemCount.Namespaces[namespace]++
			}
		} else {
			itemCount.Count--
			if namespace != "" {
				itemCount.Namespaces[namespace]--
			}
		}

		counts[schema.ID] = itemCount
		countsCopy := map[string]ItemCount{}
		for k, v := range counts {
			countsCopy[k] = v
		}

		result <- types.APIEvent{
			Name:         "resource.change",
			ResourceType: "counts",
			Object: types.ToAPI(Count{
				ID:     "count",
				Counts: countsCopy,
			}),
		}

		return nil
	}

	s.ccache.OnAdd(apiOp.Context(), func(gvr schema2.GroupVersionResource, key string, obj runtime.Object) error {
		return onChange(true, gvr, key, obj)
	})
	s.ccache.OnRemove(apiOp.Context(), func(gvr schema2.GroupVersionResource, key string, obj runtime.Object) error {
		return onChange(false, gvr, key, obj)
	})

	return result, nil
}

func (s *Store) schemasToWatch(apiOp *types.APIRequest) (result []*types.Schema) {
	for _, schema := range apiOp.Schemas.Schemas() {
		if ignore[schema.ID] {
			continue
		}

		if attributes.PreferredVersion(schema) != "" {
			continue
		}

		if attributes.PreferredGroup(schema) != "" {
			continue
		}

		if schema.Store == nil {
			continue
		}

		if apiOp.AccessControl.CanList(apiOp, schema) != nil {
			continue
		}

		if apiOp.AccessControl.CanWatch(apiOp, schema) != nil {
			continue
		}

		result = append(result, schema)
	}

	return
}

func getInfo(obj interface{}) (name string, namespace string, revision int, ok bool) {
	r, ok := obj.(runtime.Object)
	if !ok {
		return "", "", 0, false
	}

	meta, err := meta.Accessor(r)
	if err != nil {
		return "", "", 0, false
	}

	revision, err = strconv.Atoi(meta.GetResourceVersion())
	if err != nil {
		return "", "", 0, false
	}

	return meta.GetName(), meta.GetNamespace(), revision, true
}

func (s *Store) getCount(apiOp *types.APIRequest) Count {
	counts := map[string]ItemCount{}

	for _, schema := range s.schemasToWatch(apiOp) {
		gvr := attributes.GVR(schema)

		rev := 0
		itemCount := ItemCount{
			Namespaces: map[string]int{},
		}

		for _, obj := range s.ccache.List(gvr) {
			_, ns, revision, ok := getInfo(obj)
			if !ok {
				continue
			}

			if revision > rev {
				rev = revision
			}

			itemCount.Count++
			if ns != "" {
				itemCount.Namespaces[ns]++
			}
		}

		itemCount.Revision = rev
		counts[schema.ID] = itemCount
	}

	return Count{
		ID:     "count",
		Counts: counts,
	}
}
