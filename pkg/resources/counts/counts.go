package counts

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/rancher/apiserver/pkg/store/empty"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/clustercache"
	"github.com/rancher/steve/pkg/resources/ownership"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/rancher/wrangler/v3/pkg/summary"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

var (
	ignore = map[string]bool{
		"count":   true,
		"schema":  true,
		"apiRoot": true,
	}
)

// Register registers a new count schema. This schema isn't a true resource but instead returns counts for other resources
func Register(schemas *types.APISchemas, ccache clustercache.ClusterCache, schemaFactory schema.Factory) {
	schemas.MustImportAndCustomize(Count{}, func(schema *types.APISchema) {
		schema.CollectionMethods = []string{http.MethodGet}
		schema.ResourceMethods = []string{http.MethodGet}
		schema.Attributes["access"] = accesscontrol.AccessListByVerb{
			"watch": accesscontrol.AccessList{
				accesscontrol.Access{
					Namespace:    "*",
					ResourceName: "*",
				},
			},
		}
		schema.Store = &Store{
			ccache:        ccache,
			schemaFactory: schemaFactory,
		}
	})
}

type Count struct {
	ID     string               `json:"id,omitempty"`
	Counts map[string]ItemCount `json:"counts"`
}

type Summary struct {
	Count         int            `json:"count,omitempty"`
	States        map[string]int `json:"states,omitempty"`
	Error         int            `json:"errors,omitempty"`
	Transitioning int            `json:"transitioning,omitempty"`
}

func (s *Summary) DeepCopy() *Summary {
	r := *s
	if r.States != nil {
		r.States = map[string]int{}
		for k := range s.States {
			r.States[k] = s.States[k]
		}
	}
	return &r
}

type ItemCount struct {
	Summary    Summary            `json:"summary,omitempty"`
	Namespaces map[string]Summary `json:"namespaces,omitempty"`
	Revision   int                `json:"-"`
}

func (i *ItemCount) DeepCopy() *ItemCount {
	r := *i
	r.Summary = *r.Summary.DeepCopy()
	if r.Namespaces != nil {
		r.Namespaces = map[string]Summary{}
		for k, v := range i.Namespaces {
			r.Namespaces[k] = *v.DeepCopy()
		}
	}
	return &r
}

type Store struct {
	empty.Store
	ccache        clustercache.ClusterCache
	schemaFactory schema.Factory
}

func toAPIObject(c Count) types.APIObject {
	return types.APIObject{
		Type:   "count",
		ID:     c.ID,
		Object: c,
	}
}

func (s *Store) ByID(apiOp *types.APIRequest, schema *types.APISchema, id string) (types.APIObject, error) {
	c := s.getCount(apiOp)
	return toAPIObject(c), nil
}

func (s *Store) List(apiOp *types.APIRequest, schema *types.APISchema) (types.APIObjectList, error) {
	c := s.getCount(apiOp)
	return types.APIObjectList{
		Objects: []types.APIObject{
			toAPIObject(c),
		},
	}, nil
}

// Watch creates a watch for the Counts schema. This returns only the counts which have changed since the watch was established
func (s *Store) Watch(apiOp *types.APIRequest, schema *types.APISchema, w types.WatchRequest) (chan types.APIEvent, error) {
	var (
		result         = make(chan Count, 100)
		counts         map[string]ItemCount
		schemasChanged = make(chan struct{}, 1)
		gvkToSchema    = map[schema2.GroupVersionKind]*types.APISchema{}
		countLock      sync.Mutex
	)

	go func() {
		<-apiOp.Context().Done()
		countLock.Lock()
		close(result)
		result = nil
		countLock.Unlock()
	}()

	counts = s.getCount(apiOp).Counts
	for id := range counts {
		schema := apiOp.Schemas.LookupSchema(id)
		if schema == nil {
			continue
		}

		gvkToSchema[attributes.GVK(schema)] = schema
	}

	// Subscribe to global schema changes, then re-filter for this user
	if s.schemaFactory != nil {
		s.schemaFactory.OnChange(apiOp.Context(), func() {
			select {
			case schemasChanged <- struct{}{}:
			default:
			}
		})
	}

	go func() {
		for {
			select {
			case <-apiOp.Context().Done():
				return
			case <-schemasChanged:
				countLock.Lock()
				if result == nil {
					countLock.Unlock()
					continue
				}

				// Get fresh user-filtered schemas
				newSchemas := s.getUserSchemas(apiOp)
				newGVKs := map[schema2.GroupVersionKind]*types.APISchema{}
				for _, schema := range newSchemas {
					gvk := attributes.GVK(schema)
					newGVKs[gvk] = schema
				}

				// Check if any new GVKs were added and collect them
				var addedGVKs []schema2.GroupVersionKind
				for gvk := range newGVKs {
					if _, exists := gvkToSchema[gvk]; !exists {
						addedGVKs = append(addedGVKs, gvk)
						gvkToSchema[gvk] = newGVKs[gvk]
					}
				}

				// If we added new GVKs, list existing resources from ClusterCache and add to counts
				if len(addedGVKs) > 0 {
					// For each new GVK, compute its count using the same logic as getCount()
					for _, gvk := range addedGVKs {
						schema := newGVKs[gvk]
						counts[schema.ID] = s.countForSchema(schema, apiOp)
					}

					// Send full COUNT update with both old and new resource types
					result <- Count{
						ID:     "count",
						Counts: counts,
					}
				}
				countLock.Unlock()
			}
		}
	}()

	onChange := func(add bool, gvk schema2.GroupVersionKind, _ string, obj, oldObj runtime.Object) error {
		countLock.Lock()
		defer countLock.Unlock()

		if result == nil {
			return nil
		}

		schema := gvkToSchema[gvk]
		if schema == nil {
			return nil
		}

		_, namespace, revision, summary, ok := getInfo(obj, schema)
		if !ok {
			return nil
		}

		itemCount := counts[schema.ID]
		// Initialize Namespaces map if nil (new schema)
		if itemCount.Namespaces == nil {
			itemCount.Namespaces = map[string]Summary{}
		}
		if revision <= itemCount.Revision {
			return nil
		}

		if oldObj != nil {
			if _, _, _, oldSummary, ok := getInfo(oldObj, schema); ok {
				if oldSummary.Transitioning == summary.Transitioning &&
					oldSummary.Error == summary.Error &&
					simpleState(oldSummary) == simpleState(summary) {
					return nil
				}
				itemCount = removeCounts(itemCount, namespace, oldSummary)
				itemCount = addCounts(itemCount, namespace, summary)
			} else {
				return nil
			}
		} else if add {
			itemCount = addCounts(itemCount, namespace, summary)
		} else {
			itemCount = removeCounts(itemCount, namespace, summary)
		}

		counts[schema.ID] = itemCount
		changedCount := map[string]ItemCount{
			schema.ID: *itemCount.DeepCopy(),
		}

		result <- Count{
			ID:     "count",
			Counts: changedCount,
		}

		return nil
	}

	s.ccache.OnAdd(apiOp.Context(), func(gvk schema2.GroupVersionKind, key string, obj runtime.Object) error {
		return onChange(true, gvk, key, obj, nil)
	})
	s.ccache.OnChange(apiOp.Context(), func(gvk schema2.GroupVersionKind, key string, obj, oldObj runtime.Object) error {
		return onChange(true, gvk, key, obj, oldObj)
	})
	s.ccache.OnRemove(apiOp.Context(), func(gvk schema2.GroupVersionKind, key string, obj runtime.Object) error {
		return onChange(false, gvk, key, obj, nil)
	})

	// buffer the counts so that we don't spam the consumer with constant updates
	return countsBuffer(result), nil
}

// getUserSchemas fetches user-filtered schemas from the factory
func (s *Store) getUserSchemas(apiOp *types.APIRequest) []*types.APISchema {
	if s.schemaFactory == nil {
		// Fallback to apiOp.Schemas if no factory (for tests)
		return s.schemasToWatch(apiOp)
	}

	user, ok := request.UserFrom(apiOp.Context())
	if !ok {
		return s.schemasToWatch(apiOp)
	}

	userSchemas, err := s.schemaFactory.Schemas(user)
	if err != nil {
		return s.schemasToWatch(apiOp)
	}

	return s.schemasToWatchFromSchemas(userSchemas, apiOp.AccessControl)
}

// schemasToWatchFromSchemas filters schemas without needing full apiOp
func (s *Store) schemasToWatchFromSchemas(schemas *types.APISchemas, accessControl types.AccessControl) []*types.APISchema {
	var result []*types.APISchema

	for _, schema := range schemas.Schemas {
		if ignore[schema.ID] {
			continue
		}
		if schema.Store == nil {
			continue
		}

		// Create minimal apiOp for access checks
		tempOp := &types.APIRequest{
			Schemas:       schemas,
			AccessControl: accessControl,
		}

		if accessControl.CanList(tempOp, schema) != nil {
			continue
		}
		if accessControl.CanWatch(tempOp, schema) != nil {
			continue
		}

		result = append(result, schema)
	}

	return result
}

func (s *Store) schemasToWatch(apiOp *types.APIRequest) (result []*types.APISchema) {
	for _, schema := range apiOp.Schemas.Schemas {
		if ignore[schema.ID] {
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

func getInfo(obj interface{}, schema *types.APISchema) (name string, namespace string, revision int, summaryResult summary.Summary, ok bool) {
	r, ok := obj.(runtime.Object)
	if !ok {
		return "", "", 0, summaryResult, false
	}

	meta, err := meta.Accessor(r)
	if err != nil {
		return "", "", 0, summaryResult, false
	}

	revision, err = strconv.Atoi(meta.GetResourceVersion())
	if err != nil {
		return "", "", 0, summaryResult, false
	}

	opts := &summary.SummarizeOptions{HasObservedGeneration: false}
	if schema != nil && schema.Attributes != nil {
		opts.HasObservedGeneration = schemas.HasObservedGeneration(schema.Schema)
	}

	summaryResult = summary.SummarizeWithOptions(r, opts)
	return meta.GetName(), meta.GetNamespace(), revision, summaryResult, true
}

func removeCounts(itemCount ItemCount, ns string, summary summary.Summary) ItemCount {
	itemCount.Summary = removeSummary(itemCount.Summary, summary)
	if ns != "" {
		itemCount.Namespaces[ns] = removeSummary(itemCount.Namespaces[ns], summary)
	}
	return itemCount
}

func addCounts(itemCount ItemCount, ns string, summary summary.Summary) ItemCount {
	itemCount.Summary = addSummary(itemCount.Summary, summary)
	if ns != "" {
		itemCount.Namespaces[ns] = addSummary(itemCount.Namespaces[ns], summary)
	}
	return itemCount
}

func removeSummary(counts Summary, summary summary.Summary) Summary {
	counts.Count--
	if summary.Transitioning {
		counts.Transitioning--
	}
	if summary.Error {
		counts.Error--
	}
	if simpleState(summary) != "" {
		if counts.States == nil {
			counts.States = map[string]int{}
		}
		counts.States[simpleState(summary)]--
	}
	return counts
}

func addSummary(counts Summary, summary summary.Summary) Summary {
	counts.Count++
	if summary.Transitioning {
		counts.Transitioning++
	}
	if summary.Error {
		counts.Error++
	}
	if simpleState(summary) != "" {
		if counts.States == nil {
			counts.States = map[string]int{}
		}
		counts.States[simpleState(summary)]++
	}
	return counts
}

func simpleState(summary summary.Summary) string {
	if summary.Error {
		return "error"
	} else if summary.Transitioning {
		return "in-progress"
	}
	return ""
}

// countForSchema computes the ItemCount for a single schema by listing all objects from ClusterCache
func (s *Store) countForSchema(schema *types.APISchema, apiOp *types.APIRequest) ItemCount {
	gvk := attributes.GVK(schema)
	access, _ := attributes.Access(schema).(accesscontrol.AccessListByVerb)

	rev := 0
	itemCount := ItemCount{
		Namespaces: map[string]Summary{},
	}

	all := access.Grants("list", "*", "*")

	// Some resources from Rancher extension apiservers have additional
	// visibility that is not just based on RBAC. (eg: ext.cattle.io/v1 Tokens)
	//
	// Those requires more filtering rules.
	ownershipFilter, hasOwnershipFilter := ownership.Lookup(gvk)
	var userInfo user.Info
	var isAdmin bool
	if hasOwnershipFilter {
		userInfo, _ = request.UserFrom(apiOp.Request.Context())
		accessSet := accesscontrol.AccessSetFromAPIRequest(apiOp)
		isAdmin = accessSet != nil && accessSet.Grants("list", schema2.GroupResource{
			Resource: "*",
		}, "", "")
	}

	for _, obj := range s.ccache.List(gvk) {
		name, ns, revision, summary, ok := getInfo(obj, schema)
		if !ok {
			continue
		}

		if !all && !access.Grants("list", ns, name) && !access.Grants("get", ns, name) {
			continue
		}

		if hasOwnershipFilter {
			objMeta, err := meta.Accessor(obj)
			if err != nil || userInfo == nil || !ownershipFilter.Matches(userInfo, isAdmin, objMeta.GetLabels()) {
				continue
			}
		}

		if revision > rev {
			rev = revision
		}

		itemCount = addCounts(itemCount, ns, summary)
	}

	itemCount.Revision = rev
	return itemCount
}

func (s *Store) getCount(apiOp *types.APIRequest) Count {
	schemas := s.schemasToWatch(apiOp)
	counts := map[string]ItemCount{}

	for _, schema := range schemas {
		counts[schema.ID] = s.countForSchema(schema, apiOp)
	}

	return Count{
		ID:     "count",
		Counts: counts,
	}
}
