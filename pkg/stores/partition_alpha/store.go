// Package partition_alpha implements a store with parallel partitioning of data
// so that segmented data can be concurrently collected and returned as a single data set.
package partition_alpha

import (
	"context"
	"fmt"
	"reflect"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
	"github.com/rancher/steve/pkg/accesscontrol"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// Partitioner is an interface for interacting with partitions.
type Partitioner interface {
	Lookup(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) (partition.Partition, error)
	All(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) ([]partition.Partition, error)
	Store() UnstructuredStore
}

type SchemaColumnSetter interface {
	SetColumns(ctx context.Context, schema *types.APISchema) error
}

// Store implements types.proxyStore for partitions.
type Store struct {
	Partitioner Partitioner
	asl         accesscontrol.AccessSetLookup
}

// NewStore creates a types.proxyStore implementation with a partitioner
func NewStore(store UnstructuredStore, asl accesscontrol.AccessSetLookup) *Store {
	s := &Store{
		Partitioner: &rbacPartitioner{
			proxyStore: store,
		},
		asl: asl,
	}

	return s
}

// Delete deletes an object from a store.
func (s *Store) Delete(apiOp *types.APIRequest, schema *types.APISchema, id string) (types.APIObject, error) {
	target := s.Partitioner.Store()

	obj, warnings, err := target.Delete(apiOp, schema, id)
	if err != nil {
		return types.APIObject{}, err
	}
	return toAPI(schema, obj, warnings), nil
}

// ByID looks up a single object by its ID.
func (s *Store) ByID(apiOp *types.APIRequest, schema *types.APISchema, id string) (types.APIObject, error) {
	target := s.Partitioner.Store()

	obj, warnings, err := target.ByID(apiOp, schema, id)
	if err != nil {
		return types.APIObject{}, err
	}
	return toAPI(schema, obj, warnings), nil
}

// List returns a list of objects across all applicable partitions.
// If pagination parameters are used, it returns a segment of the list.
func (s *Store) List(apiOp *types.APIRequest, schema *types.APISchema) (types.APIObjectList, error) {
	var (
		result types.APIObjectList
	)

	partitions, err := s.Partitioner.All(apiOp, schema, "list", "")
	if err != nil {
		return result, err
	}

	store := s.Partitioner.Store()

	list, continueToken, err := store.ListByPartitions(apiOp, schema, partitions)
	if err != nil {
		return result, err
	}

	result.Count = len(list)

	for _, item := range list {
		item := item.DeepCopy()
		result.Objects = append(result.Objects, toAPI(schema, item, nil))
	}

	result.Revision = ""
	result.Continue = continueToken
	return result, nil
}

// Create creates a single object in the store.
func (s *Store) Create(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject) (types.APIObject, error) {
	target := s.Partitioner.Store()

	obj, warnings, err := target.Create(apiOp, schema, data)
	if err != nil {
		return types.APIObject{}, err
	}
	return toAPI(schema, obj, warnings), nil
}

// Update updates a single object in the store.
func (s *Store) Update(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject, id string) (types.APIObject, error) {
	target := s.Partitioner.Store()

	obj, warnings, err := target.Update(apiOp, schema, data, id)
	if err != nil {
		return types.APIObject{}, err
	}
	return toAPI(schema, obj, warnings), nil
}

// Watch returns a channel of events for a list or resource.
func (s *Store) Watch(apiOp *types.APIRequest, schema *types.APISchema, wr types.WatchRequest) (chan types.APIEvent, error) {
	partitions, err := s.Partitioner.All(apiOp, schema, "watch", wr.ID)
	if err != nil {
		return nil, err
	}

	store := s.Partitioner.Store()

	response := make(chan types.APIEvent)
	c, err := store.WatchByPartitions(apiOp, schema, wr, partitions)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(response)

		for i := range c {
			response <- toAPIEvent(schema, i)
		}
	}()

	return response, nil
}

func toAPI(schema *types.APISchema, obj runtime.Object, warnings []types.Warning) types.APIObject {
	if obj == nil || reflect.ValueOf(obj).IsNil() {
		return types.APIObject{}
	}

	if unstr, ok := obj.(*unstructured.Unstructured); ok {
		obj = moveToUnderscore(unstr)
	}

	apiObject := types.APIObject{
		Type:   schema.ID,
		Object: obj,
	}

	m, err := meta.Accessor(obj)
	if err != nil {
		return apiObject
	}

	id := m.GetName()
	ns := m.GetNamespace()
	if ns != "" {
		id = fmt.Sprintf("%s/%s", ns, id)
	}

	apiObject.ID = id
	apiObject.Warnings = warnings
	return apiObject
}

func moveToUnderscore(obj *unstructured.Unstructured) *unstructured.Unstructured {
	if obj == nil {
		return nil
	}

	for k := range types.ReservedFields {
		v, ok := obj.Object[k]
		if ok {
			delete(obj.Object, k)
			obj.Object["_"+k] = v
		}
	}

	return obj
}

func toAPIEvent(schema *types.APISchema, event watch.Event) types.APIEvent {
	name := types.ChangeAPIEvent
	switch event.Type {
	case watch.Deleted:
		name = types.RemoveAPIEvent
	case watch.Added:
		name = types.CreateAPIEvent
	case watch.Error:
		name = "resource.error"
	}

	apiEvent := types.APIEvent{
		Name: name,
	}

	if event.Type == watch.Error {
		status, _ := event.Object.(*metav1.Status)
		apiEvent.Error = fmt.Errorf(status.Message)
		return apiEvent
	}

	apiEvent.Object = toAPI(schema, event.Object, nil)

	m, err := meta.Accessor(event.Object)
	if err != nil {
		return apiEvent
	}

	apiEvent.Revision = m.GetResourceVersion()
	return apiEvent
}
