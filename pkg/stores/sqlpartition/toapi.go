package sqlpartition

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/rancher/apiserver/pkg/types"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// ToAPI converts a kubernetes object into an APIObject, moving any field
// colliding with a reserved field to its underscore-prefixed name.
func ToAPI(schema *types.APISchema, obj runtime.Object, warnings []types.Warning, reservedFields map[string]bool) types.APIObject {
	if obj == nil || reflect.ValueOf(obj).IsNil() {
		return types.APIObject{}
	}

	if unstr, ok := obj.(*unstructured.Unstructured); ok {
		obj = moveToUnderscore(unstr, reservedFields)
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

func moveToUnderscore(obj *unstructured.Unstructured, reservedFields map[string]bool) *unstructured.Unstructured {
	if obj == nil {
		return nil
	}

	for k := range reservedFields {
		v, ok := obj.Object[k]
		if ok {
			delete(obj.Object, k)
			obj.Object["_"+k] = v
		}
	}

	return obj
}

// ToAPIEvent converts a watch event into an APIEvent.
func ToAPIEvent(apiOp *types.APIRequest, schema *types.APISchema, event watch.Event) types.APIEvent {
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
		apiEvent.Error = errors.New(status.Message)
		return apiEvent
	}

	apiEvent.Object = ToAPI(schema, event.Object, nil, types.ReservedFields)

	m, err := meta.Accessor(event.Object)
	if err != nil {
		return apiEvent
	}

	apiEvent.Revision = m.GetResourceVersion()
	return apiEvent
}
