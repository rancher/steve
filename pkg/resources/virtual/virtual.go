// Package virtual provides functions/resources to define virtual fields (fields which don't exist in k8s
// but should be visible in the API) on resources
package virtual

import (
	"errors"

	"github.com/rancher/steve/pkg/resources/virtual/common"
	"github.com/rancher/steve/pkg/resources/virtual/events"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// TransformBuilder builds transform functions for specified GVKs through GetTransformFunc
type TransformBuilder struct {
	defaultFields *common.DefaultFields
}

// NewTransformBuilder returns a TransformBuilder using the given summary cache
func NewTransformBuilder(cache common.SummaryCache) *TransformBuilder {
	return &TransformBuilder{
		defaultFields: &common.DefaultFields{
			Cache: cache,
		},
	}
}

// GetTransformFunc returns the func to transform a raw object into a fixed object, if needed
func (t *TransformBuilder) GetTransformFunc(gvk schema.GroupVersionKind) cache.TransformFunc {
	converters := make([]func(*unstructured.Unstructured) (*unstructured.Unstructured, error), 0)
	if gvk.Kind == "Event" && gvk.Group == "" && gvk.Version == "v1" {
		converters = append(converters, events.TransformEventObject)
	}
	converters = append(converters, t.defaultFields.TransformCommon)

	return func(any interface{}) (interface{}, error) {
		raw, isSignal, err := t.defaultFields.GetUnstructuredObjectWrapper(any)
		if isSignal {
			return raw, err
		}
		obj, ok := raw.(*unstructured.Unstructured)
		if !ok {
			return raw, errors.New("failed to cast raw object to unstructured")
		}
		// Conversions are run in this loop:
		for _, f := range converters {
			obj, err = f(obj)
			if err != nil {
				return nil, err
			}
		}
		return obj, nil
	}
}
