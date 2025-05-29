// Package virtual provides functions/resources to define virtual fields (fields which don't exist in k8s
// but should be visible in the API) on resources
package virtual

import (
	"fmt"
	"time"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/resources/virtual/clusters"
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
func (t *TransformBuilder) GetTransformFunc(gvk schema.GroupVersionKind, columns []rescommon.ColumnDefinition) cache.TransformFunc {
	converters := make([]func(*unstructured.Unstructured) (*unstructured.Unstructured, error), 0)
	if gvk.Kind == "Event" && gvk.Group == "" && gvk.Version == "v1" {
		converters = append(converters, events.TransformEventObject)
	} else if gvk.Kind == "Cluster" && gvk.Group == "management.cattle.io" && gvk.Version == "v3" {
		converters = append(converters, clusters.TransformManagedCluster)
	}
	converters = append(converters, t.defaultFields.TransformCommon)

	// Detecting if we need to convert date fields
	for _, col := range columns {
		if col.Type == "date" {
			converters = append(converters, func(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
				index := rescommon.GetIndexValueFromString(col.Field)
				curValue, got, err := unstructured.NestedSlice(obj.Object, "metadata", "fields")
				if !got {
					return obj, nil
				}

				if err != nil {
					return nil, err
				}

				duration, err := time.ParseDuration(curValue[index].(string))
				if err != nil {
					return nil, err
				}

				curValue[index] = fmt.Sprintf("%d", time.Now().Add(-duration).UnixMilli())
				if err := unstructured.SetNestedSlice(obj.Object, curValue, "metadata", "fields"); err != nil {
					return nil, err
				}

				return obj, nil
			})
		}
	}

	return func(raw interface{}) (interface{}, error) {
		obj, isSignal, err := common.GetUnstructured(raw)
		if isSignal {
			// isSignal= true overrides any error
			return raw, err
		}
		if err != nil {
			return nil, fmt.Errorf("GetUnstructured: failed to get underlying object: %w", err)
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
