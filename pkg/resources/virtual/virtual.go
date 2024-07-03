// Package virtual provides functions/resources to define virtual fields (fields which don't exist in k8s
// but should be visible in the API) on resources
package virtual

import (
	"github.com/rancher/steve/pkg/resources/virtual/common"
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
		&common.DefaultFields{
			Cache: cache,
		},
	}
}

// GetTransformFunc retrieves a TransformFunc for a given GVK. Currently only returns a transformFunc for defaultFields
func (t *TransformBuilder) GetTransformFunc(_ schema.GroupVersionKind) cache.TransformFunc {
	return t.defaultFields.GetTransform()
}
