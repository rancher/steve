package schematracker

import (
	"context"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

type testResetter struct {
	Resets map[k8sschema.GroupVersionKind]struct{}
}

func (r *testResetter) Reset(gvk k8sschema.GroupVersionKind) error {
	if r.Resets == nil {
		r.Resets = make(map[k8sschema.GroupVersionKind]struct{})
	}
	r.Resets[gvk] = struct{}{}
	return nil
}

func TestSchemaTracker(t *testing.T) {
	pods := &types.APISchema{
		Schema: &schemas.Schema{ID: "pods"},
	}
	attributes.SetGVK(pods, k8sschema.GroupVersionKind{
		Version: "v1",
		Kind:    "Pod",
	})
	attributes.SetGVR(pods, k8sschema.GroupVersionResource{
		Version:  "v1",
		Resource: "pods",
	})

	configmaps := &types.APISchema{
		Schema: &schemas.Schema{ID: "configmaps"},
	}
	attributes.SetGVK(configmaps, k8sschema.GroupVersionKind{
		Version: "v1",
		Kind:    "ConfigMap",
	})
	attributes.SetGVR(configmaps, k8sschema.GroupVersionResource{
		Version:  "v1",
		Resource: "configmaps",
	})

	foo := &types.APISchema{
		Schema: &schemas.Schema{ID: "test.io.foos"},
	}
	attributes.SetGVK(foo, k8sschema.GroupVersionKind{
		Group:   "test.io",
		Version: "v1",
		Kind:    "Foo",
	})
	attributes.SetGVR(foo, k8sschema.GroupVersionResource{
		Group:    "test.io",
		Version:  "v1",
		Resource: "foos",
	})

	foos1 := &types.APISchema{
		Schema: &schemas.Schema{ID: "test.io.foos"},
	}
	attributes.SetGVK(foos1, k8sschema.GroupVersionKind{
		Group:   "test.io",
		Version: "v1",
		Kind:    "Foo",
	})
	attributes.SetGVR(foos1, k8sschema.GroupVersionResource{
		Group:    "test.io",
		Version:  "v1",
		Resource: "foos",
	})
	attributes.SetColumns(foos1, []common.ColumnDefinition{
		{Field: "field1"}, {Field: "field2"},
	})

	foos2 := &types.APISchema{
		Schema: &schemas.Schema{ID: "test.io.foos"},
	}
	attributes.SetGVK(foos2, k8sschema.GroupVersionKind{
		Group:   "test.io",
		Version: "v1",
		Kind:    "Foo",
	})
	attributes.SetGVR(foos2, k8sschema.GroupVersionResource{
		Group:    "test.io",
		Version:  "v1",
		Resource: "foos",
	})
	attributes.SetColumns(foos2, []common.ColumnDefinition{
		{Field: "field1"}, {Field: "field2"}, {Field: "field3"},
	})

	bars := &types.APISchema{
		Schema: &schemas.Schema{ID: "test.io.bars"},
	}
	attributes.SetGVK(bars, k8sschema.GroupVersionKind{
		Group:   "test.io",
		Version: "v1",
		Kind:    "Bar",
	})
	attributes.SetGVR(bars, k8sschema.GroupVersionResource{
		Group:    "test.io",
		Version:  "v1",
		Resource: "bars",
	})

	tests := []struct {
		name             string
		initialSchemas   map[string]*types.APISchema
		refreshedSchemas map[string]*types.APISchema
		expectedResets   map[k8sschema.GroupVersionKind]struct{}
	}{
		{
			name: "no change",
			initialSchemas: map[string]*types.APISchema{
				"configmaps": configmaps,
			},
			refreshedSchemas: map[string]*types.APISchema{
				"configmaps": configmaps,
			},
		},
		{
			name: "single schema added",
			initialSchemas: map[string]*types.APISchema{
				"configmaps": configmaps,
			},
			refreshedSchemas: map[string]*types.APISchema{
				"configmaps": configmaps,
				"pods":       pods,
			},
			expectedResets: map[k8sschema.GroupVersionKind]struct{}{
				attributes.GVK(pods): {},
			},
		},
		{
			name:           "multiple schemas added",
			initialSchemas: map[string]*types.APISchema{},
			refreshedSchemas: map[string]*types.APISchema{
				"configmaps": configmaps,
				"pods":       pods,
			},
			expectedResets: map[k8sschema.GroupVersionKind]struct{}{
				attributes.GVK(configmaps): {},
				attributes.GVK(pods):       {},
			},
		},
		{
			name: "single schema removed",
			initialSchemas: map[string]*types.APISchema{
				"configmaps": configmaps,
				"pods":       pods,
			},
			refreshedSchemas: map[string]*types.APISchema{
				"pods": pods,
			},
			expectedResets: map[k8sschema.GroupVersionKind]struct{}{
				attributes.GVK(configmaps): {},
			},
		},
		{
			name: "multiple schemas removed",
			initialSchemas: map[string]*types.APISchema{
				"configmaps": configmaps,
				"pods":       pods,
			},
			refreshedSchemas: map[string]*types.APISchema{},
			expectedResets: map[k8sschema.GroupVersionKind]struct{}{
				attributes.GVK(configmaps): {},
				attributes.GVK(pods):       {},
			},
		},
		{
			name: "field changed",
			initialSchemas: map[string]*types.APISchema{
				"test.io.foos": foos1,
			},
			refreshedSchemas: map[string]*types.APISchema{
				"test.io.foos": foos2,
			},
			expectedResets: map[k8sschema.GroupVersionKind]struct{}{
				attributes.GVK(foos2): {},
			},
		},
		{
			name: "added deleted and changed",
			initialSchemas: map[string]*types.APISchema{
				"configmaps":   configmaps,
				"pods":         pods,
				"test.io.foos": foos1,
			},
			refreshedSchemas: map[string]*types.APISchema{
				"configmaps":   configmaps,
				"test.io.bars": bars,
				"test.io.foos": foos2,
			},
			expectedResets: map[k8sschema.GroupVersionKind]struct{}{
				attributes.GVK(foos2): {},
				attributes.GVK(pods):  {},
				attributes.GVK(bars):  {},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetter := &testResetter{}
			tracker := NewSchemaTracker(resetter)
			collection := schema.NewCollection(context.TODO(), types.EmptyAPISchemas(), nil)

			collection.Reset(test.initialSchemas)
			err := tracker.OnSchemas(collection)
			assert.NoError(t, err)

			// Reset because we don't care about the initial list of resets
			resetter.Resets = nil

			collection.Reset(test.refreshedSchemas)
			err = tracker.OnSchemas(collection)
			assert.NoError(t, err)

			assert.Equal(t, test.expectedResets, resetter.Resets)
		})
	}
}
