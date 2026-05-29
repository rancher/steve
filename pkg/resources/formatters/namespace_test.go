package formatters

import (
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestNamespace(t *testing.T) {
	tests := []struct {
		name           string
		namespace      string
		hasGetAccess   bool
		expectStripped bool
	}{
		{
			name:           "user with get access sees full namespace",
			namespace:      "default",
			hasGetAccess:   true,
			expectStripped: false,
		},
		{
			name:           "user without get access sees stripped namespace",
			namespace:      "default",
			hasGetAccess:   false,
			expectStripped: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a namespace object with metadata
			obj := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Namespace",
					"metadata": map[string]interface{}{
						"name":              tt.namespace,
						"resourceVersion":   "12345",
						"creationTimestamp": "2024-01-01T00:00:00Z",
						"uid":               "abc-123-def",
						"labels": map[string]interface{}{
							"env":                   "production",
							"fleet.cattle.io/managed": "true",
						},
						"annotations": map[string]interface{}{
							"description":                           "sensitive info",
							"management.cattle.io/system-namespace": "true",
							"field.cattle.io/projectId":             "local:p-12345",
						},
					},
					"spec": map[string]interface{}{
						"finalizers": []interface{}{"kubernetes"},
					},
					"status": map[string]interface{}{
						"phase": "Active",
					},
				},
			}

			// Create mock access set
			accessSet := &accesscontrol.AccessSet{}
			if tt.hasGetAccess {
				accessSet.Add("get", namespaceGR, accesscontrol.Access{
					Namespace:    accesscontrol.All,
					ResourceName: tt.namespace,
				})
			}

			// Create request with access set on schemas
			schemas := types.EmptyAPISchemas()
			accesscontrol.SetAccessSetAttribute(schemas, accessSet)
			request := &types.APIRequest{
				Schemas: schemas,
			}

			resource := &types.RawResource{
				APIObject: types.APIObject{Object: obj},
			}

			// Run formatter
			Namespace(request, resource)

			// Check results
			resultObj := resource.APIObject.Object.(*unstructured.Unstructured)
			metadata := resultObj.Object["metadata"].(map[string]interface{})

			if tt.expectStripped {
				// Should have name
				assert.Equal(t, tt.namespace, metadata["name"])

				// Should have resourceVersion (needed for watch/caching)
				assert.Equal(t, "12345", metadata["resourceVersion"])

				// Should have allowed annotations
				annotations, hasAnnotations := metadata["annotations"].(map[string]interface{})
				assert.True(t, hasAnnotations, "should have annotations")
				assert.Equal(t, "true", annotations["management.cattle.io/system-namespace"])
				assert.Equal(t, "local:p-12345", annotations["field.cattle.io/projectId"])
				assert.NotContains(t, annotations, "description", "sensitive annotation should be stripped")

				// Should have allowed labels
				labels, hasLabels := metadata["labels"].(map[string]interface{})
				assert.True(t, hasLabels, "should have labels")
				assert.Equal(t, "true", labels["fleet.cattle.io/managed"])
				assert.NotContains(t, labels, "env", "sensitive label should be stripped")

				// Should have status.phase
				status, hasStatus := resultObj.Object["status"].(map[string]interface{})
				assert.True(t, hasStatus, "should have status")
				assert.Equal(t, "Active", status["phase"])

				// Should NOT have sensitive fields
				assert.NotContains(t, metadata, "creationTimestamp", "creationTimestamp should be stripped")
				assert.NotContains(t, metadata, "uid", "uid should be stripped")
				// spec and status should be empty objects (not omitted)
				spec := resultObj.Object["spec"].(map[string]interface{})
				assert.Empty(t, spec, "spec should be empty")
			} else {
				// Should have full object
				assert.Equal(t, tt.namespace, metadata["name"])
				labels := metadata["labels"].(map[string]interface{})
				assert.Equal(t, "production", labels["env"], "all labels should be present")
				annotations := metadata["annotations"].(map[string]interface{})
				assert.Equal(t, "sensitive info", annotations["description"], "all annotations should be present")
				assert.Contains(t, metadata, "creationTimestamp", "creationTimestamp should be present")
				assert.Contains(t, metadata, "uid", "uid should be present")
			}
		})
	}
}

func TestNamespace_NilAccessSet(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata": map[string]interface{}{
				"name": "default",
				"labels": map[string]interface{}{
					"env": "production",
				},
			},
		},
	}

	// Request without access set
	request := &types.APIRequest{}
	resource := &types.RawResource{
		APIObject: types.APIObject{Object: obj},
	}

	// Should not panic and not modify object
	Namespace(request, resource)

	resultObj := resource.APIObject.Object.(*unstructured.Unstructured)
	metadata := resultObj.Object["metadata"].(map[string]interface{})
	assert.Contains(t, metadata, "labels", "object should not be modified when access set is nil")
}

func TestNamespace_MinimalObject(t *testing.T) {
	// Test with a namespace that has no optional fields
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata": map[string]interface{}{
				"name": "minimal-ns",
			},
		},
	}

	accessSet := &accesscontrol.AccessSet{}
	schemas := types.EmptyAPISchemas()
	accesscontrol.SetAccessSetAttribute(schemas, accessSet)
	request := &types.APIRequest{
		Schemas: schemas,
	}

	resource := &types.RawResource{
		APIObject: types.APIObject{Object: obj},
	}

	Namespace(request, resource)

	resultObj := resource.APIObject.Object.(*unstructured.Unstructured)
	metadata := resultObj.Object["metadata"].(map[string]interface{})

	// Should have name
	assert.Equal(t, "minimal-ns", metadata["name"])

	// Should have empty annotations/labels (not omitted, to avoid nil access in UI)
	annotations := metadata["annotations"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	assert.Empty(t, annotations, "annotations should be empty")
	assert.Empty(t, labels, "labels should be empty")
}

func TestNamespace_OnlyAllowedAnnotations(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata": map[string]interface{}{
				"name": "test-ns",
				"annotations": map[string]interface{}{
					"custom-annotation":                     "should be stripped",
					"another.io/secret":                     "also stripped",
					"management.cattle.io/system-namespace": "true",
					"field.cattle.io/projectId":             "local:p-abc",
				},
				"labels": map[string]interface{}{
					"custom-label":            "should be stripped",
					"fleet.cattle.io/managed": "true",
				},
			},
		},
	}

	accessSet := &accesscontrol.AccessSet{}
	schemas := types.EmptyAPISchemas()
	accesscontrol.SetAccessSetAttribute(schemas, accessSet)
	request := &types.APIRequest{
		Schemas: schemas,
	}

	resource := &types.RawResource{
		APIObject: types.APIObject{Object: obj},
	}

	Namespace(request, resource)

	resultObj := resource.APIObject.Object.(*unstructured.Unstructured)
	metadata := resultObj.Object["metadata"].(map[string]interface{})

	annotations := metadata["annotations"].(map[string]interface{})
	assert.Len(t, annotations, 2, "should only have 2 allowed annotations")
	assert.Equal(t, "true", annotations["management.cattle.io/system-namespace"])
	assert.Equal(t, "local:p-abc", annotations["field.cattle.io/projectId"])

	labels := metadata["labels"].(map[string]interface{})
	assert.Len(t, labels, 1, "should only have 1 allowed label")
	assert.Equal(t, "true", labels["fleet.cattle.io/managed"])
}
