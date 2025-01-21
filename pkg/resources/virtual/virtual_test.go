package virtual_test

import (
	"fmt"
	"github.com/rancher/steve/pkg/resources/virtual"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"strings"
	"testing"

	"github.com/rancher/steve/pkg/resources/virtual/common"
	"github.com/rancher/steve/pkg/summarycache"
	"github.com/rancher/wrangler/v3/pkg/summary"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestTransformChain(t *testing.T) {
	tests := []struct {
		name             string
		input            any
		hasSummary       *summary.SummarizedObject
		hasRelationships []summarycache.Relationship
		wantOutput       any
		wantError        bool
	}{
		{
			name: "add summary + relationships + reserved fields",
			hasSummary: &summary.SummarizedObject{
				PartialObjectMetadata: v1.PartialObjectMetadata{
					ObjectMeta: v1.ObjectMeta{
						Name:      "testobj",
						Namespace: "test-ns",
					},
					TypeMeta: v1.TypeMeta{
						APIVersion: "test.cattle.io/v1",
						Kind:       "TestResource",
					},
				},
				Summary: summary.Summary{
					State:         "success",
					Transitioning: false,
					Error:         false,
					Message:       []string{"resource 1 rolled out", "resource 2 rolled out"},
				},
			},
			hasRelationships: []summarycache.Relationship{
				{
					ToID:        "1345",
					ToType:      "SomeType",
					ToNamespace: "some-ns",
					FromID:      "78901",
					FromType:    "TestResource",
					Rel:         "uses",
				},
			},
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "test.cattle.io/v1",
					"kind":       "TestResource",
					"metadata": map[string]interface{}{
						"name":      "testobj",
						"namespace": "test-ns",
					},
					"id": "old-id",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "test.cattle.io/v1",
					"kind":       "TestResource",
					"metadata": map[string]interface{}{
						"name":      "testobj",
						"namespace": "test-ns",
						"state": map[string]interface{}{
							"name":          "success",
							"error":         false,
							"transitioning": false,
							"message":       "resource 1 rolled out:resource 2 rolled out",
						},
						"relationships": []any{
							map[string]any{
								"toId":        "1345",
								"toType":      "SomeType",
								"toNamespace": "some-ns",
								"fromId":      "78901",
								"fromType":    "TestResource",
								"rel":         "uses",
							},
						},
					},
					"id":  "test-ns/testobj",
					"_id": "old-id",
				},
			},
		},
		{
			name: "processable event",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "/v1",
					"kind":       "Event",
					"metadata": map[string]interface{}{
						"name":      "oswaldsFarm",
						"namespace": "oswaldsNamespace",
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"status":             "False",
								"reason":             "Error",
								"message":            "some error",
								"lastTransitionTime": "2024-01-01",
							},
						},
					},
					"id":   "eventTest2id",
					"type": "Gorniplatz",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "/v1",
					"kind":       "Event",
					"metadata": map[string]interface{}{
						"name":          "oswaldsFarm",
						"namespace":     "oswaldsNamespace",
						"relationships": []any(nil),
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"status":             "False",
								"reason":             "Error",
								"transitioning":      false,
								"error":              true,
								"message":            "some error",
								"lastTransitionTime": "2024-01-01",
								"lastUpdateTime":     "2024-01-01",
							},
						},
					},
					"id":    "oswaldsNamespace/oswaldsFarm",
					"_id":   "eventTest2id",
					"type":  "Gorniplatz",
					"_type": "Gorniplatz",
				},
			},
		},
		{
			name: "don't fix non-default-group event fields",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "palau.io/v1",
					"kind":       "Event",
					"metadata": map[string]interface{}{
						"name":          "gregsFarm",
						"namespace":     "gregsNamespace",
						"relationships": []any(nil),
					},
					"id":   "eventTest1id",
					"type": "Gorniplatz",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "palau.io/v1",
					"kind":       "Event",
					"metadata": map[string]interface{}{
						"name":          "gregsFarm",
						"namespace":     "gregsNamespace",
						"relationships": []any(nil),
					},
					"id":   "gregsNamespace/gregsFarm",
					"_id":  "eventTest1id",
					"type": "Gorniplatz",
				},
			},
		},
		{
			name: "a non-ready cluster",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "management.cattle.io/v3",
					"kind":       "Cluster",
					"id":         1,
					"metadata": map[string]interface{}{
						"name": "c-m-boris",
					},
					"spec": map[string]interface{}{
						"displayName": "boris",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "DefaultProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "SystemProjectCreated",
							},
						},
					},
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "management.cattle.io/v3",
					"kind":       "Cluster",
					"id":         "c-m-boris",
					"_id":        1,
					"metadata": map[string]interface{}{
						"name":          "c-m-boris",
						"relationships": []any(nil),
					},
					"spec": map[string]interface{}{
						"displayName": "boris",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "DefaultProjectCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "SystemProjectCreated",
							},
						},
						"connected": false,
					},
				},
			},
		},
		{
			name: "a ready cluster",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "management.cattle.io/v3",
					"kind":       "Cluster",
					"id":         2,
					"metadata": map[string]interface{}{
						"name": "c-m-natasha",
					},
					"spec": map[string]interface{}{
						"displayName": "natasha",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "Ready",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "SystemProjectCreated",
							},
						},
					},
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "management.cattle.io/v3",
					"kind":       "Cluster",
					"id":         "c-m-natasha",
					"_id":        2,
					"metadata": map[string]interface{}{
						"name":          "c-m-natasha",
						"relationships": []any(nil),
					},
					"spec": map[string]interface{}{
						"displayName": "natasha",
						"internal":    false,
					},
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "BackingNamespaceCreated",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "Ready",
							},
							map[string]interface{}{
								"error":          false,
								"lastUpdateTime": "2025-01-10T22:52:16Z",
								"status":         "True",
								"transitioning":  false,
								"type":           "SystemProjectCreated",
							},
						},
						"connected": true,
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeCache := common.FakeSummaryCache{
				SummarizedObject: test.hasSummary,
				Relationships:    test.hasRelationships,
			}
			tb := virtual.NewTransformBuilder(&fakeCache)
			raw, isSignal, err := common.GetUnstructured(test.input)
			require.False(t, isSignal)
			require.Nil(t, err)
			apiVersion := raw.GetAPIVersion()
			parts := strings.Split(apiVersion, "/")
			gvk := schema.GroupVersionKind{Group: parts[0], Version: parts[1], Kind: raw.GetKind()}
			if test.name == "a non-ready cluster" {
				fmt.Printf("Stop here")
			}
			output, err := tb.GetTransformFunc(gvk)(test.input)
			require.Equal(t, test.wantOutput, output)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
