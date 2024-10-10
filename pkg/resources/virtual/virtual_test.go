package virtual_test

import (
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
