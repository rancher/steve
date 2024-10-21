package common_test

import (
	"testing"

	"github.com/rancher/steve/pkg/resources/virtual/common"
	"github.com/rancher/steve/pkg/summarycache"
	"github.com/rancher/wrangler/v3/pkg/summary"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

func TestTransformCommonObjects(t *testing.T) {
	tests := []struct {
		name             string
		input            any
		hasSummary       *summary.SummarizedObject
		hasRelationships []summarycache.Relationship
		wantOutput       any
		wantError        bool
	}{
		{
			name: "signal error",
			input: cache.DeletedFinalStateUnknown{
				Key: "some-ns/some-name",
			},
			wantOutput: cache.DeletedFinalStateUnknown{
				Key: "some-ns/some-name",
			},
			wantError: false,
		},
		{
			name: "not unstructured",
			input: map[string]any{
				"somekey": "someval",
			},
			wantError: true,
		},
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
			name: "add conditions + reserved fields",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "test.cattle.io/v1",
					"kind":       "TestResource",
					"metadata": map[string]interface{}{
						"name":      "testobj",
						"namespace": "test-ns",
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
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "test.cattle.io/v1",
					"kind":       "TestResource",
					"metadata": map[string]interface{}{
						"name":          "testobj",
						"namespace":     "test-ns",
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
					"id": "test-ns/testobj",
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
			df := common.DefaultFields{
				Cache: &fakeCache,
			}
			raw, isSignal, err := common.GetUnstructured(test.input)
			if err != nil {
				require.True(t, test.wantError)
				return
			}
			if isSignal {
				require.Equal(t, test.input, test.wantOutput)
				return
			}
			output, err := df.TransformCommon(raw)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.Equal(t, test.wantOutput, output)
				require.NoError(t, err)
			}
		})
	}
}
