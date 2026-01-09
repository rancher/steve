package virtual

import (
	"fmt"
	"strings"
	"testing"
	"time"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/resources/virtual/common"
	"github.com/rancher/steve/pkg/summarycache"
	"github.com/rancher/wrangler/v3/pkg/summary"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/jsonpath"
)

func TestTransformChain(t *testing.T) {
	now = func() time.Time { return time.Date(1992, 9, 2, 0, 0, 0, 0, time.UTC) }
	noColumns := []rescommon.ColumnDefinition{}
	jp := jsonpath.New("Age")
	jp.AllowMissingKeys(true)
	jp.Parse("{.metadata.creationTimestamp}")

	tests := []struct {
		name             string
		input            any
		hasSummary       *summary.SummarizedObject
		hasRelationships []summarycache.Relationship
		columns          []rescommon.ColumnDefinition
		isCRD            bool
		jsonPaths        map[string]*jsonpath.JSONPath
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
			columns: noColumns,
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
			name: "CRD metadata.fields has a date field - should convert to timestamp",
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
				},
			},
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "test.cattle.io/v1",
					"kind":       "TestResource",
					"metadata": map[string]interface{}{
						"name":      "testobj",
						"namespace": "test-ns",
						"fields":    []interface{}{"1d"},
					},
					"id": "old-id",
				},
			},
			isCRD: true,
			columns: []rescommon.ColumnDefinition{
				{
					Field: "metadata.fields[0]",
					TableColumnDefinition: v1.TableColumnDefinition{
						Name: "Age",
						Type: "date",
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
						"state": map[string]interface{}{
							"name":          "success",
							"error":         false,
							"transitioning": false,
							"message":       "",
						},
						"fields": []interface{}{
							fmt.Sprintf("%d", now().Add(-24*time.Hour).UnixMilli()),
						},
					},
					"id":  "test-ns/testobj",
					"_id": "old-id",
				},
			},
		},
		{
			name: "CRD with jsonPath - should use jsonPath value instead of metadata.fields",
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
				},
			},
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "test.cattle.io/v1",
					"kind":       "TestResource",
					"metadata": map[string]interface{}{
						"name":              "testobj",
						"namespace":         "test-ns",
						"creationTimestamp": "2023-01-01T00:00:00Z", // valid RFC3339
						"fields":            []interface{}{"99d"},   // stale value
					},
					"id": "old-id",
				},
			},
			isCRD: true,
			columns: []rescommon.ColumnDefinition{
				{
					Field: "metadata.fields[0]",
					TableColumnDefinition: v1.TableColumnDefinition{
						Name: "Age",
						Type: "date",
					},
				},
			},
			jsonPaths: map[string]*jsonpath.JSONPath{
				"Age": jp,
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "test.cattle.io/v1",
					"kind":       "TestResource",
					"metadata": map[string]interface{}{
						"name":              "testobj",
						"namespace":         "test-ns",
						"creationTimestamp": "2023-01-01T00:00:00Z",
						"relationships":     []any(nil),
						"state": map[string]interface{}{
							"name":          "success",
							"error":         false,
							"transitioning": false,
							"message":       "",
						},
						"fields": []interface{}{
							"2023-01-01T00:00:00Z", // Should be preserved as is
						},
					},
					"id":  "test-ns/testobj",
					"_id": "old-id",
				},
			},
		},
		{
			name: "built-in type metadata.fields has a date field - should convert to timestamp",
			hasSummary: &summary.SummarizedObject{
				PartialObjectMetadata: v1.PartialObjectMetadata{
					ObjectMeta: v1.ObjectMeta{
						Name:      "testobj",
						Namespace: "test-ns",
					},
					TypeMeta: v1.TypeMeta{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
					},
				},
				Summary: summary.Summary{
					State:         "success",
					Transitioning: false,
					Error:         false,
				},
			},
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]interface{}{
						"name":      "testobj",
						"namespace": "test-ns",
						"fields":    []interface{}{"1d"},
					},
					"id": "old-id",
				},
			},
			columns: []rescommon.ColumnDefinition{
				{
					TableColumnDefinition: v1.TableColumnDefinition{
						Name: "Age",
					},
					Field: "metadata.fields[0]",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]interface{}{
						"name":          "testobj",
						"namespace":     "test-ns",
						"relationships": []any(nil),
						"state": map[string]interface{}{
							"name":          "success",
							"error":         false,
							"transitioning": false,
							"message":       "",
						},
						"fields": []interface{}{
							fmt.Sprintf("%d", now().Add(-24*time.Hour).UnixMilli()),
						},
					},
					"id":  "test-ns/testobj",
					"_id": "old-id",
				},
			},
		},
		{
			name: "built-in type metadata.fields has a date field - should NOT convert to timestamp",
			hasSummary: &summary.SummarizedObject{
				PartialObjectMetadata: v1.PartialObjectMetadata{
					ObjectMeta: v1.ObjectMeta{
						Name:      "testobj",
						Namespace: "test-ns",
					},
					TypeMeta: v1.TypeMeta{
						APIVersion: "apiextensions.k8s.io/v1",
						Kind:       "CustomResourceDefinition",
					},
				},
				Summary: summary.Summary{
					State:         "success",
					Transitioning: false,
					Error:         false,
				},
			},
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apiextensions.k8s.io/v1",
					"kind":       "CustomResourceDefinition",
					"metadata": map[string]interface{}{
						"name":      "testobj",
						"namespace": "test-ns",
						"fields":    []interface{}{"2025-07-03T18:54:57Z"},
					},
					"id": "old-id",
				},
			},
			columns: []rescommon.ColumnDefinition{
				{
					TableColumnDefinition: v1.TableColumnDefinition{
						Name: "Created At",
						Type: "date",
					},
					Field: "metadata.fields[0]",
				},
			},
			wantOutput: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apiextensions.k8s.io/v1",
					"kind":       "CustomResourceDefinition",
					"metadata": map[string]interface{}{
						"name":          "testobj",
						"namespace":     "test-ns",
						"relationships": []any(nil),
						"state": map[string]interface{}{
							"name":          "success",
							"error":         false,
							"transitioning": false,
							"message":       "",
						},
						"fields": []interface{}{
							"2025-07-03T18:54:57Z",
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
			columns: noColumns,
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
			columns: noColumns,
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
			columns: noColumns,
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
			columns: noColumns,
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
			tb := NewTransformBuilder(&fakeCache)
			raw, isSignal, err := common.GetUnstructured(test.input)
			require.False(t, isSignal)
			require.Nil(t, err)
			apiVersion := raw.GetAPIVersion()
			parts := strings.Split(apiVersion, "/")
			gvk := schema.GroupVersionKind{Group: parts[0], Version: parts[1], Kind: raw.GetKind()}
			output, err := tb.GetTransformFunc(gvk, test.columns, test.isCRD, test.jsonPaths)(test.input)
			require.Equal(t, test.wantOutput, output)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
