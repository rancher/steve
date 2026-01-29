package multivalue

import (
	"testing"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/resources/virtual/multivalue/parsers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestConverter_Transform(t *testing.T) {
	podGVK := schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "Pod",
	}

	columns := []rescommon.ColumnDefinition{
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Name"}, Field: "metadata.fields[0]"},
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Ready"}, Field: "metadata.fields[1]"},
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Status"}, Field: "metadata.fields[2]"},
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Restarts"}, Field: "metadata.fields[3]"},
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Age"}, Field: "metadata.fields[4]"},
	}

	converter := &Converter{
		Columns: columns,
		Fields: []FieldConfig{
			{
				GVK:        podGVK,
				ColumnName: "Restarts",
				ParseFunc:  parsers.ParseRestarts,
			},
		},
	}

	tests := []struct {
		name          string
		inputFields   []interface{}
		expectedField interface{}
	}{
		{
			name:          "parse restart with time",
			inputFields:   []interface{}{"my-pod", "1/1", "Running", "4 (3h38m ago)", "5d"},
			expectedField: []interface{}{int64(4), int64(0)}, // timestamp will vary, just check structure
		},
		{
			name:          "parse restart without time",
			inputFields:   []interface{}{"my-pod", "1/1", "Running", "0", "5d"},
			expectedField: []interface{}{int64(0), nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata": map[string]interface{}{
						"name":   "test-pod",
						"fields": tt.inputFields,
					},
				},
			}

			result, err := converter.Transform(obj)
			require.NoError(t, err)

			fields, found, err := unstructured.NestedSlice(result.Object, "metadata", "fields")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, fields, 5)

			// Check that field[3] is now an array (COMPOSITE_INT fields are stored as arrays)
			restartField, ok := fields[3].([]interface{})
			require.True(t, ok, "field should be array")
			require.Len(t, restartField, 2)

			// Check count (first element)
			expectedCount := tt.expectedField.([]interface{})[0].(int64)
			actualCount, ok := restartField[0].(int64)
			require.True(t, ok)
			assert.Equal(t, expectedCount, actualCount)
		})
	}
}
