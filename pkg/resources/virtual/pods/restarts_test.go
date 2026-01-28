package pods

import (
	"fmt"
	"testing"
	"time"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestConverter_Transform(t *testing.T) {
	// Mock time for deterministic tests
	fixedTime := time.Date(2024, 1, 26, 12, 0, 0, 0, time.UTC)
	Now = func() time.Time { return fixedTime }

	tests := []struct {
		name           string
		inputFields    []interface{}
		columns        []rescommon.ColumnDefinition
		expectedFields []interface{}
		expectError    bool
	}{
		{
			name: "transform restart field with 5m ago",
			inputFields: []interface{}{
				"test-pod",
				"Running",
				"0",
				"1 (5m ago)",
			},
			columns: []rescommon.ColumnDefinition{
				{
					Field:                 "metadata.fields[0]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Name"},
				},
				{
					Field:                 "metadata.fields[1]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Status"},
				},
				{
					Field:                 "metadata.fields[2]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Ready"},
				},
				{
					Field:                 "metadata.fields[3]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Restarts"},
				},
			},
			expectedFields: []interface{}{
				"test-pod",
				"Running",
				"0",
				"1|" + fmt.Sprint(fixedTime.Add(-5*time.Minute).UnixMilli()),
			},
			expectError: false,
		},
		{
			name: "transform restart field with 0 restarts",
			inputFields: []interface{}{
				"test-pod",
				"Running",
				"1/1",
				"0",
			},
			columns: []rescommon.ColumnDefinition{
				{
					Field:                 "metadata.fields[0]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Name"},
				},
				{
					Field:                 "metadata.fields[1]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Status"},
				},
				{
					Field:                 "metadata.fields[2]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Ready"},
				},
				{
					Field:                 "metadata.fields[3]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Restarts"},
				},
			},
			expectedFields: []interface{}{
				"test-pod",
				"Running",
				"1/1",
				"0|" + fmt.Sprint(fixedTime.UnixMilli()),
			},
			expectError: false,
		},
		{
			name: "transform restart field with 2h30m ago",
			inputFields: []interface{}{
				"test-pod",
				"Running",
				"1/1",
				"10 (2h30m ago)",
			},
			columns: []rescommon.ColumnDefinition{
				{
					Field:                 "metadata.fields[0]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Name"},
				},
				{
					Field:                 "metadata.fields[1]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Status"},
				},
				{
					Field:                 "metadata.fields[2]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Ready"},
				},
				{
					Field:                 "metadata.fields[3]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Restarts"},
				},
			},
			expectedFields: []interface{}{
				"test-pod",
				"Running",
				"1/1",
				"10|" + fmt.Sprint(fixedTime.Add(-2*time.Hour-30*time.Minute).UnixMilli()),
			},
			expectError: false,
		},
		{
			name: "no restart field in columns",
			inputFields: []interface{}{
				"test-pod",
				"Running",
			},
			columns: []rescommon.ColumnDefinition{
				{
					Field:                 "metadata.fields[0]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Name"},
				},
				{
					Field:                 "metadata.fields[1]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Status"},
				},
			},
			expectedFields: []interface{}{
				"test-pod",
				"Running",
			},
			expectError: false,
		},
		{
			name: "restart field is nil",
			inputFields: []interface{}{
				"test-pod",
				"Running",
				"1/1",
				nil,
			},
			columns: []rescommon.ColumnDefinition{
				{
					Field:                 "metadata.fields[0]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Name"},
				},
				{
					Field:                 "metadata.fields[1]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Status"},
				},
				{
					Field:                 "metadata.fields[2]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Ready"},
				},
				{
					Field:                 "metadata.fields[3]",
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "Restarts"},
				},
			},
			expectedFields: []interface{}{
				"test-pod",
				"Running",
				"1/1",
				nil,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"fields": tt.inputFields,
					},
				},
			}

			converter := &Converter{
				Columns: tt.columns,
			}

			result, err := converter.Transform(obj)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				fields, got, err := unstructured.NestedSlice(result.Object, "metadata", "fields")
				assert.NoError(t, err)
				assert.True(t, got)
				assert.Equal(t, tt.expectedFields, fields)
			}
		})
	}
}
