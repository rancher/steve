package restarts

import (
	"encoding/json"
	"testing"
	"time"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestParseRestarts(t *testing.T) {
	// Mock Now for consistent testing
	Now = func() time.Time {
		return time.Unix(1737462000, 0) // Fixed time
	}

	tests := []struct {
		name      string
		input     string
		wantCount int
		wantTime  bool // whether timestamp should be present
		wantErr   bool
	}{
		{
			name:      "no restarts",
			input:     "0",
			wantCount: 0,
			wantTime:  false,
		},
		{
			name:      "single restart with time",
			input:     "1 (3h37m ago)",
			wantCount: 1,
			wantTime:  true,
		},
		{
			name:      "multiple restarts with time",
			input:     "4 (3h38m ago)",
			wantCount: 4,
			wantTime:  true,
		},
		{
			name:      "large restart count",
			input:     "42 (5d ago)",
			wantCount: 42,
			wantTime:  true,
		},
		{
			name:    "invalid format",
			input:   "invalid",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseRestarts(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			var array []interface{}
			err = json.Unmarshal(result, &array)
			require.NoError(t, err)
			require.Len(t, array, 2)

			// Check count
			count, ok := array[0].(float64)
			require.True(t, ok)
			assert.Equal(t, tt.wantCount, int(count))

			// Check timestamp
			if tt.wantTime {
				assert.NotNil(t, array[1])
			} else {
				assert.Nil(t, array[1])
			}
		})
	}
}

func TestConverter_Transform(t *testing.T) {
	// Mock Now for consistent testing
	Now = func() time.Time {
		return time.Unix(1737462000, 0)
	}

	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	columns := []rescommon.ColumnDefinition{
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Name"}, Field: "metadata.fields[0]"},
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Ready"}, Field: "metadata.fields[1]"},
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Status"}, Field: "metadata.fields[2]"},
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Restarts"}, Field: "metadata.fields[3]"},
		{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Age"}, Field: "metadata.fields[4]"},
	}

	converter := &Converter{
		GVK:     gvk,
		Columns: columns,
	}

	tests := []struct {
		name      string
		input     *unstructured.Unstructured
		wantField interface{}
		wantErr   bool
	}{
		{
			name: "transform no restarts",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"fields": []interface{}{"test-pod", "1/1", "Running", "0", "5m"},
					},
				},
			},
			wantField: json.RawMessage(`[0,null]`),
		},
		{
			name: "transform with restarts",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"fields": []interface{}{"test-pod", "1/1", "Running", "4 (3h38m ago)", "5m"},
					},
				},
			},
			wantField: json.RawMessage(`[4,1737448920000]`),
		},
		{
			name: "no metadata fields",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{},
				},
			},
			wantErr: false, // No error, just no transformation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset Now for each test
			Now = func() time.Time {
				return time.Unix(1737462000, 0)
			}
			
			result, err := converter.Transform(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.wantField != nil {
				fields, found, err := unstructured.NestedSlice(result.Object, "metadata", "fields")
				require.NoError(t, err)
				require.True(t, found)
				require.GreaterOrEqual(t, len(fields), 4)

				// Compare the array value
				actualArray, ok := fields[3].([]interface{})
				require.True(t, ok, "field should be []interface{}")
				
				// Unmarshal expected JSON to compare
				var expectedArray []interface{}
				err = json.Unmarshal(tt.wantField.(json.RawMessage), &expectedArray)
				require.NoError(t, err)
				
				assert.Equal(t, expectedArray, actualArray)
			}
		})
	}
}

func TestConverter_isRestartsColumn(t *testing.T) {
	tests := []struct {
		name string
		gvk  schema.GroupVersionKind
		col  rescommon.ColumnDefinition
		want bool
	}{
		{
			name: "pod restarts column",
			gvk:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"},
			col:  rescommon.ColumnDefinition{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Restarts"}},
			want: true,
		},
		{
			name: "not pod kind",
			gvk:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"},
			col:  rescommon.ColumnDefinition{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Restarts"}},
			want: false,
		},
		{
			name: "not restarts column",
			gvk:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"},
			col:  rescommon.ColumnDefinition{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Status"}},
			want: false,
		},
		{
			name: "wrong group",
			gvk:  schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Pod"},
			col:  rescommon.ColumnDefinition{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Restarts"}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter := &Converter{GVK: tt.gvk}
			got := converter.isRestartsColumn(tt.col)
			assert.Equal(t, tt.want, got)
		})
	}
}
