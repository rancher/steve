package dates

import (
	"fmt"
	"testing"
	"time"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestTransformIdempotency(t *testing.T) {
	mockTime := func() time.Time { return time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) }
	Now = mockTime

	tests := []struct {
		name      string
		converter Converter
		input     *unstructured.Unstructured
		wantField any
	}{
		{
			name: "built-in duration field is stable after two transforms",
			converter: Converter{
				GVK: schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
				Columns: []rescommon.ColumnDefinition{
					{
						TableColumnDefinition: v1.TableColumnDefinition{Name: "Age"},
						Field:                 "$.metadata.fields[0]",
					},
				},
			},
			input: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"metadata": map[string]any{
						"fields": []any{"1d"},
					},
				},
			},
			wantField: fmt.Sprintf("%d", mockTime().Add(-24*time.Hour).UnixMilli()),
		},
		{
			name: "CRD duration field is stable after two transforms",
			converter: Converter{
				GVK: schema.GroupVersionKind{Group: "test.cattle.io", Version: "v1", Kind: "TestResource"},
				Columns: []rescommon.ColumnDefinition{
					{
						TableColumnDefinition: v1.TableColumnDefinition{Name: "Age", Type: "date"},
						Field:                 "$.metadata.fields[0]",
					},
				},
				IsCRD: true,
			},
			input: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "test.cattle.io/v1",
					"kind":       "TestResource",
					"metadata": map[string]any{
						"fields": []any{"5m"},
					},
				},
			},
			wantField: fmt.Sprintf("%d", mockTime().Add(-5*time.Minute).UnixMilli()),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := tt.input

			first, err := tt.converter.Transform(obj)
			require.NoError(t, err)

			fields1, ok, err := unstructured.NestedSlice(first.Object, "metadata", "fields")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tt.wantField, fields1[0])

			second, err := tt.converter.Transform(first)
			require.NoError(t, err)

			fields2, ok, err := unstructured.NestedSlice(second.Object, "metadata", "fields")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, fields1[0], fields2[0], "second transform should produce the same result as the first")
		})
	}
}

func TestIsUnixMilli(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "valid unix milli timestamp",
			input: "1714567890123",
			want:  true,
		},
		{
			name:  "too short",
			input: "171456789012",
			want:  false,
		},
		{
			name:  "too long",
			input: "17145678901234",
			want:  false,
		},
		{
			name:  "contains non-digit character",
			input: "171456789012a",
			want:  false,
		},
		{
			name:  "empty string",
			input: "",
			want:  false,
		},
		{
			name:  "duration string",
			input: "5m",
			want:  false,
		},
		{
			name:  "RFC3339 timestamp",
			input: "2024-05-01T12:00:00Z",
			want:  false,
		},
		{
			name:  "negative number 13 chars",
			input: "-171456789012",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isUnixMilli(tt.input))
		})
	}
}
