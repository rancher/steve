package listprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestPaginateList(t *testing.T) {
	objects := []unstructured.Unstructured{
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "fuji",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "honeycrisp",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "granny-smith",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "red-delicious",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "crispin",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "bramley",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "golden-delicious",
				},
			},
		},
		{
			Object: map[string]interface{}{
				"kind": "apple",
				"metadata": map[string]interface{}{
					"name": "macintosh",
				},
			},
		},
	}
	tests := []struct {
		name       string
		objects    []unstructured.Unstructured
		pagination Pagination
		want       []unstructured.Unstructured
		wantPages  int
	}{
		{
			name:    "pagesize=3, page=unset",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
			},
			want:      objects[:3],
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page=1",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     1,
			},
			want:      objects[:3],
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page=2",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     2,
			},
			want:      objects[3:6],
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page=last",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     3,
			},
			want:      objects[6:],
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page>last",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     37,
			},
			want:      []unstructured.Unstructured{},
			wantPages: 3,
		},
		{
			name:    "pagesize=3, page<0",
			objects: objects,
			pagination: Pagination{
				pageSize: 3,
				page:     -4,
			},
			want:      objects[:3],
			wantPages: 3,
		},
		{
			name:       "pagesize=0",
			objects:    objects,
			pagination: Pagination{},
			want:       objects,
			wantPages:  0,
		},
		{
			name:    "pagesize=-1",
			objects: objects,
			pagination: Pagination{
				pageSize: -7,
			},
			want:      objects,
			wantPages: 0,
		},
		{
			name:    "even page size, even list size",
			objects: objects,
			pagination: Pagination{
				pageSize: 2,
				page:     2,
			},
			want:      objects[2:4],
			wantPages: 4,
		},
		{
			name:    "even page size, odd list size",
			objects: objects[1:],
			pagination: Pagination{
				pageSize: 2,
				page:     2,
			},
			want:      objects[3:5],
			wantPages: 4,
		},
		{
			name:    "odd page size, even list size",
			objects: objects,
			pagination: Pagination{
				pageSize: 5,
				page:     2,
			},
			want:      objects[5:],
			wantPages: 2,
		},
		{
			name:    "odd page size, odd list size",
			objects: objects[1:],
			pagination: Pagination{
				pageSize: 3,
				page:     2,
			},
			want:      objects[4:7],
			wantPages: 3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, gotPages := PaginateList(test.objects, test.pagination)
			assert.Equal(t, test.want, got)
			assert.Equal(t, test.wantPages, gotPages)
		})
	}
}
