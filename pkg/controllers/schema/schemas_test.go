package schema

import (
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/resources/common"
	wschemas "github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func crdSchema(expected map[string]string, tableCols ...string) *types.APISchema {
	s := &types.APISchema{Schema: &wschemas.Schema{}}
	attributes.MarkCRD(s)
	attributes.SetCRDJSONPathParsers(s, expected)
	if tableCols != nil {
		var cols []common.ColumnDefinition
		for _, name := range tableCols {
			cols = append(cols, common.ColumnDefinition{TableColumnDefinition: metav1.TableColumnDefinition{Name: name}})
		}
		attributes.SetColumns(s, cols)
	}
	return s
}

func nonCRDSchema() *types.APISchema {
	s := &types.APISchema{Schema: &wschemas.Schema{}}
	attributes.SetColumns(s, []common.ColumnDefinition{{TableColumnDefinition: metav1.TableColumnDefinition{Name: "Name"}}})
	return s
}

func TestStaleColumns(t *testing.T) {
	tests := []struct {
		name    string
		schemas map[string]*types.APISchema
		want    bool
	}{
		{
			name: "table still has default columns after CRD gained a printer column",
			schemas: map[string]*types.APISchema{
				"a": crdSchema(map[string]string{"Name": "{.metadata.name}", "Foo": "{.foo}"}, "Name", "Age"),
			},
			want: true,
		},
		{
			name: "table has new columns after CRD dropped them",
			schemas: map[string]*types.APISchema{
				"a": crdSchema(map[string]string{"Name": "{.metadata.name}", "Age": "{.metadata.creationTimestamp}"}, "Name", "Foo"),
			},
			want: true,
		},
		{
			name: "columns match",
			schemas: map[string]*types.APISchema{
				"a": crdSchema(map[string]string{"Name": "{.metadata.name}", "Foo": "{.foo}"}, "Name", "Foo"),
			},
			want: false,
		},
		{
			name: "printer column named like the default Name column",
			schemas: map[string]*types.APISchema{
				"a": crdSchema(map[string]string{"Name": "{.spec.name}"}, "Name", "Name"),
			},
			want: false,
		},
		{
			name: "table columns unavailable",
			schemas: map[string]*types.APISchema{
				"a": crdSchema(map[string]string{"Name": "{.metadata.name}", "Foo": "{.foo}"}),
			},
			want: false,
		},
		{
			name: "non-CRD schema ignored",
			schemas: map[string]*types.APISchema{
				"a": nonCRDSchema(),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, staleColumns(tt.schemas))
		})
	}
}
