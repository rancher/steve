package table

import (
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/norman/pkg/data"
	"github.com/rancher/norman/pkg/types"
)

type Column struct {
	Name   string `json:"name,omitempty"`
	Field  string `json:"field,omitempty"`
	Type   string `json:"type,omitempty"`
	Format string `json:"format,omitempty"`
}

type Table struct {
	Columns  []Column
	Computed func(data.Object)
}

type ColumnMapper struct {
	definition Table
	types.EmptyMapper
}

func NewColumns(computed func(data.Object), columns ...Column) *ColumnMapper {
	return &ColumnMapper{
		definition: Table{
			Columns:  columns,
			Computed: computed,
		},
	}
}

func (t *ColumnMapper) FromInternal(d data.Object) {
	if t.definition.Computed != nil {
		t.definition.Computed(d)
	}
}

func (t *ColumnMapper) ModifySchema(schema *types.Schema, schemas *types.Schemas) error {
	attributes.SetColumns(schema, t.definition.Columns)
	return nil
}
