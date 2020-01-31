package common

import (
	"github.com/rancher/norman/v2/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/table"
)

var (
	NameColumn = table.Column{
		Name:   "Name",
		Field:  "metadata.name",
		Type:   "string",
		Format: "name",
	}
	CreatedColumn = table.Column{
		Name:   "Created",
		Field:  "metadata.creationTimestamp",
		Type:   "string",
		Format: "date",
	}
)

type DefaultColumns struct {
	types.EmptyMapper
}

func (d *DefaultColumns) ModifySchema(schema *types.Schema, schemas *types.Schemas) error {
	if attributes.Columns(schema) == nil {
		attributes.SetColumns(schema, []table.Column{
			NameColumn,
			CreatedColumn,
		})
	}

	return nil
}
