package common

import (
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/naok/pkg/table"
	"github.com/rancher/norman/pkg/types"
)

type DefaultColumns struct {
	types.EmptyMapper
}

func (d *DefaultColumns) ModifySchema(schema *types.Schema, schemas *types.Schemas) error {
	if attributes.Columns(schema) == nil {
		attributes.SetColumns(schema, []table.Column{
			{
				Name:   "Name",
				Field:  "metadata.name",
				Type:   "string",
				Format: "name",
			},
			{
				Name:   "Created",
				Field:  "metadata.creationTimestamp",
				Type:   "string",
				Format: "date",
			},
		})
	}

	return nil
}
