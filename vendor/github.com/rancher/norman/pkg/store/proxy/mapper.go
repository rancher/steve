package proxy

import "github.com/rancher/norman/pkg/types"

type AddAPIVersionKind struct {
	APIVersion string
	Kind       string
	Next       types.Mapper
}

func (d AddAPIVersionKind) FromInternal(data map[string]interface{}) {
	if d.Next != nil {
		d.Next.FromInternal(data)
	}
}

func (d AddAPIVersionKind) ToInternal(data map[string]interface{}) error {
	if d.Next != nil {
		if err := d.Next.ToInternal(data); err != nil {
			return err
		}
	}

	if data == nil {
		return nil
	}
	data["apiVersion"] = d.APIVersion
	data["kind"] = d.Kind
	return nil
}

func (d AddAPIVersionKind) ModifySchema(schema *types.Schema, schemas *types.Schemas) error {
	return nil
}
