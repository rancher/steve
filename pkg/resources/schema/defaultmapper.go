package schema

import (
	"fmt"

	"github.com/rancher/norman/pkg/data"
	"github.com/rancher/norman/pkg/types"
)

func newDefaultMapper() types.Mapper {
	return &defaultMapper{}
}

type defaultMapper struct {
	types.EmptyMapper
}

func (d *defaultMapper) FromInternal(data data.Object) {
	if data["kind"] != "" && data["apiVersion"] != "" {
		if t, ok := data["type"]; ok && data != nil {
			data["_type"] = t
		}
	}

	if _, ok := data["id"]; ok || data == nil {
		return
	}

	name := types.Name(data)
	namespace := types.Namespace(data)

	if namespace == "" {
		data["id"] = name
	} else {
		data["id"] = fmt.Sprintf("%s/%s", namespace, name)
	}
}
