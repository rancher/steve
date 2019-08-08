package server

import (
	"fmt"

	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/norman/pkg/types/convert"
	"github.com/rancher/norman/pkg/types/values"
)

func newDefaultMapper() types.Mapper {
	return &defaultMapper{}
}

type defaultMapper struct {
	types.EmptyMapper
}

func (d *defaultMapper) FromInternal(data map[string]interface{}) {
	if t, ok := data["type"]; ok {
		data["_type"] = t
	}

	if _, ok := data["id"]; ok || data == nil {
		return
	}

	name := convert.ToString(values.GetValueN(data, "metadata", "name"))
	namespace := convert.ToString(values.GetValueN(data, "metadata", "namespace"))

	if namespace == "" {
		data["id"] = name
	} else {
		data["id"] = fmt.Sprintf("%s/%s", namespace, name)
	}
}
