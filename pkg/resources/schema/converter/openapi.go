package converter

import (
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/norman/pkg/types/convert"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/kube-openapi/pkg/util/proto"
)

func modelToSchema(modelName string, k *proto.Kind) *types.Schema {
	s := types.Schema{
		ID:             modelName,
		Type:           "schema",
		ResourceFields: map[string]types.Field{},
		Attributes:     map[string]interface{}{},
		Description:    k.GetDescription(),
		Dynamic:        true,
	}

	for fieldName, schemaField := range k.Fields {
		s.ResourceFields[fieldName] = toField(schemaField)
	}

	for _, fieldName := range k.RequiredFields {
		if f, ok := s.ResourceFields[fieldName]; ok {
			f.Required = true
			s.ResourceFields[fieldName] = f
		}
	}

	if ms, ok := k.Extensions["x-kubernetes-group-version-kind"].([]interface{}); ok {
		for _, mv := range ms {
			if m, ok := mv.(map[interface{}]interface{}); ok {
				gvk := schema.GroupVersionKind{
					Group:   convert.ToString(m["group"]),
					Version: convert.ToString(m["version"]),
					Kind:    convert.ToString(m["kind"]),
				}

				s.ID = GVKToSchemaID(gvk)
				attributes.SetGVK(&s, gvk)
			}
		}
	}

	return &s
}

func AddOpenAPI(client discovery.DiscoveryInterface, schemas map[string]*types.Schema) error {
	openapi, err := client.OpenAPISchema()
	if err != nil {
		return err
	}

	models, err := proto.NewOpenAPIData(openapi)
	if err != nil {
		return err
	}

	for _, modelName := range models.ListModels() {
		model := models.LookupModel(modelName)
		if k, ok := model.(*proto.Kind); ok {
			schema := modelToSchema(modelName, k)
			schemas[schema.ID] = schema
		}
	}

	return nil
}

func toField(schema proto.Schema) types.Field {
	f := types.Field{
		Description: schema.GetDescription(),
		Nullable:    true,
		Create:      true,
		Update:      true,
	}
	switch v := schema.(type) {
	case *proto.Array:
		f.Type = "array[" + toField(v.SubType).Type + "]"
	case *proto.Primitive:
		if v.Type == "number" {
			f.Type = "int"
		} else {
			f.Type = v.Type
		}
	case *proto.Map:
		f.Type = "map[" + toField(v.SubType).Type + "]"
	case *proto.Kind:
		parts := v.Path.Get()
		f.Type = parts[len(parts)-1]
	case proto.Reference:
		f.Type = v.SubSchema().GetPath().String()
	case *proto.Arbitrary:
	default:
		logrus.Errorf("unknown type: %v", schema)
		f.Type = "json"
	}

	return f
}
