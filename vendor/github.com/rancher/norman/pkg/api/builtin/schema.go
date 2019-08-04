package builtin

import (
	"github.com/rancher/norman/pkg/store/schema"
	"github.com/rancher/norman/pkg/types"
)

var (
	Schema = types.Schema{
		ID:                "schema",
		PluralName:        "schemas",
		CollectionMethods: []string{"GET"},
		ResourceMethods:   []string{"GET"},
		ResourceFields: map[string]types.Field{
			"collectionActions": {Type: "map[json]"},
			"collectionFields":  {Type: "map[json]"},
			"collectionFilters": {Type: "map[json]"},
			"collectionMethods": {Type: "array[string]"},
			"pluralName":        {Type: "string"},
			"resourceActions":   {Type: "map[json]"},
			"attributes":        {Type: "map[json]"},
			"resourceFields":    {Type: "map[json]"},
			"resourceMethods":   {Type: "array[string]"},
			"version":           {Type: "map[json]"},
		},
		Formatter: SchemaFormatter,
		Store:     schema.NewSchemaStore(),
	}

	Error = types.Schema{
		ID:                "error",
		ResourceMethods:   []string{},
		CollectionMethods: []string{},
		ResourceFields: map[string]types.Field{
			"code":      {Type: "string"},
			"detail":    {Type: "string", Nullable: true},
			"message":   {Type: "string", Nullable: true},
			"fieldName": {Type: "string", Nullable: true},
			"status":    {Type: "int"},
		},
	}

	Collection = types.Schema{
		ID:                "collection",
		ResourceMethods:   []string{},
		CollectionMethods: []string{},
		ResourceFields: map[string]types.Field{
			"data":       {Type: "array[json]"},
			"pagination": {Type: "map[json]"},
			"sort":       {Type: "map[json]"},
			"filters":    {Type: "map[json]"},
		},
	}

	APIRoot = types.Schema{
		ID:                "apiRoot",
		CollectionMethods: []string{"GET"},
		ResourceMethods:   []string{"GET"},
		ResourceFields: map[string]types.Field{
			"apiVersion": {Type: "map[json]"},
			"path":       {Type: "string"},
		},
		Formatter: APIRootFormatter,
		Store:     NewAPIRootStore(nil, nil),
	}

	Schemas = types.EmptySchemas().
		MustAddSchema(Schema).
		MustAddSchema(Error).
		MustAddSchema(Collection).
		MustAddSchema(APIRoot)
)

func apiVersionFromMap(schemas *types.Schemas, apiVersion map[string]interface{}) string {
	version, _ := apiVersion["version"].(string)
	return version
}

func SchemaFormatter(apiOp *types.APIRequest, resource *types.RawResource) {
	schema := apiOp.Schemas.Schema(resource.ID)
	if schema == nil {
		return
	}

	collectionLink := getSchemaCollectionLink(apiOp, schema)
	if collectionLink != "" {
		resource.Links["collection"] = collectionLink
	}
}

func contains(list []string, needle string) bool {
	for _, v := range list {
		if v == needle {
			return true
		}
	}
	return false
}
