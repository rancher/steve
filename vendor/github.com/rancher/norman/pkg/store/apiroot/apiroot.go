package apiroot

import (
	"net/http"

	"github.com/rancher/norman/pkg/store/empty"
	"github.com/rancher/norman/pkg/types"
)

func Register(schemas *types.Schemas, versions, roots []string) {
	schemas.MustAddSchema(types.Schema{
		ID:                "apiRoot",
		CollectionMethods: []string{"GET"},
		ResourceMethods:   []string{"GET"},
		ResourceFields: map[string]types.Field{
			"apiVersion": {Type: "map[json]"},
			"path":       {Type: "string"},
		},
		Formatter: APIRootFormatter,
		Store:     NewAPIRootStore(versions, roots),
	})
}

func APIRootFormatter(apiOp *types.APIRequest, resource *types.RawResource) {
	path, _ := resource.Values["path"].(string)
	if path == "" {
		return
	}

	delete(resource.Values, "path")

	resource.Links["root"] = apiOp.URLBuilder.RelativeToRoot(path)
	resource.Links["schemas"] = apiOp.URLBuilder.RelativeToRoot(path)

	data, _ := resource.Values["apiVersion"].(map[string]interface{})
	apiVersion := apiVersionFromMap(apiOp.Schemas, data)

	resource.Links["self"] = apiOp.URLBuilder.RelativeToRoot(apiVersion)

	for _, schema := range apiOp.Schemas.Schemas() {
		addCollectionLink(apiOp, schema, resource.Links)
	}

	return
}

func addCollectionLink(apiOp *types.APIRequest, schema *types.Schema, links map[string]string) {
	collectionLink := getSchemaCollectionLink(apiOp, schema)
	if collectionLink != "" {
		links[schema.PluralName] = collectionLink
	}
}

func getSchemaCollectionLink(apiOp *types.APIRequest, schema *types.Schema) string {
	if schema != nil && contains(schema.CollectionMethods, http.MethodGet) {
		return apiOp.URLBuilder.Collection(schema)
	}
	return ""
}

type APIRootStore struct {
	empty.Store
	roots    []string
	versions []string
}

func NewAPIRootStore(versions []string, roots []string) types.Store {
	return &APIRootStore{
		roots:    roots,
		versions: versions,
	}
}

func (a *APIRootStore) ByID(apiOp *types.APIRequest, schema *types.Schema, id string) (types.APIObject, error) {
	for _, version := range a.versions {
		if version == id {
			return types.ToAPI(apiVersionToAPIRootMap(version)), nil
		}
	}
	return types.APIObject{}, nil
}

func (a *APIRootStore) List(apiOp *types.APIRequest, schema *types.Schema, opt *types.QueryOptions) (types.APIObject, error) {
	var roots []map[string]interface{}

	versions := a.versions

	for _, version := range versions {
		roots = append(roots, apiVersionToAPIRootMap(version))
	}

	for _, root := range a.roots {
		roots = append(roots, map[string]interface{}{
			"path": root,
		})
	}

	return types.ToAPI(roots), nil
}

func apiVersionToAPIRootMap(version string) map[string]interface{} {
	return map[string]interface{}{
		"id":   version,
		"type": "apiRoot",
		"apiVersion": map[string]interface{}{
			"version": version,
		},
		"path": "/" + version,
	}
}

func apiVersionFromMap(schemas *types.Schemas, apiVersion map[string]interface{}) string {
	version, _ := apiVersion["version"].(string)
	return version
}

func contains(list []string, needle string) bool {
	for _, v := range list {
		if v == needle {
			return true
		}
	}
	return false
}
