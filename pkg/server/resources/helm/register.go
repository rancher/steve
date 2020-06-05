package helm

import (
	"net/http"

	"github.com/rancher/steve/pkg/schemaserver/types"
	"github.com/rancher/steve/pkg/server/store/partition"
)

func Register(schemas *types.APISchemas) {
	schemas.InternalSchemas.TypeName("helmrelease", Release{})
	schemas.MustImportAndCustomize(Release{}, func(schema *types.APISchema) {
		schema.CollectionMethods = []string{http.MethodGet}
		schema.ResourceMethods = []string{http.MethodGet}
		schema.Store = &partition.Store{
			Partitioner: &partitioner{},
		}
		schema.Formatter = FormatRelease
	})
}
