package converter

import (
	"github.com/rancher/apiserver/pkg/types"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/discovery"
	"k8s.io/kube-openapi/pkg/util/proto"
)

// AddDescription adds a description to all schemas in schemas. Will not add new schemas, only add mutate existing
// ones. Returns an error if the definitions could not be retrieved.
func AddDescription(client discovery.DiscoveryInterface, schemas map[string]*types.APISchema) error {
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
			gvk := GetGVKForKind(k)
			if gvk == nil {
				// kind was not for top level gvk, we can skip this resource
				logrus.Debugf("kind %s, was not for a top level resource", k.Path.String())
				continue
			}
			schemaID := GVKToVersionedSchemaID(*gvk)
			schema, ok := schemas[schemaID]
			// some kinds have a gvk but don't correspond to a schema (like a podList). We can
			// skip these resources as well
			if !ok {
				logrus.Debugf("Did not find schema for ID %s", schemaID)
				continue
			}
			schema.Description = k.GetDescription()
		}
	}

	return nil
}
