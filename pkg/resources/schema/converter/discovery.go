package converter

import (
	"strings"

	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/wrangler/pkg/merr"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

func AddDiscovery(client discovery.DiscoveryInterface, schemas map[string]*types.Schema) error {
	logrus.Info("Refreshing all schemas")

	_, resourceLists, err := client.ServerGroupsAndResources()
	if err != nil {
		return err
	}

	var errs []error
	for _, resourceList := range resourceLists {
		gv, err := schema.ParseGroupVersion(resourceList.GroupVersion)
		if err != nil {
			errs = append(errs, err)
		}

		if err := refresh(gv, resourceList, schemas); err != nil {
			errs = append(errs, err)
		}
	}

	return merr.NewErrors(errs...)
}

func refresh(gv schema.GroupVersion, resources *metav1.APIResourceList, schemas map[string]*types.Schema) error {
	for _, resource := range resources.APIResources {
		if strings.Contains(resource.Name, "/") {
			continue
		}

		gvk := schema.GroupVersionKind{
			Group:   gv.Group,
			Version: gv.Version,
			Kind:    resource.Kind,
		}

		gvr := gvk.GroupVersion().WithResource(resource.Name)

		logrus.Infof("APIVersion %s/%s Kind %s", gvk.Group, gvk.Version, gvk.Kind)

		schema := schemas[gvkToSchemaID(gvk)]
		if schema == nil {
			schema = &types.Schema{
				Type:    "schema",
				Dynamic: true,
			}
			attributes.SetGVK(schema, gvk)
		}

		schema.PluralName = resource.Name
		attributes.SetAPIResource(schema, resource)

		// switch ID to be GVR, not GVK
		if schema.ID != "" {
			delete(schemas, schema.ID)
		}

		schema.ID = GVRToSchemaID(gvr)
		schemas[schema.ID] = schema
	}

	return nil
}
