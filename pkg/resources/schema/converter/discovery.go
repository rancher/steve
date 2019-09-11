package converter

import (
	"strings"

	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/wrangler/pkg/merr"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

var (
	preferredGroups = map[string]string{
		"extensions": "apps",
	}
)

func AddDiscovery(client discovery.DiscoveryInterface, schemas map[string]*types.Schema) error {
	logrus.Info("Refreshing all schemas")

	groups, resourceLists, err := client.ServerGroupsAndResources()
	if err != nil {
		return err
	}

	versions := indexVersions(groups)

	var errs []error
	for _, resourceList := range resourceLists {
		gv, err := schema.ParseGroupVersion(resourceList.GroupVersion)
		if err != nil {
			errs = append(errs, err)
		}

		if err := refresh(gv, versions, resourceList, schemas); err != nil {
			errs = append(errs, err)
		}
	}

	return merr.NewErrors(errs...)
}

func indexVersions(groups []*metav1.APIGroup) map[string]string {
	result := map[string]string{}
	for _, group := range groups {
		result[group.Name] = group.PreferredVersion.Version
	}
	return result
}

func refresh(gv schema.GroupVersion, groupToPreferredVersion map[string]string, resources *metav1.APIResourceList, schemas map[string]*types.Schema) error {
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

		schema := schemas[GVKToSchemaID(gvk)]
		if schema == nil {
			schema = &types.Schema{
				ID:      GVKToSchemaID(gvk),
				Type:    "schema",
				Dynamic: true,
			}
			attributes.SetGVK(schema, gvk)
		}

		schema.PluralName = GVRToPluralName(gvr)
		attributes.SetAPIResource(schema, resource)
		if preferredVersion := groupToPreferredVersion[gv.Group]; preferredVersion != "" && preferredVersion != gv.Version {
			attributes.SetPreferredVersion(schema, preferredVersion)
		}
		if group := preferredGroups[gv.Group]; group != "" {
			attributes.SetPreferredGroup(schema, group)
		}

		schemas[schema.ID] = schema
	}

	return nil
}
