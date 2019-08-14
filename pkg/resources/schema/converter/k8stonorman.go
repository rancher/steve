package converter

import (
	"fmt"

	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/apiextensions.k8s.io/v1beta1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

func gvkToSchemaID(gvk schema.GroupVersionKind) string {
	if gvk.Group == "" {
		return fmt.Sprintf("core.%s.%s", gvk.Version, gvk.Kind)
	}
	return fmt.Sprintf("%s.%s.%s", gvk.Group, gvk.Version, gvk.Kind)
}

func GVRToSchemaID(gvr schema.GroupVersionResource) string {
	if gvr.Group == "" {
		return fmt.Sprintf("core.%s.%s", gvr.Version, gvr.Resource)
	}
	return fmt.Sprintf("%s.%s.%s", gvr.Group, gvr.Version, gvr.Resource)
}

func ToSchemas(crd v1beta1.CustomResourceDefinitionClient, client discovery.DiscoveryInterface) (map[string]*types.Schema, error) {
	result := map[string]*types.Schema{}

	if err := AddOpenAPI(client, result); err != nil {
		return nil, err
	}

	if err := AddDiscovery(client, result); err != nil {
		return nil, err
	}

	if err := AddCustomResources(crd, result); err != nil {
		return nil, err
	}

	return result, nil
}
