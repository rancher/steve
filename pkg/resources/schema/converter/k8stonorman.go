package converter

import (
	"fmt"

	"github.com/rancher/norman/pkg/types"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

func gvkToSchemaID(gvk schema.GroupVersionKind) string {
	if gvk.Group == "" {
		return fmt.Sprintf("apis/core/%s/%s", gvk.Version, gvk.Kind)
	}
	return fmt.Sprintf("apis/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind)
}

func GVRToSchemaID(gvk schema.GroupVersionResource) string {
	if gvk.Group == "" {
		return fmt.Sprintf("apis/core/%s/%s", gvk.Version, gvk.Resource)
	}
	return fmt.Sprintf("apis/%s/%s/%s", gvk.Group, gvk.Version, gvk.Resource)
}

func ToSchemas(client discovery.DiscoveryInterface) (map[string]*types.Schema, error) {
	result := map[string]*types.Schema{}

	if err := AddOpenAPI(client, result); err != nil {
		return nil, err
	}

	if err := AddDiscovery(client, result); err != nil {
		return nil, err
	}

	return result, nil
}
