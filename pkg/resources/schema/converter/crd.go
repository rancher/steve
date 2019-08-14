package converter

import (
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/naok/pkg/table"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/apiextensions.k8s.io/v1beta1"
	beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func AddCustomResources(crd v1beta1.CustomResourceDefinitionClient, schemas map[string]*types.Schema) error {
	crds, err := crd.List(metav1.ListOptions{})
	if err != nil {
		return nil
	}

	for _, crd := range crds.Items {
		if crd.Status.AcceptedNames.Plural == "" {
			continue
		}

		var columns []table.Column
		for _, col := range crd.Spec.AdditionalPrinterColumns {
			columns = append(columns, table.Column{
				Name:  col.Name,
				Field: col.JSONPath,
				Type:  col.Type,
			})
		}

		group, resource := crd.Spec.Group, crd.Status.AcceptedNames.Plural

		if crd.Spec.Version != "" {
			forVersion(group, crd.Spec.Version, resource, schemas, crd.Spec.AdditionalPrinterColumns, columns)
		}
		for _, version := range crd.Spec.Versions {
			forVersion(group, version.Name, resource, schemas, crd.Spec.AdditionalPrinterColumns, columns)
		}
	}

	return nil
}

func forVersion(group, version, resource string, schemas map[string]*types.Schema, columnDefs []beta1.CustomResourceColumnDefinition, columns []table.Column) {
	var versionColumns []table.Column
	for _, col := range columnDefs {
		versionColumns = append(versionColumns, table.Column{
			Name:   col.Name,
			Field:  col.JSONPath,
			Type:   col.Type,
			Format: col.Format,
		})
	}
	if len(versionColumns) == 0 {
		versionColumns = columns
	}

	id := GVRToSchemaID(schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: resource,
	})

	schema := schemas[id]
	if schema == nil {
		return
	}
	attributes.SetColumns(schema, columns)
}
