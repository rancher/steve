package core

import (
	"fmt"
	"strings"

	"github.com/rancher/naok/pkg/resources/common"
	"github.com/rancher/naok/pkg/resources/schema"
	"github.com/rancher/naok/pkg/table"
	"github.com/rancher/norman/pkg/data"
	"github.com/rancher/norman/pkg/types/convert"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
)

func Register(collection *schema.Collection) {
	collection.AddTemplate(&schema.Template{
		Kind: "ConfigMap",
		ComputedColumns: func(obj data.Object) {
			var fields []string
			for field := range obj.Map("data") {
				fields = append(fields, field)
			}

			obj.SetNested(len(obj.Map("data")), "metadata", "computed", "data")
			obj.SetNested(fields, "metadata", "computed", "fields")
		},
		Columns: []table.Column{
			common.NameColumn,
			{
				Name:  "Data",
				Field: "metadata.computed.data",
				Type:  "int",
			},
			{
				Name:  "Fields",
				Field: "metadata.computed.fields",
				Type:  "array[string]",
			},
			common.CreatedColumn,
		},
	})

	collection.AddTemplate(&schema.Template{
		Group: "apps",
		Kind:  "ControllerRevision",
		ComputedColumns: func(obj data.Object) {
			for _, owner := range obj.Map("metadata").Slice("ownerReferences") {
				if owner.Bool("controller") {
					obj.SetNested(getReference(collection, obj, owner), "metadata", "computed", "controller")
				}
			}
		},
		Columns: []table.Column{
			common.NameColumn,
			{
				Name:  "Controller",
				Field: "metadata.computed.controller",
				Type:  "reference",
			},
			{
				Name:  "Revision",
				Field: "revision",
				Type:  "int",
			},
			common.CreatedColumn,
		},
	})

	collection.AddTemplate(&schema.Template{
		Group: "apps",
		Kind:  "DaemonSet",
		Columns: []table.Column{
			common.NameColumn,
			{
				Name:  "Desired",
				Field: "status.desiredNumberScheduled",
				Type:  "int",
			},
			{
				Name:  "Current",
				Field: "status.currentNumberScheduled",
				Type:  "int",
			},
			{
				Name:  "Ready",
				Field: "status.numberReady",
				Type:  "int",
			},
			{
				Name:  "Up-to-date",
				Field: "status.updatedNumberScheduled",
				Type:  "int",
			},
			{
				Name:  "Available",
				Field: "status.numberAvailable",
				Type:  "int",
			},
			//{
			//	Name:  "Node Selector",
			//	Field: "metadata.computed.nodeSelector",
			//	Type:  "selector",
			//},
			common.CreatedColumn,
		},
		//ComputedColumns: func(obj data.Object) {
		//	obj.SetNested(podSelector(obj.String("metadata", "namespace"), obj.Map("spec", "selector")), "metadata", "computed", "nodeSelector")
		//},
	})
}

func getReference(collection *schema.Collection, obj data.Object, owner data.Object) map[string]interface{} {
	apiVersion := owner.String("apiVersion")
	kind := owner.String("kind")
	name := owner.String("name")
	namespace := obj.String("metadata", "namespace")
	gvk := schema2.FromAPIVersionAndKind(apiVersion, kind)
	typeName := collection.ByGVK(gvk)
	id := fmt.Sprintf("%s/%s", namespace, name)
	if namespace == "" {
		id = name
	}

	return map[string]interface{}{
		"id":   id,
		"type": typeName,
	}
}

type selector struct {
	Type      string   `json:"type,omitempty"`
	Namespace string   `json:"namespace,omitempty"`
	Terms     []string `json:"terms,omitempty"`
}

func podSelector(namespace string, obj data.Object) (result selector) {
	result.Type = "core.v1.pod"
	result.Namespace = namespace

	for k, v := range obj.Map("matchLabels") {
		vStr := convert.ToString(v)
		if vStr == "" {
			result.Terms = append(result.Terms, k)
		} else {
			result.Terms = append(result.Terms, fmt.Sprintf("%s=%s", k, v))
		}
	}

	for _, term := range obj.Slice("matchExpressions") {
		key := term.String("key")
		values := term.StringSlice("values")
		switch term.String("operator") {
		case "In":
			if len(values) == 1 {
				result.Terms = append(result.Terms, fmt.Sprintf("%s=%s", key, values[0]))
			} else {
				result.Terms = append(result.Terms, fmt.Sprintf("%s in (%s)", key, strings.Join(values, ",")))
			}
		case "Not In":
			if len(values) == 1 {
				result.Terms = append(result.Terms, fmt.Sprintf("%s!=%s", key, values[0]))
			} else {
				result.Terms = append(result.Terms, fmt.Sprintf("%s notin (%s)", key, strings.Join(values, ",")))
			}
		case "Exists":
			result.Terms = append(result.Terms, key)
		case "NotExists":
			result.Terms = append(result.Terms, "!"+key)
		}
	}

	return
}
