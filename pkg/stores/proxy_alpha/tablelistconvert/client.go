/*
Package tablelistconvert provides a client that will use a table client but convert *UnstructuredList and *Unstructured objects
returned by ByID and List to resemble those returned by non-table clients while preserving some table-related data.
*/
package tablelistconvert

import (
	"context"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/pkg/data"
	"k8s.io/apimachinery/pkg/watch"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

type Client struct {
	dynamic.ResourceInterface
}

type WarningBuffer []types.Warning

// List will return an *UnstructuredList that contains Items instead of just using the Object field to store a table as
// Table Clients do. The items will preserve values for columns in the form of metadata.fields.
func (c *Client) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	list, err := c.ResourceInterface.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	tableToList(list)
	return list, nil
}

func (c *Client) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	buffer := &WarningBuffer{}
	w, err := c.Watch(ctx, opts)
	if err != nil {
		return nil, err
	}
	result := make(chan watch.Event)
	go func() {
		for e := range w.ResultChan() {
			e.Object
		}
	}()
	return result, nil
}

func tableToList(obj *unstructured.UnstructuredList) {
	if obj.Object["kind"] != "Table" ||
		(obj.Object["apiVersion"] != "meta.k8s.io/v1" &&
			obj.Object["apiVersion"] != "meta.k8s.io/v1beta1") {
		return
	}

	obj.Items = tableToObjects(obj.Object)
}

func tableToObjects(obj map[string]interface{}) []unstructured.Unstructured {
	var result []unstructured.Unstructured

	rows, _ := obj["rows"].([]interface{})
	for _, row := range rows {
		m, ok := row.(map[string]interface{})
		if !ok {
			continue
		}
		cells := m["cells"]
		object, ok := m["object"].(map[string]interface{})
		if !ok {
			continue
		}

		data.PutValue(object, cells, "metadata", "fields")
		result = append(result, unstructured.Unstructured{
			Object: object,
		})
	}

	return result
}
