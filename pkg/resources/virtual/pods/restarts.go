// Package pods provides cache.TransformFunc's for /v1 Pod objects
package pods

import (
	"fmt"
	"time"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var Now = time.Now

// Converter handles the transformation of Pod restart fields
type Converter struct {
	Columns []rescommon.ColumnDefinition
}

// Transform converts restart fields from "1 (38m ago)" to "1|timestamp_ms" format
func (c *Converter) Transform(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	fields, got, err := unstructured.NestedSlice(obj.Object, "metadata", "fields")
	if err != nil || !got {
		return obj, err
	}

	updated := false

	for _, col := range c.Columns {
		// Check if this is a restart field
		if col.Name != "Restarts" {
			continue
		}

		index := rescommon.GetIndexValueFromString(col.Field)
		if index == -1 || index >= len(fields) || fields[index] == nil {
			continue
		}

		value, ok := fields[index].(string)
		if !ok {
			logrus.Errorf("restart field is not string: %v", fields[index])
			continue
		}

		// Parse "1 (38m ago)" format
		count, duration, err := rescommon.ParseRestartField(value)
		if err != nil {
			logrus.Errorf("parse restart field %s: %s", value, err)
			continue
		}

		// Store as "count|timestamp_ms"
		timestamp := Now().Add(-duration).UnixMilli()
		fields[index] = fmt.Sprintf("%s|%d", count, timestamp)
		updated = true
	}

	if updated {
		if err := unstructured.SetNestedSlice(obj.Object, fields, "metadata", "fields"); err != nil {
			return obj, err
		}
	}

	return obj, nil
}
