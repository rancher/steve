package multivalue

import (
	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ParseFunc transforms a string value into a multi-value array
type ParseFunc func(string) ([]interface{}, error)

// FieldConfig defines a multi-value field transformation
type FieldConfig struct {
	ColumnName string
	ParseFunc  ParseFunc
}

// Converter transforms multi-value fields into JSON arrays for storage
type Converter struct {
	Columns []rescommon.ColumnDefinition
	Fields  []FieldConfig
}

// Transform processes metadata.fields and converts registered multi-value fields
func (c *Converter) Transform(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	fields, got, err := unstructured.NestedSlice(obj.Object, "metadata", "fields")
	if err != nil || !got {
		return obj, err
	}

	updated := false

	// Process each registered field config
	for _, fieldConfig := range c.Fields {
		// Find the column index for this field
		index := findColumnIndex(c.Columns, fieldConfig.ColumnName)
		if index == -1 || index >= len(fields) {
			continue
		}

		if fields[index] == nil {
			continue
		}

		// Only process string values
		value, ok := fields[index].(string)
		if !ok {
			continue
		}

		// Parse the value using the field's parser
		arrayValue, err := fieldConfig.ParseFunc(value)
		if err != nil {
			logrus.Debugf("failed to parse %s value %q: %v", fieldConfig.ColumnName, value, err)
			continue
		}

		// Store as array - SQL layer handles COMPOSITE_INT fields directly
		fields[index] = arrayValue
		updated = true
	}

	if updated {
		if err := unstructured.SetNestedSlice(obj.Object, fields, "metadata", "fields"); err != nil {
			return obj, err
		}
	}

	return obj, nil
}

// findColumnIndex finds the index of a column by name
func findColumnIndex(columns []rescommon.ColumnDefinition, columnName string) int {
	for _, col := range columns {
		if col.Name == columnName {
			return rescommon.GetIndexValueFromString(col.Field)
		}
	}
	return -1
}
