package common

import (
	"strconv"
	"strings"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/schema/table"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetIndexValueFromString looks for values between [ ].
// e.g: $.metadata.fields[2], in this case it would return 2
// In case it doesn't find any value between brackets it returns -1
func GetIndexValueFromString(pathString string) int {
	idxStart := strings.Index(pathString, "[")
	if idxStart == -1 {
		return -1
	}
	idxEnd := strings.Index(pathString[idxStart+1:], "]")
	if idxEnd == -1 {
		return -1
	}
	idx, err := strconv.Atoi(pathString[idxStart+1 : idxStart+1+idxEnd])
	if err != nil {
		return -1
	}
	return idx
}

// GetColumnDefinitions returns ColumnDefinitions from an APISchema
func GetColumnDefinitions(schema *types.APISchema) []ColumnDefinition {
	columns := attributes.Columns(schema)
	if columns == nil {
		return nil
	}
	if colDefs, ok := columns.([]ColumnDefinition); ok {
		return colDefs
	}
	if cols, ok := columns.([]table.Column); ok {
		var colDefs []ColumnDefinition
		for _, col := range cols {
			colDefs = append(colDefs, ColumnDefinition{
				TableColumnDefinition: metav1.TableColumnDefinition{
					Name:        col.Name,
					Type:        col.Type,
					Format:      col.Format,
					Description: col.Description,
					Priority:    int32(col.Priority),
				},
				Field: col.Field,
			})
		}
		return colDefs
	}
	return nil
}
