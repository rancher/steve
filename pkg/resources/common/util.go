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
	colDefs, ok := columns.([]ColumnDefinition)
	if !ok {
		tableDefs, ok := columns.([]table.Column)
		if !ok {
			return nil
		}
		colDefs = TableColsToCommonCols(tableDefs)
	}
	return colDefs
}

func TableColsToCommonCols(tableDefs []table.Column) []ColumnDefinition {
	colDefs := make([]ColumnDefinition, len(tableDefs))
	for i, td := range tableDefs {
		// This isn't used right now, but it is used in the PR that tries to identify
		// numeric fields, so leave it here.
		// Although the `table.Column` and `metav1.TableColumnDefinition` types
		// are structurally the same, Go doesn't allow a quick way to cast one to the other.
		tcd := metav1.TableColumnDefinition{
			Name:        td.Name,
			Type:        td.Type,
			Format:      td.Format,
			Description: td.Description,
		}
		colDefs[i] = ColumnDefinition{
			TableColumnDefinition: tcd,
			Field:                 td.Field,
		}
	}
	return colDefs
}
