package common

import (
	"strconv"
	"strings"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
)

// GetIndexValueFromString looks for values between [ ].
// e.g: $.metadata.fields[2], in this case it would return 2
// In case it doens't find any value between brackets it
// returns -1
func GetIndexValueFromString(pathString string) int {
	index := -1
	idxStart := strings.Index(pathString, "[")
	idxEnd := strings.Index(pathString, "]")
	if idxStart > 0 {
		idx, err := strconv.Atoi(pathString[idxStart+1 : idxEnd])
		if err != nil {
			return index
		}
		index = idx
	}
	return index
}

// GetColumnDefinitions returns ColumnDefinitions from an APISchema
func GetColumnDefinitions(schema *types.APISchema) []ColumnDefinition {
	columns := attributes.Columns(schema)
	if columns == nil {
		return nil
	}
	colDefs, ok := columns.([]ColumnDefinition)
	if !ok {
		return nil
	}
	return colDefs
}
