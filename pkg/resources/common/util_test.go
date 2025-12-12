package common

import (
	"testing"

	"github.com/rancher/steve/pkg/schema/table"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTableColsToCommonCols(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "table columns are converted to common columns",
		test: func(t *testing.T) {
			originalColumns := []table.Column{
				{
					Name:        "weight",
					Field:       "$.metadata.fields[0]",
					Type:        "integer",
					Description: "how much the pod weighs",
				},
				{
					Name:        "position",
					Field:       "$.metadata.fields[1]",
					Type:        "string",
					Description: "number of this pod",
				},
				{
					Name:        "favoriteColour",
					Field:       "$.metadata.fields[2]",
					Type:        "string",
					Description: "green of course",
				},
			}
			expectedColumns := []ColumnDefinition{
				{
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "weight", Type: "integer", Description: "how much the pod weighs"},
					Field:                 "$.metadata.fields[0]",
				},
				{
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "position", Type: "string", Description: "number of this pod"},
					Field:                 "$.metadata.fields[1]",
				},
				{
					TableColumnDefinition: metav1.TableColumnDefinition{Name: "favoriteColour", Type: "string", Description: "green of course"},
					Field:                 "$.metadata.fields[2]",
				},
			}
			got := TableColsToCommonCols(originalColumns)
			assert.Equal(t, expectedColumns, got)
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}
