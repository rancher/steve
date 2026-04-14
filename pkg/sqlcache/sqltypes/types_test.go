package sqltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewExternalLabelDependency(t *testing.T) {
	cases := []struct {
		name string
		eld  ExternalLabelDependency

		expectedQuery string
		err           string
	}{
		{
			name: "missing label and field pairs",
			eld: ExternalLabelDependency{
				SourceGVK:              "_v1_Secret",
				TargetGVK:              "management.cattle.io_v3_Project",
				SourceLabelTargetField: map[string]string{},
				TargetFinalFieldName:   "spec.clusterName",
			},

			err: "ExternalLabelDependency must have at least 1 label and field pair",
		},
		{
			name: "one label and field pair",

			eld: ExternalLabelDependency{
				SourceGVK: "_v1_Namespace",
				TargetGVK: "management.cattle.io_v3_Project",
				SourceLabelTargetField: map[string]string{
					"field.cattle.io/projectId": "metadata.name",
				},
				TargetFinalFieldName: "spec.displayName",
			},

			expectedQuery: `SELECT DISTINCT f.key, ex2."spec.displayName" FROM "_v1_Namespace_fields" f
	LEFT OUTER JOIN "_v1_Namespace_labels" lt0 ON f.key = lt0.key
	JOIN "management.cattle.io_v3_Project_fields" ex2 ON (lt0.value = ex2."metadata.name")
	WHERE (lt0.label = "field.cattle.io/projectId") AND f."spec.displayName" != ex2."spec.displayName"`,
		},
		{
			name: "two label and field pairs",
			eld: ExternalLabelDependency{
				SourceGVK: "_v1_Secret",
				TargetGVK: "management.cattle.io_v3_Project",
				SourceLabelTargetField: map[string]string{
					"management.cattle.io/project-scoped-secret":         "metadata.name",
					"management.cattle.io/project-scoped-secret-cluster": "spec.clusterName",
				},
				TargetFinalFieldName: "spec.displayName",
			},

			expectedQuery: `SELECT DISTINCT f.key, ex2."spec.displayName" FROM "_v1_Secret_fields" f
	LEFT OUTER JOIN "_v1_Secret_labels" lt0 ON f.key = lt0.key
	LEFT OUTER JOIN "_v1_Secret_labels" lt1 ON f.key = lt1.key
	JOIN "management.cattle.io_v3_Project_fields" ex2 ON (lt0.value = ex2."metadata.name" AND lt1.value = ex2."spec.clusterName")
	WHERE (lt0.label = "management.cattle.io/project-scoped-secret" AND lt1.label = "management.cattle.io/project-scoped-secret-cluster") AND f."spec.displayName" != ex2."spec.displayName"`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			eld, err := NewExternalLabelDependency(c.eld)

			if c.err != "" {
				assert.EqualError(t, err, c.err)
			} else {
				assert.Equal(t, c.expectedQuery, eld.Query())
			}
		})
	}
}
