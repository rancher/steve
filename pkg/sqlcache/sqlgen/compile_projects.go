package sqlgen

import (
	"fmt"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

const (
	namespacesDbName    = "_v1_Namespace"
	projectIDFieldLabel = "field.cattle.io/projectId"
)

// CompileProjectsOrNamespaces compiles the project/namespace filter into an Expr.
// It also adds the necessary JOINs to the JoinContext.
func CompileProjectsOrNamespaces(orFilter sqltypes.OrFilter, registry FieldRegistry, dbName string, jc *JoinContext) (sqlexpr.Expr, []sqlexpr.Join, error) {
	if len(orFilter.Filters) == 0 {
		return nil, nil, nil
	}

	// Add the namespace fields JOIN and the label JOIN for project ID
	extraJoins := []sqlexpr.Join{
		{
			Kind:  sqlexpr.LeftOuterJoin,
			Table: sqlexpr.TableRef{Name: namespacesDbName + "_fields", Alias: "nsf"},
			On: sqlexpr.Compare{
				Left:  sqlexpr.Col{Table: jc.prefix, Name: "metadata.namespace"},
				Op:    "=",
				Right: sqlexpr.Col{Table: "nsf", Name: "metadata.name"},
			},
		},
	}

	// Get or create the projectID label alias
	ltAlias := jc.EnsureLabelJoin(projectIDFieldLabel)

	// Override: the project ID label JOIN needs to be on nsf.key, not jc.prefix.key
	// Remove the auto-generated one and add the correct one
	// Actually, let's just add a specific JOIN for the namespace labels
	nsProjJoin := sqlexpr.Join{
		Kind:  sqlexpr.LeftOuterJoin,
		Table: sqlexpr.TableRef{Name: namespacesDbName + "_labels", Alias: ltAlias},
		On: sqlexpr.Compare{
			Left:  sqlexpr.Col{Table: "nsf", Name: "key"},
			Op:    "=",
			Right: sqlexpr.Col{Table: ltAlias, Name: "key"},
		},
	}
	extraJoins = append(extraJoins, nsProjJoin)

	// Compile the individual filters
	var exprs []sqlexpr.Expr
	for _, filter := range orFilter.Filters {
		var expr sqlexpr.Expr
		var err error

		if isLabelFilter(&filter) {
			expr, err = compileProjectsOrNamespacesLabelFilter(filter, ltAlias, dbName)
		} else {
			expr, err = compileProjectsOrNamespacesFieldFilter(filter, registry)
		}
		if err != nil {
			return nil, nil, err
		}
		exprs = append(exprs, expr)
	}

	if len(exprs) == 0 {
		return nil, nil, nil
	}

	// For In operations: OR the clauses; for NotIn: AND them
	var result sqlexpr.Expr
	if orFilter.Filters[0].Op == sqltypes.In {
		if len(exprs) == 1 {
			result = exprs[0]
		} else {
			result = sqlexpr.Or(exprs)
		}
	} else if orFilter.Filters[0].Op == sqltypes.NotIn {
		if len(exprs) == 1 {
			result = exprs[0]
		} else {
			result = sqlexpr.And(exprs)
		}
	} else {
		return nil, nil, fmt.Errorf("project or namespaces supports only 'IN' or 'NOT IN' operation. op: %s is not valid", orFilter.Filters[0].Op)
	}

	return result, extraJoins, nil
}

func compileProjectsOrNamespacesFieldFilter(filter sqltypes.Filter, registry FieldRegistry) (sqlexpr.Expr, error) {
	col, err := registry.Resolve("nsf", filter.Field, false)
	if err != nil {
		return nil, err
	}

	switch filter.Op {
	case sqltypes.In, sqltypes.NotIn:
		params := make([]sqlexpr.Expr, len(filter.Matches))
		for i, m := range filter.Matches {
			params[i] = sqlexpr.Param{Value: m}
		}
		return sqlexpr.In{Expr: col, Values: params, Negate: filter.Op == sqltypes.NotIn}, nil
	}

	return nil, fmt.Errorf("unrecognized operator: %s", filter.Op)
}

func compileProjectsOrNamespacesLabelFilter(filter sqltypes.Filter, ltAlias string, dbName string) (sqlexpr.Expr, error) {
	labelName := filter.Field[2]

	params := make([]sqlexpr.Expr, len(filter.Matches))
	for i, m := range filter.Matches {
		params[i] = sqlexpr.Param{Value: m}
	}

	switch filter.Op {
	case sqltypes.In:
		labelIs := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
		}
		valueIn := sqlexpr.In{Expr: sqlexpr.Col{Table: ltAlias, Name: "value"}, Values: params}
		return sqlexpr.And{labelIs, valueIn}, nil

	case sqltypes.NotIn:
		// (label=? AND value NOT IN (?...)) OR (key NOT IN subquery)
		labelIs := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
		}
		valueNotIn := sqlexpr.In{Expr: sqlexpr.Col{Table: ltAlias, Name: "value"}, Values: params, Negate: true}
		clause1 := sqlexpr.And{labelIs, valueNotIn}

		// Subquery for NOT IN
		var index int
		fmt.Sscanf(ltAlias, "lt%d", &index)
		subquery := fmt.Sprintf(`o.key NOT IN (SELECT f1.key FROM "%s_fields" f1`+"\n"+
			`		LEFT OUTER JOIN "_v1_Namespace_fields" nsf1 ON f1."metadata.namespace" = nsf1."metadata.name"`+"\n"+
			`		LEFT OUTER JOIN "_v1_Namespace_labels" lt%di1 ON nsf1.key = lt%di1.key`+"\n"+
			`		WHERE lt%di1.label = ?)`, dbName, index, index, index)
		clause2 := sqlexpr.Raw{SQL: subquery, Params: []any{labelName}}

		return sqlexpr.Or{clause1, clause2}, nil
	}

	return nil, fmt.Errorf("unrecognized operator: %s", filter.Op)
}
