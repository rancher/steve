package sqlgen

import (
	"fmt"
	"maps"
	"slices"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

// CompileSort compiles sort directives into OrderBy expressions and any needed CTEs.
// Returns the OrderBy slice and CTE (WithClause) slice for unbound label sorts.
func CompileSort(sortList sqltypes.SortList, registry FieldRegistry, prefix string, namespaced bool, jc *JoinContext) ([]sqlexpr.OrderBy, []sqlexpr.WithClause, error) {
	if len(sortList.SortDirectives) == 0 {
		// Default sort
		if namespaced {
			return []sqlexpr.OrderBy{{Expr: sqlexpr.Raw{SQL: prefix + ".id"}, Desc: false}}, nil, nil
		}
		return []sqlexpr.OrderBy{{Expr: sqlexpr.Col{Table: prefix, Name: "metadata.name"}, Desc: false}}, nil, nil
	}

	var orderBy []sqlexpr.OrderBy
	var ctes []sqlexpr.WithClause

	for _, sortDirective := range sortList.SortDirectives {
		fields := sortDirective.Fields
		desc := sortDirective.Order == sqltypes.DESC

		if isLabelsFieldList(fields) {
			labelName := fields[2]
			alias, ok := jc.AliasFor(labelName)
			if !ok {
				// Unbound sort label: needs a CTE
				alias = jc.EnsureLabelJoin(labelName)
				// For unbound sort labels, we create a CTE and join on the view
				// Actually, unbound sort labels use WITH clauses + view joins
				// We need to generate the CTE for it
				ctes = append(ctes, sqlexpr.WithClause{
					Name:    alias,
					Columns: []string{"key", "value"},
					Body: sqlexpr.Raw{
						SQL:    fmt.Sprintf(`SELECT key, value FROM "%s_labels" WHERE label = ?`, jc.dbName),
						Params: []any{labelName},
					},
				})
			}

			fieldExpr := sqlexpr.Expr(sqlexpr.Col{Table: alias, Name: "value"})
			if sortDirective.SortAsIP {
				fieldExpr = sqlexpr.FuncCall{Name: "inet_aton", Args: []sqlexpr.Expr{fieldExpr}}
			}

			var nulls sqlexpr.NullsPosition
			if desc {
				nulls = sqlexpr.NullsFirst // NULLS FIRST for DESC
			} else {
				nulls = sqlexpr.NullsLast // NULLS LAST for ASC
			}
			orderBy = append(orderBy, sqlexpr.OrderBy{Expr: fieldExpr, Desc: desc, Nulls: nulls})
		} else {
			fieldExpr, err := registry.Resolve(prefix, fields, true)
			if err != nil {
				return nil, nil, err
			}
			if sortDirective.SortAsIP {
				fieldExpr = sqlexpr.FuncCall{Name: "inet_aton", Args: []sqlexpr.Expr{fieldExpr}}
			}
			orderBy = append(orderBy, sqlexpr.OrderBy{Expr: fieldExpr, Desc: desc})
		}
	}

	return orderBy, ctes, nil
}

// GetUnboundSortLabels identifies labels that appear in sort directives but not in filters.
// These require WITH clauses (CTEs) to be generated.
func GetUnboundSortLabels(lo *sqltypes.ListOptions) []string {
	numSortDirectives := len(lo.SortList.SortDirectives)
	if numSortDirectives == 0 {
		return nil
	}
	unboundSortLabels := make(map[string]bool)
	for _, sortDirective := range lo.SortList.SortDirectives {
		fields := sortDirective.Fields
		if isLabelsFieldList(fields) {
			unboundSortLabels[fields[2]] = true
		}
	}
	if lo.Filters != nil {
		for _, andFilter := range lo.Filters {
			for _, orFilter := range andFilter.Filters {
				if isLabelFilter(&orFilter) {
					switch orFilter.Op {
					case sqltypes.In, sqltypes.Eq, sqltypes.Gt, sqltypes.Lt, sqltypes.Exists:
						delete(unboundSortLabels, orFilter.Field[2])
					}
				}
			}
		}
	}
	return slices.Collect(maps.Keys(unboundSortLabels))
}
