package sqlgen

import (
	"fmt"
	"maps"
	"slices"

	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

const (
	namespacesDbName    = "_v1_Namespace"
	projectIDFieldLabel = "field.cattle.io/projectId"
)

// WithUnboundSortLabels creates CTEs and view joins for labels that appear in
// sort directives but not in filters.
func WithUnboundSortLabels(lo *sqltypes.ListOptions) Transform {
	return func(q QueryState) (QueryState, error) {
		unboundLabels := GetUnboundSortLabels(lo)
		for _, label := range unboundLabels {
			var alias string
			q, alias = q.NextAlias()
			q = q.RegisterAlias(label, alias)

			q = q.AddCTE(sqlexpr.WithClause{
				Name:    alias,
				Columns: []string{"key", "value"},
				Body: sqlexpr.Raw{
					SQL:    fmt.Sprintf("SELECT key, value FROM \"%s_labels\"\n  WHERE label = ?", q.DBName()),
					Params: []any{label},
				},
			})

			// View join (references CTE, no physical table)
			q = q.AddJoin(sqlexpr.Join{
				Kind:  sqlexpr.LeftOuterJoin,
				Table: sqlexpr.TableRef{Alias: alias},
				On:    sqlexpr.Raw{SQL: fmt.Sprintf("%s.key = %s.key", q.Prefix(), alias)},
			})
			q.usesLabels = true
		}
		return q, nil
	}
}

// WithFilters compiles all filter groups into WHERE clauses.
// Uses the FieldOps and LabelOps registries for dispatch.
func WithFilters(lo *sqltypes.ListOptions, registry FieldRegistry, isSummaryFilter bool) Transform {
	return func(q QueryState) (QueryState, error) {
		// Pre-register label joins (order must match legacy code)
		for _, orFilter := range lo.Filters {
			for _, filter := range orFilter.Filters {
				if isLabelFilter(&filter) {
					q, _ = q.EnsureLabelJoin(filter.Field[2])
				}
			}
		}

		// Compile each OR group
		for _, orFilter := range lo.Filters {
			expr, err := compileOrFilter(orFilter, registry, q, isSummaryFilter)
			if err != nil {
				return q, err
			}
			if expr != nil {
				q = q.AddWhere(expr)
			}
		}
		return q, nil
	}
}

func compileOrFilter(orFilter sqltypes.OrFilter, registry FieldRegistry, q QueryState, isSummaryFilter bool) (sqlexpr.Expr, error) {
	if len(orFilter.Filters) == 0 {
		return nil, nil
	}

	exprs := make([]sqlexpr.Expr, 0, len(orFilter.Filters))
	for _, filter := range orFilter.Filters {
		var expr sqlexpr.Expr
		var err error

		if isLabelFilter(&filter) {
			alias, _ := q.AliasFor(filter.Field[2])
			ctx := LabelContext{
				Alias:           alias,
				MainFieldPrefix: q.Prefix(),
				IsSummaryFilter: isSummaryFilter,
				DBName:          q.DBName(),
			}
			handler, ok := LabelOps[filter.Op]
			if !ok {
				return nil, fmt.Errorf("unrecognized operator: %s", filter.Op)
			}
			expr, err = handler(filter, ctx)
		} else {
			col, resolveErr := registry.Resolve(q.Prefix(), filter.Field, false)
			if resolveErr != nil {
				return nil, resolveErr
			}
			handler, ok := FieldOps[filter.Op]
			if !ok {
				return nil, fmt.Errorf("unrecognized operator: %s", filter.Op)
			}
			expr, err = handler(col, filter)
		}
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	if len(exprs) == 1 {
		return exprs[0], nil
	}
	return sqlexpr.Or(exprs), nil
}

// WithProjects compiles the project/namespace filter into WHERE + extra JOINs.
func WithProjects(orFilter sqltypes.OrFilter, registry FieldRegistry) Transform {
	return func(q QueryState) (QueryState, error) {
		if len(orFilter.Filters) == 0 {
			return q, nil
		}

		// Add nsf JOIN
		extraJoins := []sqlexpr.Join{
			{
				Kind:  sqlexpr.LeftOuterJoin,
				Table: sqlexpr.TableRef{Name: namespacesDbName + "_fields", Alias: "nsf"},
				On: sqlexpr.Raw{
					SQL: fmt.Sprintf(`%s."metadata.namespace" = nsf."metadata.name"`, q.Prefix()),
				},
			},
		}

		// Get or create projectID label alias
		ltAlias, exists := q.AliasFor(projectIDFieldLabel)
		if !exists {
			q, ltAlias = q.NextAlias()
			q = q.RegisterAlias(projectIDFieldLabel, ltAlias)
		}

		// Project label JOIN (uses nsf.key)
		extraJoins = append(extraJoins, sqlexpr.Join{
			Kind:  sqlexpr.LeftOuterJoin,
			Table: sqlexpr.TableRef{Name: namespacesDbName + "_labels", Alias: ltAlias},
			On:    sqlexpr.Raw{SQL: fmt.Sprintf("nsf.key = %s.key", ltAlias)},
		})

		q = q.AddJoins(extraJoins)

		// Compile individual project filters using registries
		var exprs []sqlexpr.Expr
		for _, filter := range orFilter.Filters {
			var expr sqlexpr.Expr
			var err error

			if isLabelFilter(&filter) {
				handler, ok := ProjectLabelOps[filter.Op]
				if !ok {
					return q, fmt.Errorf("unrecognized project label op: %s", filter.Op)
				}
				expr, err = handler(filter, ltAlias, q.DBName())
			} else {
				col, resolveErr := registry.Resolve("nsf", filter.Field, false)
				if resolveErr != nil {
					return q, resolveErr
				}
				handler, ok := ProjectFieldOps[filter.Op]
				if !ok {
					return q, fmt.Errorf("project or namespaces supports only 'IN' or 'NOT IN' operation. op: %s is not valid", filter.Op)
				}
				expr, err = handler(col, filter)
			}
			if err != nil {
				return q, err
			}
			exprs = append(exprs, expr)
		}

		if len(exprs) == 0 {
			return q, nil
		}

		// Combine: OR for In, AND for NotIn
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
				result = sqlexpr.InlineAnd(exprs)
			}
		} else {
			return q, fmt.Errorf("project or namespaces supports only 'IN' or 'NOT IN' operation. op: %s is not valid", orFilter.Filters[0].Op)
		}

		q = q.AddWhere(result)
		return q, nil
	}
}

// WithNamespace adds a namespace equality filter if needed.
func WithNamespace(namespace string) Transform {
	return func(q QueryState) (QueryState, error) {
		if namespace == "" || namespace == "*" {
			return q, nil
		}
		nsExpr := sqlexpr.Compare{
			Left:  sqlexpr.Col{Table: q.Prefix(), Name: "metadata.namespace"},
			Op:    "=",
			Right: sqlexpr.Param{Value: namespace},
		}
		return q.AddWhere(nsExpr), nil
	}
}

// WithPartitions adds partition RBAC constraints.
func WithPartitions(namespace string, partitions []partition.Partition) Transform {
	return func(q QueryState) (QueryState, error) {
		partExpr := CompilePartitions(namespace, partitions, q.Prefix())
		if partExpr != nil {
			q = q.AddWhere(partExpr)
		}
		return q, nil
	}
}

// WithSort compiles sort directives into ORDER BY clauses.
func WithSort(sortList sqltypes.SortList, registry FieldRegistry, namespaced bool) Transform {
	return func(q QueryState) (QueryState, error) {
		if len(sortList.SortDirectives) == 0 {
			// Default sort
			if namespaced {
				q = q.AddOrderBy(sqlexpr.OrderBy{Expr: sqlexpr.Raw{SQL: q.Prefix() + ".id"}, Desc: false})
			} else {
				q = q.AddOrderBy(sqlexpr.OrderBy{Expr: sqlexpr.Col{Table: q.Prefix(), Name: "metadata.name"}, Desc: false})
			}
			return q, nil
		}

		for _, sortDirective := range sortList.SortDirectives {
			fields := sortDirective.Fields
			desc := sortDirective.Order == sqltypes.DESC

			if isLabelsFieldList(fields) {
				labelName := fields[2]
				alias, ok := q.AliasFor(labelName)
				if !ok {
					// This shouldn't happen if WithUnboundSortLabels ran first
					q, alias = q.EnsureLabelJoin(labelName)
				}

				fieldExpr := sqlexpr.Expr(sqlexpr.Col{Table: alias, Name: "value"})
				if sortDirective.SortAsIP {
					fieldExpr = sqlexpr.FuncCall{Name: "inet_aton", Args: []sqlexpr.Expr{fieldExpr}}
				}

				var nulls sqlexpr.NullsPosition
				if desc {
					nulls = sqlexpr.NullsFirst
				} else {
					nulls = sqlexpr.NullsLast
				}
				q = q.AddOrderBy(sqlexpr.OrderBy{Expr: fieldExpr, Desc: desc, Nulls: nulls})
			} else {
				fieldExpr, err := registry.Resolve(q.Prefix(), fields, true)
				if err != nil {
					return q, err
				}
				if sortDirective.SortAsIP {
					fieldExpr = sqlexpr.FuncCall{Name: "inet_aton", Args: []sqlexpr.Expr{fieldExpr}}
				}
				q = q.AddOrderBy(sqlexpr.OrderBy{Expr: fieldExpr, Desc: desc})
			}
		}

		return q, nil
	}
}

// WithPagination adds LIMIT and OFFSET.
func WithPagination(pagination sqltypes.Pagination) Transform {
	return func(q QueryState) (QueryState, error) {
		if pagination.PageSize > 0 {
			q = q.SetLimit(pagination.PageSize)
			if pagination.Page >= 1 {
				offset := pagination.PageSize * (pagination.Page - 1)
				if offset > 0 {
					q = q.SetOffset(offset)
				}
			}
		}
		return q, nil
	}
}

// GetUnboundSortLabels identifies labels that appear in sort but not in bound filters.
func GetUnboundSortLabels(lo *sqltypes.ListOptions) []string {
	if len(lo.SortList.SortDirectives) == 0 {
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

// --- Also keep CompileOrFilter and CompilePartitions as public for backward compat ---

// CompileOrFilter compiles an OR filter group (public wrapper for tests).
func CompileOrFilter(orFilter sqltypes.OrFilter, registry FieldRegistry, prefix string, isSummaryFilter bool, dbName string, jc *JoinContext) (sqlexpr.Expr, error) {
	// Build a temporary QueryState from the JoinContext for dispatch
	q := QueryState{
		dbName:      dbName,
		prefix:      prefix,
		labelLookup: make(map[string]string),
	}
	// Copy existing aliases from JoinContext
	for label, alias := range jc.labelIndex {
		q.labelLookup[label] = alias
	}
	return compileOrFilter(orFilter, registry, q, isSummaryFilter)
}

// CompileProjectsOrNamespaces compiles project/namespace filters (public wrapper).
func CompileProjectsOrNamespaces(orFilter sqltypes.OrFilter, registry FieldRegistry, dbName string, jc *JoinContext) (sqlexpr.Expr, []sqlexpr.Join, error) {
	if len(orFilter.Filters) == 0 {
		return nil, nil, nil
	}

	prefix := jc.prefix

	// nsf JOIN
	extraJoins := []sqlexpr.Join{
		{
			Kind:  sqlexpr.LeftOuterJoin,
			Table: sqlexpr.TableRef{Name: namespacesDbName + "_fields", Alias: "nsf"},
			On: sqlexpr.Raw{
				SQL: fmt.Sprintf(`%s."metadata.namespace" = nsf."metadata.name"`, prefix),
			},
		},
	}

	ltAlias, exists := jc.AliasFor(projectIDFieldLabel)
	if !exists {
		ltAlias = jc.NextAlias()
		jc.RegisterAlias(projectIDFieldLabel, ltAlias)
	}

	nsProjJoin := sqlexpr.Join{
		Kind:  sqlexpr.LeftOuterJoin,
		Table: sqlexpr.TableRef{Name: namespacesDbName + "_labels", Alias: ltAlias},
		On:    sqlexpr.Raw{SQL: fmt.Sprintf("nsf.key = %s.key", ltAlias)},
	}
	extraJoins = append(extraJoins, nsProjJoin)

	var exprs []sqlexpr.Expr
	for _, filter := range orFilter.Filters {
		var expr sqlexpr.Expr
		var err error

		if isLabelFilter(&filter) {
			handler, ok := ProjectLabelOps[filter.Op]
			if !ok {
				return nil, nil, fmt.Errorf("unrecognized operator: %s", filter.Op)
			}
			expr, err = handler(filter, ltAlias, dbName)
		} else {
			col, resolveErr := registry.Resolve("nsf", filter.Field, false)
			if resolveErr != nil {
				return nil, nil, resolveErr
			}
			handler, ok := ProjectFieldOps[filter.Op]
			if !ok {
				return nil, nil, fmt.Errorf("project or namespaces supports only 'IN' or 'NOT IN' operation. op: %s is not valid", filter.Op)
			}
			expr, err = handler(col, filter)
		}
		if err != nil {
			return nil, nil, err
		}
		exprs = append(exprs, expr)
	}

	if len(exprs) == 0 {
		return nil, nil, nil
	}

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
			result = sqlexpr.InlineAnd(exprs)
		}
	} else {
		return nil, nil, fmt.Errorf("project or namespaces supports only 'IN' or 'NOT IN' operation. op: %s is not valid", orFilter.Filters[0].Op)
	}

	return result, extraJoins, nil
}
