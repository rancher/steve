package sqlgen

import (
	"fmt"

	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

// CompiledQuery holds the result of compiling ListOptions into an expression tree.
type CompiledQuery struct {
	// Query is the main SELECT expression.
	Query sqlexpr.Select
	// CountQuery is the COUNT(*) wrapper (only set when pagination is used).
	CountQuery *sqlexpr.Expr
	// HasPagination indicates whether LIMIT/OFFSET are set.
	HasPagination bool
}

// Resolve produces the final SQL and params from the compiled query.
func (cq *CompiledQuery) Resolve() (query string, params []any, countQuery string, countParams []any) {
	query, params = cq.Query.Resolve()
	if cq.HasPagination {
		// Build count query: same as main query but without ORDER BY, LIMIT, OFFSET
		countSelect := cq.Query
		countSelect.OrderBy = nil
		countSelect.Limit = nil
		countSelect.Offset = nil
		countExpr := sqlexpr.CountWrap(countSelect)
		countQuery, countParams = countExpr.Resolve()
	}
	return
}

// CompileListQuery compiles ListOptions, partitions, and namespace into an expression tree.
// This replaces the old compileQuery + generateSQL combination.
func CompileListQuery(lo *sqltypes.ListOptions, partitions []partition.Partition,
	namespace, dbName string, registry FieldRegistry, namespaced bool) (*CompiledQuery, error) {

	const mainObjectPrefix = "o"
	const mainFieldPrefix = "f"

	jc := NewJoinContext(dbName, mainFieldPrefix)

	// Determine if sort is needed
	includeSort := true // constructQuery always includes sort

	// Handle unbound sort labels first (they need CTEs and view joins)
	var ctes []sqlexpr.WithClause
	if includeSort {
		unboundSortLabels := GetUnboundSortLabels(lo)
		for _, label := range unboundSortLabels {
			jc.counter++
			alias := fmt.Sprintf("lt%d", jc.counter)
			jc.labelIndex[label] = alias

			ctes = append(ctes, sqlexpr.WithClause{
				Name:    alias,
				Columns: []string{"key", "value"},
				Body: sqlexpr.Raw{
					SQL:    fmt.Sprintf("SELECT key, value FROM \"%s_labels\"\n  WHERE label = ?", dbName),
					Params: []any{label},
				},
			})

			// View join (no table name, only alias)
			jc.joins = append(jc.joins, sqlexpr.Join{
				Kind:  sqlexpr.LeftOuterJoin,
				Table: sqlexpr.TableRef{Alias: alias},
				On: sqlexpr.Raw{
					SQL: fmt.Sprintf("%s.key = %s.key", mainFieldPrefix, alias),
				},
			})
			jc.UsesLabels = true
		}
	}

	// Pre-register label JOINs needed by filters (so they exist before compiling)
	for _, orFilter := range lo.Filters {
		for _, filter := range orFilter.Filters {
			if isLabelFilter(&filter) {
				jc.EnsureLabelJoin(filter.Field[2])
			}
		}
	}

	// Compile WHERE subclauses
	var whereParts []sqlexpr.Expr

	// 1. Filter clauses
	for _, orFilter := range lo.Filters {
		expr, err := CompileOrFilter(orFilter, registry, mainFieldPrefix, false, dbName, jc)
		if err != nil {
			return nil, err
		}
		if expr != nil {
			whereParts = append(whereParts, expr)
		}
	}

	// 2. Projects/Namespaces filter
	if len(lo.ProjectsOrNamespaces.Filters) > 0 {
		projExpr, extraJoins, err := CompileProjectsOrNamespaces(lo.ProjectsOrNamespaces, registry, dbName, jc)
		if err != nil {
			return nil, err
		}
		if projExpr != nil {
			whereParts = append(whereParts, projExpr)
		}
		// Add the extra joins (nsf + namespace labels)
		jc.joins = append(jc.joins, extraJoins...)
	}

	// 3. Namespace filter
	if namespace != "" && namespace != "*" {
		nsExpr := sqlexpr.Compare{
			Left:  sqlexpr.Col{Table: mainFieldPrefix, Name: "metadata.namespace"},
			Op:    "=",
			Right: sqlexpr.Param{Value: namespace},
		}
		whereParts = append(whereParts, nsExpr)
	}

	// 4. Partition clauses
	partExpr := CompilePartitions(namespace, partitions, mainFieldPrefix)
	if partExpr != nil {
		whereParts = append(whereParts, partExpr)
	}

	// Compose WHERE
	var where sqlexpr.Expr
	if len(whereParts) > 0 {
		if len(whereParts) == 1 {
			where = whereParts[0]
		} else {
			where = sqlexpr.And(whereParts)
		}
	}

	// Compile ORDER BY
	var orderBy []sqlexpr.OrderBy
	if includeSort {
		var sortCTEs []sqlexpr.WithClause
		var err error
		orderBy, sortCTEs, err = CompileSort(lo.SortList, registry, mainFieldPrefix, namespaced, jc)
		if err != nil {
			return nil, err
		}
		// Note: sortCTEs should be empty if we already handled unbound labels above
		// but append any extras just in case
		ctes = append(ctes, sortCTEs...)
	}

	// Compile pagination
	var limit, offset *int
	hasPagination := false
	if lo.Pagination.PageSize > 0 {
		l := lo.Pagination.PageSize
		limit = &l
		hasPagination = true

		if lo.Pagination.Page >= 1 {
			o := lo.Pagination.PageSize * (lo.Pagination.Page - 1)
			if o > 0 {
				offset = &o
			}
		}
	}

	// Assemble the base fields JOIN
	baseJoin := sqlexpr.Join{
		Kind:  sqlexpr.InnerJoin,
		Table: sqlexpr.TableRef{Name: fmt.Sprintf("%s_fields", dbName), Alias: mainFieldPrefix},
		On: sqlexpr.Raw{
			SQL: fmt.Sprintf("%s.key = %s.key", mainObjectPrefix, mainFieldPrefix),
		},
	}

	allJoins := append([]sqlexpr.Join{baseJoin}, jc.Joins()...)

	query := sqlexpr.Select{
		CTEs:     ctes,
		Distinct: jc.UsesLabels,
		Columns: []sqlexpr.Expr{
			sqlexpr.Raw{SQL: mainObjectPrefix + ".object"},
			sqlexpr.Raw{SQL: mainObjectPrefix + ".objectnonce"},
			sqlexpr.Raw{SQL: mainObjectPrefix + ".dekid"},
		},
		From:    sqlexpr.TableRef{Name: dbName, Alias: mainObjectPrefix},
		Joins:   allJoins,
		Where:   where,
		OrderBy: orderBy,
		Limit:   limit,
		Offset:  offset,
	}

	return &CompiledQuery{
		Query:         query,
		HasPagination: hasPagination,
	}, nil
}

// CompileSummaryListQuery compiles the filter portion for summary queries.
// This uses "f1" as the field prefix (matching the old code's behavior for summaries).
func CompileSummaryListQuery(lo *sqltypes.ListOptions, partitions []partition.Partition,
	namespace, dbName string, registry FieldRegistry, includeSort bool) (*SummaryCompilation, error) {

	const mainFieldPrefix = "f1"

	jc := NewJoinContext(dbName, mainFieldPrefix)

	// Force sort if we have pagination
	if !includeSort && lo.Pagination.PageSize > 0 {
		includeSort = true
	}

	// Handle unbound sort labels
	var ctes []sqlexpr.WithClause
	if includeSort {
		unboundSortLabels := GetUnboundSortLabels(lo)
		for _, label := range unboundSortLabels {
			jc.counter++
			alias := fmt.Sprintf("lt%d", jc.counter)
			jc.labelIndex[label] = alias
			ctes = append(ctes, sqlexpr.WithClause{
				Name:    alias,
				Columns: []string{"key", "value"},
				Body: sqlexpr.Raw{
					SQL:    fmt.Sprintf("SELECT key, value FROM \"%s_labels\"\n  WHERE label = ?", dbName),
					Params: []any{label},
				},
			})
			jc.joins = append(jc.joins, sqlexpr.Join{
				Kind:  sqlexpr.LeftOuterJoin,
				Table: sqlexpr.TableRef{Alias: alias},
				On: sqlexpr.Raw{
					SQL: fmt.Sprintf("%s.key = %s.key", mainFieldPrefix, alias),
				},
			})
			jc.UsesLabels = true
		}
	}

	// Pre-register label JOINs
	for _, orFilter := range lo.Filters {
		for _, filter := range orFilter.Filters {
			if isLabelFilter(&filter) {
				jc.EnsureLabelJoin(filter.Field[2])
			}
		}
	}

	// Compile WHERE
	var whereParts []sqlexpr.Expr
	for _, orFilter := range lo.Filters {
		expr, err := CompileOrFilter(orFilter, registry, mainFieldPrefix, true, dbName, jc)
		if err != nil {
			return nil, err
		}
		if expr != nil {
			whereParts = append(whereParts, expr)
		}
	}

	if len(lo.ProjectsOrNamespaces.Filters) > 0 {
		projExpr, extraJoins, err := CompileProjectsOrNamespaces(lo.ProjectsOrNamespaces, registry, dbName, jc)
		if err != nil {
			return nil, err
		}
		if projExpr != nil {
			whereParts = append(whereParts, projExpr)
		}
		jc.joins = append(jc.joins, extraJoins...)
	}

	if namespace != "" && namespace != "*" {
		nsExpr := sqlexpr.Compare{
			Left:  sqlexpr.Col{Table: mainFieldPrefix, Name: "metadata.namespace"},
			Op:    "=",
			Right: sqlexpr.Param{Value: namespace},
		}
		whereParts = append(whereParts, nsExpr)
	}

	partExpr := CompilePartitions(namespace, partitions, mainFieldPrefix)
	if partExpr != nil {
		whereParts = append(whereParts, partExpr)
	}

	var where sqlexpr.Expr
	if len(whereParts) > 0 {
		if len(whereParts) == 1 {
			where = whereParts[0]
		} else {
			where = sqlexpr.And(whereParts)
		}
	}

	// Compile ORDER BY
	var orderBy []sqlexpr.OrderBy
	if includeSort {
		var sortCTEs []sqlexpr.WithClause
		var err error
		orderBy, sortCTEs, err = CompileSort(lo.SortList, registry, mainFieldPrefix, false, jc)
		if err != nil {
			return nil, err
		}
		ctes = append(ctes, sortCTEs...)
	}

	// Pagination
	var limit, offset *int
	if lo.Pagination.PageSize > 0 {
		l := lo.Pagination.PageSize
		limit = &l
		if lo.Pagination.Page >= 1 {
			o := lo.Pagination.PageSize * (lo.Pagination.Page - 1)
			if o > 0 {
				offset = &o
			}
		}
	}

	isEmpty := where == nil && len(jc.Joins()) == 0 && len(orderBy) == 0 && limit == nil

	return &SummaryCompilation{
		Where:      where,
		Joins:      jc.Joins(),
		OrderBy:    orderBy,
		CTEs:       ctes,
		Limit:      limit,
		Offset:     offset,
		Prefix:     mainFieldPrefix,
		JoinCtx:    jc,
		UsesLabels: jc.UsesLabels,
		IsEmpty:    isEmpty,
	}, nil
}

// SummaryCompilation holds the pre-compiled components for summary queries.
// The summary query can be either "simple" (no filters) or "complex" (filters active).
type SummaryCompilation struct {
	Where      sqlexpr.Expr
	Joins      []sqlexpr.Join
	OrderBy    []sqlexpr.OrderBy
	CTEs       []sqlexpr.WithClause
	Limit      *int
	Offset     *int
	Prefix     string
	JoinCtx    *JoinContext
	UsesLabels bool
	IsEmpty    bool
}
