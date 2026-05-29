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
		countSelect := cq.Query
		countSelect.OrderBy = nil
		countSelect.Limit = nil
		countSelect.Offset = nil
		countExpr := sqlexpr.CountWrap(countSelect)
		countQuery, countParams = countExpr.Resolve()
	}
	return
}

// CompileListQuery compiles ListOptions into an expression tree using the
// functional pipeline. This is the main entry point for list queries.
func CompileListQuery(lo *sqltypes.ListOptions, partitions []partition.Partition,
	namespace, dbName string, registry FieldRegistry, namespaced bool) (*CompiledQuery, error) {

	const mainObjectPrefix = "o"
	const mainFieldPrefix = "f"

	initial := NewQueryState(dbName, mainFieldPrefix)
	initial.Columns = []sqlexpr.Expr{
		sqlexpr.Raw{SQL: mainObjectPrefix + ".object"},
		sqlexpr.Raw{SQL: mainObjectPrefix + ".objectnonce"},
		sqlexpr.Raw{SQL: mainObjectPrefix + ".dekid"},
	}
	initial.From = sqlexpr.TableRef{Name: dbName, Alias: mainObjectPrefix}

	// Compose the pipeline
	pipeline := Pipeline(
		WithUnboundSortLabels(lo),
		WithFilters(lo, registry, false),
		WithProjects(lo.ProjectsOrNamespaces, registry),
		WithNamespace(namespace),
		WithPartitions(namespace, partitions),
		WithSort(lo.SortList, registry, namespaced),
		WithPagination(lo.Pagination),
	)

	final, err := pipeline(initial)
	if err != nil {
		return nil, err
	}

	// Assemble the base fields JOIN (always first)
	baseJoin := sqlexpr.Join{
		Kind:  sqlexpr.InnerJoin,
		Table: sqlexpr.TableRef{Name: fmt.Sprintf("%s_fields", dbName), Alias: mainFieldPrefix},
		On: sqlexpr.Raw{
			SQL: fmt.Sprintf("%s.key = %s.key", mainObjectPrefix, mainFieldPrefix),
		},
	}

	allJoins := make([]sqlexpr.Join, 0, 1+len(final.Joins))
	allJoins = append(allJoins, baseJoin)
	allJoins = append(allJoins, final.Joins...)

	// Compose WHERE
	var where sqlexpr.Expr
	if len(final.Where) > 0 {
		if len(final.Where) == 1 {
			where = final.Where[0]
		} else {
			where = sqlexpr.And(final.Where)
		}
	}

	query := sqlexpr.Select{
		CTEs:     final.CTEs,
		Distinct: final.usesLabels,
		Columns:  final.Columns,
		From:     final.From,
		Joins:    allJoins,
		Where:    where,
		OrderBy:  final.OrderBy,
		Limit:    final.Limit,
		Offset:   final.Offset,
	}

	return &CompiledQuery{
		Query:         query,
		HasPagination: final.Limit != nil,
	}, nil
}

// CompileSummaryListQuery compiles the filter portion for summary queries.
func CompileSummaryListQuery(lo *sqltypes.ListOptions, partitions []partition.Partition,
	namespace, dbName string, registry FieldRegistry, includeSort bool) (*SummaryCompilation, error) {

	const mainFieldPrefix = "f1"

	// Force sort if we have pagination
	if !includeSort && lo.Pagination.PageSize > 0 {
		includeSort = true
	}

	initial := NewQueryState(dbName, mainFieldPrefix)

	// Build the pipeline
	var transforms []Transform
	transforms = append(transforms, WithUnboundSortLabels(lo))
	transforms = append(transforms, WithFilters(lo, registry, true))

	if len(lo.ProjectsOrNamespaces.Filters) > 0 {
		transforms = append(transforms, WithProjects(lo.ProjectsOrNamespaces, registry))
	}
	transforms = append(transforms, WithNamespace(namespace))
	transforms = append(transforms, WithPartitions(namespace, partitions))

	if includeSort {
		transforms = append(transforms, WithSort(lo.SortList, registry, false))
	}
	transforms = append(transforms, WithPagination(lo.Pagination))

	pipeline := Pipeline(transforms...)
	final, err := pipeline(initial)
	if err != nil {
		return nil, err
	}

	// Compose WHERE
	var where sqlexpr.Expr
	if len(final.Where) > 0 {
		if len(final.Where) == 1 {
			where = final.Where[0]
		} else {
			where = sqlexpr.And(final.Where)
		}
	}

	isEmpty := where == nil && len(final.Joins) == 0 && len(final.OrderBy) == 0 && final.Limit == nil

	// Build a JoinContext for backward compat with summary code
	jc := NewJoinContext(dbName, mainFieldPrefix)
	jc.counter = final.nextAliasIdx
	for _, la := range final.LabelAliases {
		jc.labelIndex[la.Name] = la.Alias
	}
	jc.UsesLabels = final.usesLabels
	jc.joins = final.Joins

	return &SummaryCompilation{
		Where:      where,
		Joins:      final.Joins,
		OrderBy:    final.OrderBy,
		CTEs:       final.CTEs,
		Limit:      final.Limit,
		Offset:     final.Offset,
		Prefix:     mainFieldPrefix,
		JoinCtx:    jc,
		UsesLabels: final.usesLabels,
		IsEmpty:    isEmpty,
	}, nil
}

// SummaryCompilation holds the pre-compiled components for summary queries.
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
