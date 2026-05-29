package sqlgen

import (
	"fmt"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

// ProjectFieldOpHandler handles project/namespace field filter ops.
type ProjectFieldOpHandler func(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error)

// ProjectLabelOpHandler handles project/namespace label filter ops.
type ProjectLabelOpHandler func(f sqltypes.Filter, ltAlias, dbName string) (sqlexpr.Expr, error)

// ProjectFieldOps maps operators for project/namespace field filters.
var ProjectFieldOps = map[sqltypes.Op]ProjectFieldOpHandler{
	sqltypes.In:    projectFieldIn,
	sqltypes.NotIn: projectFieldNotIn,
}

// ProjectLabelOps maps operators for project/namespace label filters.
var ProjectLabelOps = map[sqltypes.Op]ProjectLabelOpHandler{
	sqltypes.In:    projectLabelIn,
	sqltypes.NotIn: projectLabelNotIn,
}

func projectFieldIn(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	return sqlexpr.In{Expr: col, Values: toParams(f.Matches)}, nil
}

func projectFieldNotIn(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	return sqlexpr.In{Expr: col, Values: toParams(f.Matches), Negate: true}, nil
}

func projectLabelIn(f sqltypes.Filter, ltAlias, _ string) (sqlexpr.Expr, error) {
	labelName := f.Field[2]
	labelIs := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
	}
	valueIn := sqlexpr.In{Expr: sqlexpr.Col{Table: ltAlias, Name: "value"}, Values: toParams(f.Matches)}
	return sqlexpr.FlatAnd{labelIs, valueIn}, nil
}

func projectLabelNotIn(f sqltypes.Filter, ltAlias, dbName string) (sqlexpr.Expr, error) {
	labelName := f.Field[2]

	labelIs := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
	}
	valueNotIn := sqlexpr.In{Expr: sqlexpr.Col{Table: ltAlias, Name: "value"}, Values: toParams(f.Matches), Negate: true}
	clause1 := sqlexpr.FlatAnd{labelIs, valueNotIn}

	// Subquery for NOT IN
	var index int
	fmt.Sscanf(ltAlias, "lt%d", &index)
	subquery := fmt.Sprintf(`o.key NOT IN (SELECT f1.key FROM "%s_fields" f1`+"\n"+
		`		LEFT OUTER JOIN "_v1_Namespace_fields" nsf1 ON f1."metadata.namespace" = nsf1."metadata.name"`+"\n"+
		`		LEFT OUTER JOIN "_v1_Namespace_labels" lt%di1 ON nsf1.key = lt%di1.key`+"\n"+
		`		WHERE lt%di1.label = ?)`, dbName, index, index, index)
	clause2 := sqlexpr.Raw{SQL: subquery, Params: []any{labelName}}

	return sqlexpr.FlatOr{clause1, clause2}, nil
}
