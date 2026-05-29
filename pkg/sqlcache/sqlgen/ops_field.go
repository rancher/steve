package sqlgen

import (
	"errors"
	"fmt"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

// FieldOpHandler is a pure function: (column expression, filter) → SQL expression.
// No side effects, no state mutation.
type FieldOpHandler func(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error)

// FieldOps maps each operator to its handler. Adding a new op = one function + one entry.
var FieldOps = map[sqltypes.Op]FieldOpHandler{
	sqltypes.Eq:          fieldEq,
	sqltypes.NotEq:       fieldNotEq,
	sqltypes.Lt:          fieldLt,
	sqltypes.Gt:          fieldGt,
	sqltypes.In:          fieldIn,
	sqltypes.NotIn:       fieldNotIn,
	sqltypes.Contains:    fieldContains,
	sqltypes.NotContains: fieldNotContains,
	sqltypes.Exists:      fieldExistsUnsupported,
	sqltypes.NotExists:   fieldExistsUnsupported,
}

func fieldEq(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	param := formatMatchTarget(f)
	if f.Partial {
		return sqlexpr.Like{Col: col, Pattern: sqlexpr.Param{Value: param}}, nil
	}
	return sqlexpr.Compare{Left: col, Op: "=", Right: sqlexpr.Param{Value: param}}, nil
}

func fieldNotEq(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	param := formatMatchTarget(f)
	if f.Partial {
		return sqlexpr.Like{Col: col, Pattern: sqlexpr.Param{Value: param}, Negate: true}, nil
	}
	return sqlexpr.Compare{Left: col, Op: "!=", Right: sqlexpr.Param{Value: param}}, nil
}

func fieldLt(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	sym, target, err := prepareComparisonParameters(f.Op, f.Matches[0])
	if err != nil {
		return nil, err
	}
	return sqlexpr.Compare{Left: col, Op: sym, Right: sqlexpr.Param{Value: target}}, nil
}

func fieldGt(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	sym, target, err := prepareComparisonParameters(f.Op, f.Matches[0])
	if err != nil {
		return nil, err
	}
	return sqlexpr.Compare{Left: col, Op: sym, Right: sqlexpr.Param{Value: target}}, nil
}

func fieldIn(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	return sqlexpr.In{Expr: col, Values: toParams(f.Matches)}, nil
}

func fieldNotIn(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	return sqlexpr.In{Expr: col, Values: toParams(f.Matches), Negate: true}, nil
}

func fieldContains(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	if len(f.Matches) != 1 {
		return nil, fmt.Errorf("array checking works on exactly one field, %d were specified", len(f.Matches))
	}
	return sqlexpr.FuncCall{
		Name: "hasBarredValue",
		Args: []sqlexpr.Expr{col, sqlexpr.Param{Value: f.Matches[0]}},
	}, nil
}

func fieldNotContains(col sqlexpr.Expr, f sqltypes.Filter) (sqlexpr.Expr, error) {
	if len(f.Matches) != 1 {
		return nil, fmt.Errorf("array checking works on exactly one field, %d were specified", len(f.Matches))
	}
	return sqlexpr.Not{Inner: sqlexpr.FuncCall{
		Name: "hasBarredValue",
		Args: []sqlexpr.Expr{col, sqlexpr.Param{Value: f.Matches[0]}},
	}}, nil
}

func fieldExistsUnsupported(_ sqlexpr.Expr, _ sqltypes.Filter) (sqlexpr.Expr, error) {
	return nil, errors.New("NULL and NOT NULL tests aren't supported for non-label queries")
}

// CompileFieldFilter is the public dispatch function for field filters.
// It resolves the column, looks up the handler, and delegates.
func CompileFieldFilter(filter sqltypes.Filter, registry FieldRegistry, prefix string) (sqlexpr.Expr, error) {
	col, err := registry.Resolve(prefix, filter.Field, false)
	if err != nil {
		return nil, err
	}
	handler, ok := FieldOps[filter.Op]
	if !ok {
		return nil, fmt.Errorf("unrecognized operator: %s", filter.Op)
	}
	return handler(col, filter)
}

// --- Helpers ---

func toParams(matches []string) []sqlexpr.Expr {
	params := make([]sqlexpr.Expr, len(matches))
	for i, m := range matches {
		params[i] = sqlexpr.Param{Value: m}
	}
	return params
}
