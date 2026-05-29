package sqlgen

import (
	"fmt"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

// LabelContext provides the immutable context needed by label op handlers.
type LabelContext struct {
	Alias           string // join alias (e.g., "lt1")
	MainFieldPrefix string // e.g., "f" or "f1"
	IsSummaryFilter bool
	DBName          string
}

// LabelOpHandler is a pure function: (filter, context) → SQL expression.
type LabelOpHandler func(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error)

// LabelOps maps each operator to its label handler.
var LabelOps = map[sqltypes.Op]LabelOpHandler{
	sqltypes.Eq:          labelEq,
	sqltypes.NotEq:       labelNotEq,
	sqltypes.Lt:          labelCmp,
	sqltypes.Gt:          labelCmp,
	sqltypes.Exists:      labelExists,
	sqltypes.NotExists:   labelNotExists,
	sqltypes.In:          labelIn,
	sqltypes.NotIn:       labelNotIn,
	sqltypes.Contains:    labelContains,
	sqltypes.NotContains: labelNotContains,
}

func labelEq(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error) {
	labelName := f.Field[2]
	param := formatMatchTargetForLabel(f)
	labelIs := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ctx.Alias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
	}
	if f.Partial {
		valueLike := sqlexpr.Like{
			Col: sqlexpr.Col{Table: ctx.Alias, Name: "value"}, Pattern: sqlexpr.Param{Value: param},
		}
		return sqlexpr.FlatAnd{labelIs, valueLike}, nil
	}
	valueEq := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ctx.Alias, Name: "value"}, Op: "=", Right: sqlexpr.Param{Value: param},
	}
	return sqlexpr.FlatAnd{labelIs, valueEq}, nil
}

func labelNotEq(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error) {
	labelName := f.Field[2]
	param := formatMatchTargetForLabel(f)

	notExistsExpr, err := compileLabelNotExists(ctx.Alias, labelName, ctx.MainFieldPrefix, ctx.IsSummaryFilter, ctx.DBName)
	if err != nil {
		return nil, err
	}

	labelIs := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ctx.Alias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
	}
	if f.Partial {
		valueNotLike := sqlexpr.Like{
			Col: sqlexpr.Col{Table: ctx.Alias, Name: "value"}, Pattern: sqlexpr.Param{Value: param}, Negate: true,
		}
		return sqlexpr.FlatOr{notExistsExpr, sqlexpr.FlatAnd{labelIs, valueNotLike}}, nil
	}
	valueNe := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ctx.Alias, Name: "value"}, Op: "!=", Right: sqlexpr.Param{Value: param},
	}
	return sqlexpr.FlatOr{notExistsExpr, sqlexpr.FlatAnd{labelIs, valueNe}}, nil
}

func labelCmp(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error) {
	labelName := f.Field[2]
	sym, target, err := prepareComparisonParameters(f.Op, f.Matches[0])
	if err != nil {
		return nil, err
	}
	labelIs := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ctx.Alias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
	}
	valueCmp := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ctx.Alias, Name: "value"}, Op: sym, Right: sqlexpr.Param{Value: target},
	}
	return sqlexpr.FlatAnd{labelIs, valueCmp}, nil
}

func labelExists(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error) {
	labelName := f.Field[2]
	return sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ctx.Alias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
	}, nil
}

func labelNotExists(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error) {
	labelName := f.Field[2]
	return compileLabelNotExists(ctx.Alias, labelName, ctx.MainFieldPrefix, ctx.IsSummaryFilter, ctx.DBName)
}

func labelIn(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error) {
	labelName := f.Field[2]
	labelIs := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ctx.Alias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
	}
	valueIn := sqlexpr.In{Expr: sqlexpr.Col{Table: ctx.Alias, Name: "value"}, Values: toParams(f.Matches)}
	return sqlexpr.FlatAnd{labelIs, valueIn}, nil
}

func labelNotIn(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error) {
	labelName := f.Field[2]

	notExistsExpr, err := compileLabelNotExists(ctx.Alias, labelName, ctx.MainFieldPrefix, ctx.IsSummaryFilter, ctx.DBName)
	if err != nil {
		return nil, err
	}

	labelIs := sqlexpr.Compare{
		Left: sqlexpr.Col{Table: ctx.Alias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
	}
	valueNotIn := sqlexpr.In{Expr: sqlexpr.Col{Table: ctx.Alias, Name: "value"}, Values: toParams(f.Matches), Negate: true}
	return sqlexpr.FlatOr{notExistsExpr, sqlexpr.FlatAnd{labelIs, valueNotIn}}, nil
}

func labelContains(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error) {
	if len(f.Matches) != 1 {
		return nil, fmt.Errorf("array checking works on exactly one field, %d were specified", len(f.Matches))
	}
	f.Op = sqltypes.Eq
	return labelEq(f, ctx)
}

func labelNotContains(f sqltypes.Filter, ctx LabelContext) (sqlexpr.Expr, error) {
	if len(f.Matches) != 1 {
		return nil, fmt.Errorf("array checking works on exactly one field, %d were specified", len(f.Matches))
	}
	f.Op = sqltypes.NotEq
	return labelNotEq(f, ctx)
}

// CompileLabelFilter is the public dispatch function for label filters.
func CompileLabelFilter(filter sqltypes.Filter, ltAlias string, mainFieldPrefix string, isSummaryFilter bool, dbName string) (sqlexpr.Expr, error) {
	ctx := LabelContext{
		Alias:           ltAlias,
		MainFieldPrefix: mainFieldPrefix,
		IsSummaryFilter: isSummaryFilter,
		DBName:          dbName,
	}
	handler, ok := LabelOps[filter.Op]
	if !ok {
		return nil, fmt.Errorf("unrecognized operator: %s", filter.Op)
	}
	return handler(filter, ctx)
}

// compileLabelNotExists creates the NOT EXISTS subquery pattern for label filters.
func compileLabelNotExists(ltAlias, labelName, mainFieldPrefix string, isSummaryFilter bool, dbName string) (sqlexpr.Expr, error) {
	var index int
	fmt.Sscanf(ltAlias, "lt%d", &index)

	innerAlias := fmt.Sprintf("lt%di1", index)

	var keyPrefix, subFieldPrefix string
	if isSummaryFilter {
		keyPrefix = "f1"
		subFieldPrefix = "f11"
	} else {
		keyPrefix = "o"
		subFieldPrefix = "f1"
	}

	subquery := fmt.Sprintf(`%s.key NOT IN (SELECT %s.key FROM "%s_fields" %s`+"\n"+
		`		LEFT OUTER JOIN "%s_labels" %s ON %s.key = %s.key`+"\n"+
		`		WHERE %s.label = ?)`,
		keyPrefix, subFieldPrefix, dbName, subFieldPrefix,
		dbName, innerAlias, subFieldPrefix, innerAlias,
		innerAlias)

	return sqlexpr.Raw{SQL: subquery, Params: []any{labelName}}, nil
}
