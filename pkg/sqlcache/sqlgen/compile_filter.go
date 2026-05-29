package sqlgen

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

const (
	escapeBackslashDirective = ` ESCAPE '\'`
	matchFmt                 = `%%%s%%`
	strictMatchFmt           = `%s`
)

// CompileFieldFilter compiles a single field filter into an Expr.
func CompileFieldFilter(filter sqltypes.Filter, registry FieldRegistry, prefix string) (sqlexpr.Expr, error) {
	col, err := registry.Resolve(prefix, filter.Field, false)
	if err != nil {
		return nil, err
	}

	switch filter.Op {
	case sqltypes.Eq:
		param := formatMatchTarget(filter)
		if filter.Partial {
			return sqlexpr.Like{Col: col, Pattern: sqlexpr.Param{Value: param}}, nil
		}
		return sqlexpr.Compare{Left: col, Op: "=", Right: sqlexpr.Param{Value: param}}, nil

	case sqltypes.NotEq:
		param := formatMatchTarget(filter)
		if filter.Partial {
			return sqlexpr.Like{Col: col, Pattern: sqlexpr.Param{Value: param}, Negate: true}, nil
		}
		return sqlexpr.Compare{Left: col, Op: "!=", Right: sqlexpr.Param{Value: param}}, nil

	case sqltypes.Lt, sqltypes.Gt:
		sym, target, err := prepareComparisonParameters(filter.Op, filter.Matches[0])
		if err != nil {
			return nil, err
		}
		return sqlexpr.Compare{Left: col, Op: sym, Right: sqlexpr.Param{Value: target}}, nil

	case sqltypes.Exists, sqltypes.NotExists:
		return nil, errors.New("NULL and NOT NULL tests aren't supported for non-label queries")

	case sqltypes.In, sqltypes.NotIn:
		params := make([]sqlexpr.Expr, len(filter.Matches))
		for i, m := range filter.Matches {
			params[i] = sqlexpr.Param{Value: m}
		}
		return sqlexpr.In{Expr: col, Values: params, Negate: filter.Op == sqltypes.NotIn}, nil

	case sqltypes.Contains:
		if len(filter.Matches) != 1 {
			return nil, fmt.Errorf("array checking works on exactly one field, %d were specified", len(filter.Matches))
		}
		return sqlexpr.FuncCall{
			Name: "hasBarredValue",
			Args: []sqlexpr.Expr{col, sqlexpr.Param{Value: filter.Matches[0]}},
		}, nil

	case sqltypes.NotContains:
		if len(filter.Matches) != 1 {
			return nil, fmt.Errorf("array checking works on exactly one field, %d were specified", len(filter.Matches))
		}
		return sqlexpr.Not{Inner: sqlexpr.FuncCall{
			Name: "hasBarredValue",
			Args: []sqlexpr.Expr{col, sqlexpr.Param{Value: filter.Matches[0]}},
		}}, nil
	}

	return nil, fmt.Errorf("unrecognized operator: %s", filter.Op)
}

// CompileLabelFilter compiles a single label filter into an Expr.
// ltAlias is the alias for the label join table (e.g., "lt1").
// isSummaryFilter and dbName are used for NotExists subqueries.
func CompileLabelFilter(filter sqltypes.Filter, ltAlias string, mainFieldPrefix string, isSummaryFilter bool, dbName string) (sqlexpr.Expr, error) {
	labelName := filter.Field[2]

	switch filter.Op {
	case sqltypes.Eq:
		param := formatMatchTargetForLabel(filter)
		labelIs := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
		}
		if filter.Partial {
			valueLike := sqlexpr.Like{
				Col: sqlexpr.Col{Table: ltAlias, Name: "value"}, Pattern: sqlexpr.Param{Value: param},
			}
			return sqlexpr.FlatAnd{labelIs, valueLike}, nil
		}
		valueEq := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "value"}, Op: "=", Right: sqlexpr.Param{Value: param},
		}
		return sqlexpr.FlatAnd{labelIs, valueEq}, nil

	case sqltypes.NotEq:
		param := formatMatchTargetForLabel(filter)
		// NotEq for labels: (NOT EXISTS label) OR (label=? AND value != ?)
		notExistsExpr, err := compileLabelNotExists(ltAlias, labelName, mainFieldPrefix, isSummaryFilter, dbName)
		if err != nil {
			return nil, err
		}
		labelIs := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
		}
		if filter.Partial {
			valueNotLike := sqlexpr.Like{
				Col: sqlexpr.Col{Table: ltAlias, Name: "value"}, Pattern: sqlexpr.Param{Value: param}, Negate: true,
			}
			return sqlexpr.FlatOr{notExistsExpr, sqlexpr.FlatAnd{labelIs, valueNotLike}}, nil
		}
		valueNe := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "value"}, Op: "!=", Right: sqlexpr.Param{Value: param},
		}
		return sqlexpr.FlatOr{notExistsExpr, sqlexpr.FlatAnd{labelIs, valueNe}}, nil

	case sqltypes.Lt, sqltypes.Gt:
		sym, target, err := prepareComparisonParameters(filter.Op, filter.Matches[0])
		if err != nil {
			return nil, err
		}
		labelIs := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
		}
		valueCmp := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "value"}, Op: sym, Right: sqlexpr.Param{Value: target},
		}
		return sqlexpr.FlatAnd{labelIs, valueCmp}, nil

	case sqltypes.Exists:
		return sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
		}, nil

	case sqltypes.NotExists:
		return compileLabelNotExists(ltAlias, labelName, mainFieldPrefix, isSummaryFilter, dbName)

	case sqltypes.In:
		labelIs := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
		}
		params := make([]sqlexpr.Expr, len(filter.Matches))
		for i, m := range filter.Matches {
			params[i] = sqlexpr.Param{Value: m}
		}
		valueIn := sqlexpr.In{Expr: sqlexpr.Col{Table: ltAlias, Name: "value"}, Values: params}
		return sqlexpr.FlatAnd{labelIs, valueIn}, nil

	case sqltypes.NotIn:
		// (NOT EXISTS) OR (label=? AND value NOT IN (?...))
		notExistsExpr, err := compileLabelNotExists(ltAlias, labelName, mainFieldPrefix, isSummaryFilter, dbName)
		if err != nil {
			return nil, err
		}
		labelIs := sqlexpr.Compare{
			Left: sqlexpr.Col{Table: ltAlias, Name: "label"}, Op: "=", Right: sqlexpr.Param{Value: labelName},
		}
		params := make([]sqlexpr.Expr, len(filter.Matches))
		for i, m := range filter.Matches {
			params[i] = sqlexpr.Param{Value: m}
		}
		valueNotIn := sqlexpr.In{Expr: sqlexpr.Col{Table: ltAlias, Name: "value"}, Values: params, Negate: true}
		return sqlexpr.FlatOr{notExistsExpr, sqlexpr.FlatAnd{labelIs, valueNotIn}}, nil

	case sqltypes.Contains:
		if len(filter.Matches) != 1 {
			return nil, fmt.Errorf("array checking works on exactly one field, %d were specified", len(filter.Matches))
		}
		// Labels can't have | characters so Contains is implemented like Eq
		filter.Op = sqltypes.Eq
		return CompileLabelFilter(filter, ltAlias, mainFieldPrefix, isSummaryFilter, dbName)

	case sqltypes.NotContains:
		if len(filter.Matches) != 1 {
			return nil, fmt.Errorf("array checking works on exactly one field, %d were specified", len(filter.Matches))
		}
		// Labels can't have | characters so NotContains is implemented like NotEq
		filter.Op = sqltypes.NotEq
		return CompileLabelFilter(filter, ltAlias, mainFieldPrefix, isSummaryFilter, dbName)
	}

	return nil, fmt.Errorf("unrecognized operator: %s", filter.Op)
}

// compileLabelNotExists creates the NOT EXISTS subquery pattern for label filters.
// Pattern: key NOT IN (SELECT subKey FROM "db_fields" subPrefix LEFT OUTER JOIN "db_labels" ltNi1 ON ... WHERE ltNi1.label = ?)
func compileLabelNotExists(ltAlias, labelName, mainFieldPrefix string, isSummaryFilter bool, dbName string) (sqlexpr.Expr, error) {
	// Extract the index from the alias to generate the inner alias
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

	// Build: keyPrefix.key NOT IN (SELECT subFieldPrefix.key FROM "dbName_fields" subFieldPrefix
	//   LEFT OUTER JOIN "dbName_labels" innerAlias ON subFieldPrefix.key = innerAlias.key
	//   WHERE innerAlias.label = ?)
	subquery := fmt.Sprintf(`%s.key NOT IN (SELECT %s.key FROM "%s_fields" %s`+"\n"+
		`		LEFT OUTER JOIN "%s_labels" %s ON %s.key = %s.key`+"\n"+
		`		WHERE %s.label = ?)`,
		keyPrefix, subFieldPrefix, dbName, subFieldPrefix,
		dbName, innerAlias, subFieldPrefix, innerAlias,
		innerAlias)

	return sqlexpr.Raw{SQL: subquery, Params: []any{labelName}}, nil
}

// CompileOrFilter compiles an OR filter group into a single Expr.
func CompileOrFilter(orFilter sqltypes.OrFilter, registry FieldRegistry, prefix string, isSummaryFilter bool, dbName string, jc *JoinContext) (sqlexpr.Expr, error) {
	if len(orFilter.Filters) == 0 {
		return nil, nil
	}

	exprs := make([]sqlexpr.Expr, 0, len(orFilter.Filters))
	for _, filter := range orFilter.Filters {
		var expr sqlexpr.Expr
		var err error

		if isLabelFilter(&filter) {
			ltAlias := jc.EnsureLabelJoin(filter.Field[2])
			expr, err = CompileLabelFilter(filter, ltAlias, jc.prefix, isSummaryFilter, dbName)
		} else {
			expr, err = CompileFieldFilter(filter, registry, prefix)
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

// Helper functions

func formatMatchTarget(filter sqltypes.Filter) string {
	format := strictMatchFmt
	if filter.Partial {
		format = matchFmt
	}
	return formatMatchTargetWithFormatter(filter.Matches[0], format)
}

func formatMatchTargetForLabel(filter sqltypes.Filter) string {
	format := strictMatchFmt
	if filter.Partial {
		format = matchFmt
	}
	return formatMatchTargetWithFormatter(filter.Matches[0], format)
}

func formatMatchTargetWithFormatter(match string, format string) string {
	match = strings.ReplaceAll(match, `\`, `\\`)
	match = strings.ReplaceAll(match, `_`, `\_`)
	match = strings.ReplaceAll(match, `%`, `\%`)
	return fmt.Sprintf(format, match)
}

func isLabelFilter(f *sqltypes.Filter) bool {
	return len(f.Field) >= 2 && f.Field[0] == "metadata" && f.Field[1] == "labels"
}

func isLabelsFieldList(fields []string) bool {
	return len(fields) == 3 && fields[0] == "metadata" && fields[1] == "labels"
}

func prepareComparisonParameters(op sqltypes.Op, target string) (string, float64, error) {
	num, err := parseFloat(target)
	if err != nil {
		return "", 0, err
	}
	switch op {
	case sqltypes.Lt:
		return "<", num, nil
	case sqltypes.Gt:
		return ">", num, nil
	}
	return "", 0, fmt.Errorf("unrecognized operator when expecting '<' or '>': '%s'", op)
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
