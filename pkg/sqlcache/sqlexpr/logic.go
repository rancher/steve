package sqlexpr

// And resolves each child expression and joins them with AND.
// Empty produces no output. Single child resolves without wrapping.
// Multiple children: wraps each in parens with newline-indented separator.
type And []Expr

func (a And) Resolve() (string, []any) {
	return joinExprs([]Expr(a), " AND\n    ")
}

// Or resolves each child expression and joins them with OR.
// Empty produces no output. Single child resolves without wrapping.
type Or []Expr

func (o Or) Resolve() (string, []any) {
	return joinExprs([]Expr(o), " OR ")
}

// FlatAnd joins expressions with " AND " without wrapping in parens or newlines.
// Used for inline conditions like "lt1.label = ? AND lt1.value = ?".
type FlatAnd []Expr

func (f FlatAnd) Resolve() (string, []any) {
	return joinExprsFlat([]Expr(f), " AND ")
}

// InlineAnd wraps each child in parens and joins with " AND " (no newlines).
// Used for project/namespace filters: "(field NOT IN (?)) AND ((label OR subquery))".
type InlineAnd []Expr

func (ia InlineAnd) Resolve() (string, []any) {
	return joinExprs([]Expr(ia), " AND ")
}

// FlatOr joins expressions with " OR " wrapping each part in parens.
// Used for label filter OR patterns like "(notExists) OR (labelMatch)".
type FlatOr []Expr

func (f FlatOr) Resolve() (string, []any) {
	return joinExprs([]Expr(f), " OR ")
}

// Not wraps a child expression in NOT (...).
type Not struct {
	Inner Expr
}

func (n Not) Resolve() (string, []any) {
	sql, params := n.Inner.Resolve()
	if sql == "" {
		return "", nil
	}
	return "NOT " + sql, params
}
